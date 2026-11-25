from __future__ import annotations

import time
from typing import Any, ClassVar, cast

from qtoolkit.core.data_objects import (
    BaseQResources,
    CancelResult,
    CancelStatus,
    QCloudResources,
    QJob,
    QJobInfo,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import APIError
from qtoolkit.io.api.base import BaseAPISchedulerIO


class AWSBatchState(QSubState):
    """Job states in AWS Batch mapped to :class:`~qtoolkit.QState` values."""

    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    @property
    def qstate(self) -> QState:
        return _STATUS_MAPPING[self]  # type: ignore


_STATUS_MAPPING = {
    AWSBatchState.SUBMITTED: QState.QUEUED,
    AWSBatchState.PENDING: QState.QUEUED,
    AWSBatchState.RUNNABLE: QState.QUEUED,
    AWSBatchState.STARTING: QState.QUEUED,
    AWSBatchState.RUNNING: QState.RUNNING,
    AWSBatchState.SUCCEEDED: QState.DONE,
    AWSBatchState.FAILED: QState.FAILED,
}

_ACTIVE_STATES = (
    AWSBatchState.SUBMITTED,
    AWSBatchState.PENDING,
    AWSBatchState.RUNNABLE,
    AWSBatchState.STARTING,
    AWSBatchState.RUNNING,
)

# AWS Batch DescribeJobs accepts at most 100 job IDs per API call.
_DESCRIBE_JOBS_BATCH_SIZE = 100
_INCOMPATIBLE_OVERRIDE_KEYS = (
    "nodeOverrides",
    "eksPropertiesOverride",
    "ecsPropertiesOverride",
)
_MERGEABLE_OVERRIDE_LISTS = (
    ("resourceRequirements", "type"),
    ("environment", "name"),
)


def _normalize_queue_name(queue: str | None) -> str | None:
    if isinstance(queue, str) and "job-queue/" in queue:
        return queue.rsplit("job-queue/", 1)[-1]
    return queue


def _merge_container_overrides(
    base: dict[str, Any], extra: dict[str, Any]
) -> dict[str, Any]:
    """Merge ``containerOverrides`` dicts, merging ``resourceRequirements`` and ``environment`` by key."""
    merged = dict(base)
    merge_keys = dict(_MERGEABLE_OVERRIDE_LISTS)
    for key, value in extra.items():
        id_field = merge_keys.get(key)
        if id_field and isinstance(value, list):
            by_id = {
                item[id_field]: dict(item)
                for item in merged.get(key, [])
                if isinstance(item, dict) and id_field in item
            }
            for item in value:
                if isinstance(item, dict) and id_field in item:
                    by_id[item[id_field]] = dict(item)
            merged[key] = list(by_id.values())
        else:
            merged[key] = list(value) if isinstance(value, list) else value
    return merged


class AWSBatchIO(BaseAPISchedulerIO):
    """API-based scheduler IO backend for single-container AWS Batch jobs.

    Parameters
    ----------
    job_queue
        Default AWS Batch job queue name or ARN.
    job_definition
        Default AWS Batch job definition name, ``name:revision``, or ARN.
    region_name
        AWS region name (falls back to standard boto3 configuration if ``None``).
    profile_name
        AWS shared credentials profile name.
    endpoint_url
        Optional custom endpoint URL (e.g. for local testing or VPC endpoints).
    default_job_name
        Job name used when none is provided in ``options``.
    shell_cmd
        Shell executable used to wrap ``str`` or ``list[str]`` commands as
        ``[shell_cmd, "-c", script]`` (default ``"/bin/sh"``). Set to ``None``
        to pass ``commands: list[str]`` directly as container ``argv``.
    """

    job_id_regex: str | None = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}([:#]\d+)?$"
    )
    supported_resources_cls: tuple[type, ...] = (QCloudResources,)
    # "image" is omitted because AWS Batch sets the container image on the jobDefinition.
    _supported_keys: ClassVar[list[str]] = [
        "queue_name",
        "job_name",
        "vcpus",
        "memory",
        "gpus",
        "time_limit",
        "max_retries",
        "environment",
        "priority",
        "scheduler_kwargs",
    ]

    def __init__(
        self,
        job_queue: str | None = None,
        job_definition: str | None = None,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        default_job_name: str = "qtoolkit-job",
        shell_cmd: str | None = "/bin/sh",
    ) -> None:
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self.default_job_name = default_job_name
        self.shell_cmd = shell_cmd
        self._client: Any = None
        self._boto_errors: tuple[type[Exception], ...] | None = None

    def _get_client_and_errors(self) -> tuple[Any, tuple[type[Exception], ...]]:
        """Return the cached boto3 Batch client and botocore error classes."""
        if self._boto_errors is None or self._client is None:
            try:
                import boto3
                from botocore.exceptions import BotoCoreError, ClientError
            except ModuleNotFoundError as exc:
                msg = (
                    "boto3 is required for AWSBatchIO. "
                    "Install it with 'pip install qtoolkit[aws]'."
                )
                raise ImportError(msg) from exc

            self._boto_errors = (ClientError, BotoCoreError)
            if self._client is None:
                session = boto3.Session(
                    region_name=self.region_name,
                    profile_name=self.profile_name,
                )
                self._client = session.client("batch", endpoint_url=self.endpoint_url)
        return self._client, self._boto_errors

    def format_command(self, commands: str | list[str]) -> list[str]:
        """Convert ``commands`` to an AWS Batch ``containerOverrides.command`` list.

        When :attr:`shell_cmd` is set (default ``"/bin/sh"``), ``str`` or
        ``list[str]`` lines are joined with ``"\\n"`` (as in
        :meth:`~qtoolkit.io.base.BaseSchedulerIO.generate_run_commands`) and
        wrapped as ``[shell_cmd, "-c", script]``. When :attr:`shell_cmd` is
        ``None``, ``commands`` must be a ``list[str]`` and is passed directly
        as container ``argv``.
        """
        if self.shell_cmd is None:
            if (
                not isinstance(commands, list)
                or not commands
                or any(not isinstance(c, str) for c in commands)
                or not commands[0].strip()
            ):
                raise ValueError(
                    "commands must be a non-empty list of strings with a non-empty executable when shell_cmd is None."
                )
            return list(commands)

        if not isinstance(commands, (str, list)) or (
            isinstance(commands, list)
            and (not commands or any(not isinstance(c, str) for c in commands))
        ):
            raise TypeError("commands must be a str or a list of str.")
        script = self.generate_run_commands(commands)
        if not script.strip():
            raise ValueError("commands must not be empty.")
        return [self.shell_cmd, "-c", script]

    @property
    def supported_resources_keys(self) -> list[str]:
        """Attribute names of :class:`~qtoolkit.QCloudResources` supported by AWS Batch."""
        return list(self._supported_keys)

    def _convert_resources(self, resources: BaseQResources) -> dict:
        resources = cast("QCloudResources", resources)

        payload: dict[str, Any] = {}
        container_overrides: dict[str, Any] = {}
        resource_requirements: list[dict[str, str]] = []

        if resources.queue_name is not None:
            payload["jobQueue"] = resources.queue_name
        if resources.job_name is not None:
            payload["jobName"] = resources.job_name

        if resources.vcpus is not None:
            vcpu_val = (
                str(int(resources.vcpus))
                if float(resources.vcpus).is_integer()
                else str(resources.vcpus)
            )
            resource_requirements.append({"type": "VCPU", "value": vcpu_val})
        if resources.memory is not None:
            resource_requirements.append(
                {"type": "MEMORY", "value": str(resources.memory)}
            )
        if resources.gpus is not None and resources.gpus > 0:
            resource_requirements.append({"type": "GPU", "value": str(resources.gpus)})
        if resource_requirements:
            container_overrides["resourceRequirements"] = resource_requirements

        if resources.environment:
            container_overrides["environment"] = [
                {"name": str(k), "value": str(v)}
                for k, v in resources.environment.items()
            ]
        if container_overrides:
            payload["containerOverrides"] = container_overrides

        if resources.time_limit is not None:
            if resources.time_limit < 60:
                raise ValueError(
                    "AWS Batch requires time_limit (attemptDurationSeconds) >= 60."
                )
            payload["timeout"] = {"attemptDurationSeconds": resources.time_limit}

        if resources.max_retries is not None:
            attempts = resources.max_retries + 1
            if attempts > 10:
                raise ValueError(
                    "AWS Batch supports at most 10 attempts (max_retries <= 9)."
                )
            payload["retryStrategy"] = {"attempts": attempts}

        if resources.priority is not None:
            if not 0 <= resources.priority <= 9999:
                raise ValueError(
                    "AWS Batch schedulingPriorityOverride must be between 0 and 9999."
                )
            # AWS Batch requires a fair-share scheduling policy on the job queue
            # (and shareIdentifier in scheduler_kwargs) for schedulingPriorityOverride.
            payload["schedulingPriorityOverride"] = resources.priority

        if resources.scheduler_kwargs:
            extra = dict(resources.scheduler_kwargs)
            if "containerOverrides" in extra:
                extra_overrides = extra.pop("containerOverrides")
                if extra_overrides is not None:
                    if not isinstance(extra_overrides, dict):
                        raise TypeError("containerOverrides must be a dict.")
                    payload["containerOverrides"] = _merge_container_overrides(
                        payload.get("containerOverrides", {}),
                        extra_overrides,
                    )
            payload.update(extra)

        return payload

    def _submit(self, commands: str | list[str], options: dict) -> SubmissionResult:
        payload = dict(options)
        if not str(payload.get("jobQueue") or "").strip():
            payload["jobQueue"] = self.job_queue
        if not str(payload.get("jobDefinition") or "").strip():
            payload["jobDefinition"] = self.job_definition
        if not str(payload.get("jobName") or "").strip():
            payload["jobName"] = self.default_job_name

        if not str(payload.get("jobQueue") or "").strip():
            raise ValueError(
                "A jobQueue must be provided either on AWSBatchIO or in options."
            )
        if not str(payload.get("jobDefinition") or "").strip():
            raise ValueError(
                "A jobDefinition must be provided either on AWSBatchIO or in options."
            )
        if not str(payload.get("jobName") or "").strip():
            raise ValueError(
                "A non-empty jobName must be provided either on AWSBatchIO or in options."
            )

        for incompatible_key in _INCOMPATIBLE_OVERRIDE_KEYS:
            if payload.get(incompatible_key) is not None:
                raise ValueError(
                    f"{incompatible_key} cannot be combined with single-container "
                    "containerOverrides in AWSBatchIO.submit."
                )

        container_overrides = payload.get("containerOverrides")
        if container_overrides is None:
            container_overrides = {}
        elif isinstance(container_overrides, dict):
            container_overrides = dict(container_overrides)
        else:
            raise TypeError("containerOverrides must be a dict.")
        container_overrides["command"] = self.format_command(commands)
        payload["containerOverrides"] = container_overrides

        client, boto_errors = self._get_client_and_errors()
        try:
            response = client.submit_job(**payload)
        except boto_errors as exc:
            return SubmissionResult(
                status=SubmissionStatus("FAILED"),
                stderr=str(exc),
            )

        job_id = response.get("jobId")
        if not job_id:
            return SubmissionResult(status=SubmissionStatus("JOB_ID_UNKNOWN"))

        return SubmissionResult(
            job_id=job_id,
            status=SubmissionStatus("SUCCESSFUL"),
        )

    def _cancel(self, job_id: str, reason: str | None = None) -> CancelResult:
        client, boto_errors = self._get_client_and_errors()
        cancel_reason = reason or "Cancelled by qtoolkit"
        try:
            # TerminateJob cancels queued jobs and terminates running jobs.
            client.terminate_job(jobId=job_id, reason=cancel_reason)
        except boto_errors as exc:
            return CancelResult(
                job_id=job_id,
                status=CancelStatus("FAILED"),
                stderr=str(exc),
            )
        return CancelResult(job_id=job_id, status=CancelStatus("SUCCESSFUL"))

    def _get_job(self, job_id: str) -> QJob | None:
        jobs = self._describe_jobs_chunked([job_id])
        return jobs[0] if jobs else None

    def _get_jobs_list(
        self,
        job_ids: list[str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
    ) -> list[QJob]:
        if user is not None:
            raise NotImplementedError(
                "AWS Batch does not support filtering jobs by username."
            )

        state_set = set(states) if states is not None else None
        if state_set is not None:
            aws_statuses = [
                status
                for status, mapped_state in _STATUS_MAPPING.items()
                if mapped_state in state_set
            ]
            if not aws_statuses:
                return []
        else:
            aws_statuses = list(_ACTIVE_STATES)

        if job_ids is not None:
            jobs = self._describe_jobs_chunked(job_ids)
            if queue is not None:
                norm_queue = _normalize_queue_name(queue)
                jobs = [j for j in jobs if j.queue_name == norm_queue]
            if state_set is not None:
                jobs = [j for j in jobs if j.state in state_set]
            return jobs

        target_queue = queue or self.job_queue
        if not target_queue:
            raise ValueError(
                "A job queue must be specified either on AWSBatchIO or via "
                "'queue' when 'jobs' is None."
            )

        client, boto_errors = self._get_client_and_errors()
        collected_ids: list[str] = []
        seen_ids: set[str] = set()
        for aws_status in aws_statuses:
            list_kwargs: dict[str, Any] = {
                "jobQueue": target_queue,
                "jobStatus": aws_status.value,  # type: ignore[attr-defined]
            }
            while True:
                try:
                    response = client.list_jobs(**list_kwargs)
                except boto_errors as exc:
                    raise APIError(
                        f"Failed to list AWS Batch jobs in queue '{target_queue}': {exc}"
                    ) from exc
                for summary in response.get("jobSummaryList", []):
                    jid = summary.get("jobId")
                    if jid and jid not in seen_ids:
                        seen_ids.add(jid)
                        collected_ids.append(jid)
                next_token = response.get("nextToken")
                if not next_token:
                    break
                list_kwargs["nextToken"] = next_token

        if not collected_ids:
            return []
        jobs = self._describe_jobs_chunked(collected_ids)
        target_states = state_set or {QState("QUEUED"), QState("RUNNING")}
        return [j for j in jobs if j.state in target_states]

    def _describe_jobs_chunked(self, job_ids: list[str]) -> list[QJob]:
        client, boto_errors = self._get_client_and_errors()
        unique_ids = list(dict.fromkeys(job_ids))
        results: list[QJob] = []
        for idx in range(0, len(unique_ids), _DESCRIBE_JOBS_BATCH_SIZE):
            chunk = unique_ids[idx : idx + _DESCRIBE_JOBS_BATCH_SIZE]
            try:
                response = client.describe_jobs(jobs=chunk)
            except boto_errors as exc:
                raise APIError(f"Failed to describe AWS Batch jobs: {exc}") from exc
            results.extend(self._parse_job(raw) for raw in response.get("jobs", []))
        return results

    @staticmethod
    def _parse_job(raw: dict[str, Any]) -> QJob:
        raw_status = raw.get("status")
        sub_state: AWSBatchState | None
        state: QState
        try:
            sub_state = AWSBatchState(raw_status)
            state = sub_state.qstate
        except ValueError:
            sub_state = None
            state = QState("UNDETERMINED")

        queue_name = _normalize_queue_name(raw.get("jobQueue"))

        container = raw.get("container") or {}
        exit_status = container.get("exitCode")

        started_at = raw.get("startedAt")
        stopped_at = raw.get("stoppedAt")
        runtime: int | None = None
        if started_at is not None and started_at > 0:
            if stopped_at is not None and stopped_at > 0:
                runtime = max(0, int((stopped_at - started_at) // 1000))
            elif state == QState.RUNNING:
                runtime = max(0, int(time.time() - started_at / 1000))

        # Extract configured container resources into QJobInfo (memory in KiB).
        vcpus_val: float | None = None
        memory_mib: int | None = None
        for req in container.get("resourceRequirements") or []:
            req_type = req.get("type")
            req_val = req.get("value")
            if req_val is None:
                continue
            if req_type == "VCPU":
                try:
                    vcpus_val = float(req_val)
                except ValueError:
                    pass
            elif req_type == "MEMORY":
                try:
                    memory_mib = int(float(req_val))
                except ValueError:
                    pass

        if vcpus_val is None and container.get("vcpus") is not None:
            vcpus_val = float(container["vcpus"])
        if memory_mib is None and container.get("memory") is not None:
            memory_mib = int(container["memory"])

        # QJobInfo.cpus is int-only, so fractional vCPUs (e.g. 0.5 on Fargate) map to None.
        cpus = (
            int(vcpus_val)
            if vcpus_val is not None and vcpus_val >= 1 and vcpus_val.is_integer()
            else None
        )
        memory_kib = memory_mib * 1024 if memory_mib is not None else None

        node_props = raw.get("nodeProperties") or {}
        if node_props.get("numNodes") is not None:
            nodes: int | None = int(node_props["numNodes"])
        elif container:
            nodes = 1
        else:
            nodes = None

        timeout = raw.get("timeout") or {}
        time_limit = timeout.get("attemptDurationSeconds")

        info = QJobInfo(
            memory=memory_kib,
            nodes=nodes,
            cpus=cpus,
            time_limit=time_limit,
        )

        return QJob(
            name=raw.get("jobName"),
            job_id=raw.get("jobId"),
            exit_status=exit_status,
            state=state,
            sub_state=sub_state,
            info=info,
            runtime=runtime,
            queue_name=queue_name,
        )
