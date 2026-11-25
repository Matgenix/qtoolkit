from __future__ import annotations

import copy
import time
from typing import Any, ClassVar

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QCloudResources,
    QJob,
    QJobInfo,
    QResources,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import APIError, UnsupportedResourcesError
from qtoolkit.io.api.base import BaseAPISchedulerIO


class AWSBatchState(QSubState):
    """Job states in AWS Batch mapped to standardized :class:`~qtoolkit.QState` values."""

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


def _normalize_queue_name(queue: str | None) -> str | None:
    if isinstance(queue, str) and "job-queue/" in queue:
        return queue.rsplit("job-queue/", 1)[-1]
    return queue


def _merge_container_overrides(
    base: dict[str, Any], extra: dict[str, Any]
) -> dict[str, Any]:
    """Merge AWS Batch ``containerOverrides`` dicts without clobbering list items."""
    merged = copy.deepcopy(base)
    for key, value in extra.items():
        if key == "resourceRequirements" and isinstance(value, list):
            by_type = {
                item["type"]: dict(item)
                for item in merged.get("resourceRequirements", [])
                if isinstance(item, dict) and "type" in item
            }
            for item in value:
                if isinstance(item, dict) and "type" in item:
                    by_type[item["type"]] = dict(item)
            merged["resourceRequirements"] = list(by_type.values())
        elif key == "environment" and isinstance(value, list):
            by_name = {
                item["name"]: dict(item)
                for item in merged.get("environment", [])
                if isinstance(item, dict) and "name" in item
            }
            for item in value:
                if isinstance(item, dict) and "name" in item:
                    by_name[item["name"]] = dict(item)
            merged["environment"] = list(by_name.values())
        else:
            merged[key] = copy.deepcopy(value)
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
        Optional custom endpoint URL (useful for local testing or VPC endpoints).
    default_job_name
        Job name used when none is provided in ``options``.
    shell_cmd
        Shell executable used to run string or multi-line shell commands via
        ``[shell_cmd, "-c", script]`` (default ``"/bin/sh"``). Set to ``None``
        to pass ``commands: list[str]`` directly as a container ``argv`` list.
    """

    job_id_regex: str | None = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}([:#]\d+)?$"
    )
    supported_resources_cls: tuple[type, ...] = (QCloudResources,)
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

    def _get_client(self) -> Any:
        """Return a cached boto3 Batch client, importing boto3 lazily."""
        if self._client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:
                msg = (
                    "boto3 is required for AWSBatchIO. "
                    "Install it with 'pip install qtoolkit[aws]'."
                )
                raise ImportError(msg) from exc

            session = boto3.Session(
                region_name=self.region_name,
                profile_name=self.profile_name,
            )
            self._client = session.client("batch", endpoint_url=self.endpoint_url)
        return self._client

    @staticmethod
    def _get_boto_errors() -> tuple[type[Exception], ...]:
        """Return boto/botocore error classes (imported lazily after boto3 check)."""
        try:
            from botocore.exceptions import BotoCoreError, ClientError
        except ModuleNotFoundError as exc:
            msg = (
                "boto3 is required for AWSBatchIO. "
                "Install it with 'pip install qtoolkit[aws]'."
            )
            raise ImportError(msg) from exc
        return (ClientError, BotoCoreError)

    def format_command(self, commands: str | list[str]) -> list[str]:
        """Convert ``commands`` to an AWS Batch ``containerOverrides.command`` list.

        When :attr:`shell_cmd` is set (default ``"/bin/sh"``), both a ``str``
        and a ``list[str]`` of shell lines are joined with ``"\\n"`` and wrapped
        as ``[shell_cmd, "-c", script]``, matching
        :meth:`~qtoolkit.io.base.BaseSchedulerIO.generate_run_commands`.
        When :attr:`shell_cmd` is ``None``, ``commands`` must be a ``list[str]``
        and is passed directly as container ``argv``.
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

        if isinstance(commands, str):
            script = commands
        elif isinstance(commands, list):
            if not commands or any(not isinstance(c, str) for c in commands):
                raise ValueError("commands must be a non-empty list of strings.")
            script = "\n".join(commands)
        else:
            raise TypeError("commands must be a str or a list of str.")

        if not script.strip():
            raise ValueError("commands must not be empty.")
        return [self.shell_cmd, "-c", script]

    @property
    def supported_resources_keys(self) -> list[str]:
        """Attribute names of :class:`~qtoolkit.QCloudResources` supported by AWS Batch."""
        return list(self._supported_keys)

    def _convert_resources(self, resources: QCloudResources | QResources) -> dict:
        if not isinstance(resources, QCloudResources):
            raise UnsupportedResourcesError(
                f"AWSBatchIO only supports QCloudResources, got {type(resources).__name__}."
            )

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
            # Note: AWS Batch requires a fair-share scheduling policy on the
            # job queue (and shareIdentifier in scheduler_kwargs) when using
            # schedulingPriorityOverride.
            payload["schedulingPriorityOverride"] = resources.priority

        if resources.scheduler_kwargs:
            extra = copy.deepcopy(resources.scheduler_kwargs)
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
        payload = copy.deepcopy(options)
        if not payload.get("jobQueue"):
            payload["jobQueue"] = self.job_queue
        if not payload.get("jobDefinition"):
            payload["jobDefinition"] = self.job_definition
        if not payload.get("jobName"):
            payload["jobName"] = self.default_job_name

        if not payload.get("jobQueue"):
            raise ValueError(
                "A jobQueue must be provided either on AWSBatchIO or in options."
            )
        if not payload.get("jobDefinition"):
            raise ValueError(
                "A jobDefinition must be provided either on AWSBatchIO or in options."
            )
        if not payload.get("jobName"):
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
            payload["containerOverrides"] = container_overrides
        elif not isinstance(container_overrides, dict):
            raise TypeError("containerOverrides must be a dict.")
        container_overrides["command"] = self.format_command(commands)

        client = self._get_client()
        boto_errors = self._get_boto_errors()
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
        client = self._get_client()
        boto_errors = self._get_boto_errors()
        cancel_reason = reason or "Cancelled by qtoolkit"
        try:
            # TerminateJob handles both queued (SUBMITTED/PENDING/RUNNABLE)
            # and running (STARTING/RUNNING) jobs in AWS Batch.
            client.terminate_job(jobId=job_id, reason=cancel_reason)
        except boto_errors as exc:
            return CancelResult(
                job_id=job_id,
                status=CancelStatus("FAILED"),
                stderr=str(exc),
            )
        return CancelResult(job_id=job_id, status=CancelStatus("SUCCESSFUL"))

    def _get_job(self, job_id: str) -> QJob | None:
        client = self._get_client()
        boto_errors = self._get_boto_errors()
        try:
            response = client.describe_jobs(jobs=[job_id])
        except boto_errors as exc:
            raise APIError(
                f"Failed to describe AWS Batch job '{job_id}': {exc}"
            ) from exc
        raw_jobs = response.get("jobs", [])
        if not raw_jobs:
            return None
        return self._parse_job(raw_jobs[0])

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
            client = self._get_client()
            boto_errors = self._get_boto_errors()
            jobs = self._describe_jobs_chunked(client, job_ids, boto_errors)
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

        client = self._get_client()
        boto_errors = self._get_boto_errors()

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
        jobs = self._describe_jobs_chunked(client, collected_ids, boto_errors)
        target_states = (
            state_set
            if state_set is not None
            else {_STATUS_MAPPING[s] for s in aws_statuses}
        )
        return [j for j in jobs if j.state in target_states]

    def _describe_jobs_chunked(
        self,
        client: Any,
        job_ids: list[str],
        boto_errors: tuple[type[Exception], ...],
    ) -> list[QJob]:
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

        # Extract resource utilization into QJobInfo (memory in KiB).
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

        cpus = (
            int(vcpus_val)
            if vcpus_val is not None and vcpus_val >= 1 and vcpus_val.is_integer()
            else None
        )
        memory_kib = memory_mib * 1024 if memory_mib is not None else None

        node_props = raw.get("nodeProperties")
        if isinstance(node_props, dict) and node_props.get("numNodes") is not None:
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
