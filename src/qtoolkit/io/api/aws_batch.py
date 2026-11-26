from __future__ import annotations

import re
import time
from typing import Any, cast

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

try:
    from botocore.exceptions import BotoCoreError, ClientError

    _BOTO_ERRORS: tuple[type[Exception], ...] = (BotoCoreError, ClientError)
except ModuleNotFoundError:
    _BOTO_ERRORS = ()


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
# containerOverrides lists merged item by item, keyed by this field of each item.
_MERGE_KEYS = {"resourceRequirements": "type", "environment": "name"}


def sanitize_job_name(name: str) -> str:
    """Make ``name`` a valid AWS Batch job name.

    AWS Batch job names have at most 128 characters, start with a letter or a
    digit, and contain only letters, digits, hyphens, and underscores.
    Leading and trailing whitespace is removed and other characters are replaced
    with ``_``. A name that does not start with a letter or a digit gets the
    prefix ``job_``.
    """
    name = re.sub(r"[^A-Za-z0-9_-]", "_", name.strip())
    if not name[:1].isalnum():
        name = f"job_{name}"
    return name[:128]


def _queue_name_from_arn(queue: str | None) -> str | None:
    if queue and "job-queue/" in queue:
        return queue.rsplit("job-queue/", 1)[-1]
    return queue


def _merge_container_overrides(
    base: dict[str, Any], extra: dict[str, Any]
) -> dict[str, Any]:
    """Merge two ``containerOverrides`` dicts.

    Items of ``resourceRequirements`` and ``environment`` in ``extra`` replace
    the items of ``base`` with the same type or name. Other keys of ``extra``
    replace those of ``base``.
    """
    merged = dict(base)
    for key, value in extra.items():
        id_field = _MERGE_KEYS.get(key)
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
    """Scheduler IO for single-container AWS Batch jobs.

    A ``max_retries`` set in :class:`~qtoolkit.QCloudResources` replaces the
    whole retry strategy of the job definition, including its
    ``evaluateOnExit`` rules. AWS Batch retries a failed attempt until the
    attempts are used up, but it does not retry jobs that timed out, jobs that
    were cancelled or terminated, and jobs with an invalid job definition.

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
        Job name used when none is provided in ``options``. Every job name is
        passed through :func:`sanitize_job_name` before submission.
    shell_cmd
        Shell executable used to wrap ``str`` or ``list[str]`` commands as
        ``[shell_cmd, "-c", script]`` (default ``"/bin/bash"``, the same shell
        as qtoolkit's HPC submission scripts). The container image must
        provide it. Set to ``None`` to pass ``commands: list[str]`` directly
        as container ``argv``.
    """

    job_id_regex: str | None = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}([:#]\d+)?$"
    )

    def __init__(
        self,
        job_queue: str | None = None,
        job_definition: str | None = None,
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
        default_job_name: str = "qtoolkit-job",
        shell_cmd: str | None = "/bin/bash",
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
        """Return the boto3 Batch client, created on the first call.

        Raises
        ------
        APIError
            If the client cannot be created, for example when no AWS region is
            configured or the credentials profile does not exist.
        """
        if self._client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:
                msg = "AWSBatchIO needs boto3. Install it with 'pip install qtoolkit[aws]'."
                raise ImportError(msg) from exc
            try:
                session = boto3.Session(
                    region_name=self.region_name, profile_name=self.profile_name
                )
                self._client = session.client("batch", endpoint_url=self.endpoint_url)
            except _BOTO_ERRORS as exc:
                raise APIError(f"Failed to create the AWS Batch client: {exc}") from exc
        return self._client

    def format_command(self, commands: str | list[str]) -> list[str]:
        """Return ``commands`` as the ``command`` list of the container.

        With :attr:`shell_cmd` set, the command lines are joined into one script
        that runs as ``[shell_cmd, "-c", script]``. With :attr:`shell_cmd` set to
        ``None``, ``commands`` must be a list and is used as it is.
        """
        if self.shell_cmd is None:
            if not isinstance(commands, list):
                raise ValueError("commands must be a list when shell_cmd is None.")
            return list(commands)
        return [self.shell_cmd, "-c", self.generate_run_commands(commands)]

    @property
    def supported_qresources_keys(self) -> list[str]:
        # The job definition sets the image, so "image" is not supported.
        return [
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

    def _convert_qresources(self, resources: BaseQResources) -> dict:
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
        if resources.gpus:
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
            extra_overrides = extra.pop("containerOverrides", None)
            if extra_overrides is not None:
                if not isinstance(extra_overrides, dict):
                    raise TypeError("containerOverrides must be a dict.")
                payload["containerOverrides"] = _merge_container_overrides(
                    payload.get("containerOverrides", {}), extra_overrides
                )
            payload.update(extra)

        return payload

    def _submit(self, commands: str | list[str], options: dict) -> SubmissionResult:
        payload = dict(options)
        payload["jobQueue"] = payload.get("jobQueue") or self.job_queue
        payload["jobDefinition"] = payload.get("jobDefinition") or self.job_definition
        for key in ("jobQueue", "jobDefinition"):
            if not payload[key]:
                raise ValueError(f"{key} must be set on AWSBatchIO or in options.")
        job_name = (payload.get("jobName") or "").strip() or self.default_job_name
        payload["jobName"] = sanitize_job_name(job_name)

        for incompatible_key in _INCOMPATIBLE_OVERRIDE_KEYS:
            if payload.get(incompatible_key) is not None:
                raise ValueError(
                    f"{incompatible_key} is not supported. "
                    "AWSBatchIO submits single-container jobs only."
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

        client = self._get_client()
        try:
            response = client.submit_job(**payload)
        except _BOTO_ERRORS as exc:
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
        cancel_reason = reason or "Cancelled by qtoolkit"
        try:
            # TerminateJob cancels queued jobs and terminates running jobs.
            client.terminate_job(jobId=job_id, reason=cancel_reason)
        except _BOTO_ERRORS as exc:
            return CancelResult(
                job_id=job_id,
                status=CancelStatus("FAILED"),
                stderr=str(exc),
            )
        return CancelResult(job_id=job_id, status=CancelStatus("SUCCESSFUL"))

    def _get_job(self, job_id: str) -> QJob | None:
        jobs = self._describe_jobs([job_id])
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
            jobs = self._describe_jobs(job_ids)
            if queue is not None:
                norm_queue = _queue_name_from_arn(queue)
                jobs = [j for j in jobs if j.queue_name == norm_queue]
            if state_set is not None:
                jobs = [j for j in jobs if j.state in state_set]
            return jobs

        target_queue = queue or self.job_queue
        if not target_queue:
            raise ValueError(
                "Set job_queue on AWSBatchIO or pass queue to get_jobs_list."
            )

        client = self._get_client()
        collected_ids: list[str] = []
        for aws_status in aws_statuses:
            list_kwargs: dict[str, Any] = {
                "jobQueue": target_queue,
                "jobStatus": aws_status.value,  # type: ignore[attr-defined]
            }
            while True:
                try:
                    response = client.list_jobs(**list_kwargs)
                except _BOTO_ERRORS as exc:
                    raise APIError(
                        f"Failed to list AWS Batch jobs in queue '{target_queue}': {exc}"
                    ) from exc
                collected_ids.extend(
                    summary["jobId"] for summary in response.get("jobSummaryList", [])
                )
                next_token = response.get("nextToken")
                if not next_token:
                    break
                list_kwargs["nextToken"] = next_token

        if not collected_ids:
            return []
        jobs = self._describe_jobs(collected_ids)
        target_states = state_set or {QState("QUEUED"), QState("RUNNING")}
        return [j for j in jobs if j.state in target_states]

    def _describe_jobs(self, job_ids: list[str]) -> list[QJob]:
        client = self._get_client()
        unique_ids = list(dict.fromkeys(job_ids))
        results: list[QJob] = []
        for idx in range(0, len(unique_ids), _DESCRIBE_JOBS_BATCH_SIZE):
            chunk = unique_ids[idx : idx + _DESCRIBE_JOBS_BATCH_SIZE]
            try:
                response = client.describe_jobs(jobs=chunk)
            except _BOTO_ERRORS as exc:
                raise APIError(f"Failed to describe AWS Batch jobs: {exc}") from exc
            results.extend(self._parse_job(raw) for raw in response.get("jobs", []))
        return results

    @staticmethod
    def _parse_job(raw: dict[str, Any]) -> QJob:
        try:
            sub_state: AWSBatchState | None = AWSBatchState(raw.get("status"))
            state = sub_state.qstate
        except ValueError:
            sub_state = None
            state = QState("UNDETERMINED")

        queue_name = _queue_name_from_arn(raw.get("jobQueue"))

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
                vcpus_val = float(req_val)
            elif req_type == "MEMORY":
                memory_mib = int(float(req_val))

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
