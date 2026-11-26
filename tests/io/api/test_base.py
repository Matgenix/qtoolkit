from __future__ import annotations

import pytest

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QCloudResources,
    QJob,
    QResources,
    QState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import InvalidJobIDError, UnsupportedResourcesError
from qtoolkit.io.api.base import BaseAPISchedulerIO

try:
    import monty
except ModuleNotFoundError:
    monty = None


class DummyAPISchedulerIO(BaseAPISchedulerIO):
    job_id_regex = r"^job-\d+$"
    supported_resources_cls = (QCloudResources,)

    def __init__(self, endpoint: str = "https://example.invalid") -> None:
        self.endpoint = endpoint
        self.submitted: list[tuple[str | list[str], dict]] = []
        self.cancelled: list[tuple[str, str | None]] = []

    @property
    def supported_qresources_keys(self) -> list[str]:
        return ["queue_name", "job_name", "vcpus", "memory", "scheduler_kwargs"]

    def _convert_qresources(self, resources: QCloudResources | QResources) -> dict:
        assert isinstance(resources, QCloudResources)
        out = {}
        if resources.queue_name is not None:
            out["queue"] = resources.queue_name
        if resources.job_name is not None:
            out["name"] = resources.job_name
        if resources.vcpus is not None:
            out["vcpus"] = resources.vcpus
        if resources.memory is not None:
            out["memory"] = resources.memory
        if resources.scheduler_kwargs:
            out.update(resources.scheduler_kwargs)
        return out

    def _submit(self, commands: str | list[str], options: dict) -> SubmissionResult:
        self.submitted.append((commands, options))
        return SubmissionResult(job_id="job-1", status=SubmissionStatus.SUCCESSFUL)

    def _cancel(self, job_id: str, reason: str | None = None) -> CancelResult:
        self.cancelled.append((job_id, reason))
        return CancelResult(job_id=job_id, status=CancelStatus.SUCCESSFUL)

    def _get_job(self, job_id: str) -> QJob | None:
        return QJob(job_id=job_id, state=QState.RUNNING)

    def _get_jobs_list(
        self,
        job_ids: list[str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
    ) -> list[QJob]:
        if job_ids is None:
            return [QJob(job_id="job-1", state=QState.RUNNING)]
        return [QJob(job_id=jid, state=QState.RUNNING) for jid in job_ids]


class TestBaseAPISchedulerIO:
    def test_submit_with_qcloud_resources_and_dict(self):
        io = DummyAPISchedulerIO()
        res = QCloudResources(
            queue_name="gpu-q",
            job_name="test",
            vcpus=4,
            memory=8192,
            scheduler_kwargs={"extra": "val"},
        )
        result = io.submit("echo hello", options=res)
        assert result.status == SubmissionStatus.SUCCESSFUL
        assert result.job_id == "job-1"
        assert io.submitted[-1] == (
            "echo hello",
            {
                "queue": "gpu-q",
                "name": "test",
                "vcpus": 4,
                "memory": 8192,
                "extra": "val",
            },
        )

        raw_opts = {"queue": "q2"}
        io.submit(["echo step1", "echo step2"], options=raw_opts)
        assert io.submitted[-1] == (["echo step1", "echo step2"], {"queue": "q2"})

        io.submit("echo hi")
        assert io.submitted[-1] == ("echo hi", {})

    def test_submit_validation(self):
        io = DummyAPISchedulerIO()
        with pytest.raises(ValueError, match="empty"):
            io.submit("")
        with pytest.raises(ValueError, match="empty"):
            io.submit("   ")
        with pytest.raises(ValueError, match="non-empty entry"):
            io.submit([])
        with pytest.raises(ValueError, match="non-empty entry"):
            io.submit(["", "   "])
        with pytest.raises(ValueError, match="non-empty entry"):
            io.submit(["ok", 123])  # type: ignore[list-item]
        # Blank lines or empty arguments in list[str] are allowed when at least one entry is non-empty
        io.submit(["echo a", "", "echo b"])
        assert io.submitted[-1] == (["echo a", "", "echo b"], {})
        with pytest.raises(TypeError, match="str or a list"):
            io.submit(123)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="options must be"):
            io.submit("echo hi", options="invalid")  # type: ignore[arg-type]

    def test_check_convert_qresources_errors(self):
        io = DummyAPISchedulerIO()
        with pytest.raises(
            UnsupportedResourcesError, match="does not support QResources"
        ):
            io.submit("echo hi", options=QResources(processes=2))

        with pytest.raises(UnsupportedResourcesError, match="gpus, image"):
            io.submit("echo hi", options=QCloudResources(gpus=1, image="ubuntu:latest"))

        io.submit(
            "echo hi", options=QCloudResources(environment={}, scheduler_kwargs={})
        )

    def test_job_id_conversion_and_validation(self):
        io = DummyAPISchedulerIO()
        assert io.to_job_id("job-42") == "job-42"
        assert io.to_job_id(QJob(job_id="job-42")) == "job-42"
        assert io.is_valid_job_id("job-42") is True
        assert io.is_valid_job_id("bad-id") is False

        for empty in ("", "   ", QJob(job_id=None)):
            with pytest.raises(ValueError, match="Job ID must not be empty"):
                io.to_job_id(empty)
        with pytest.raises(InvalidJobIDError):
            io.to_job_id("bad-id")
        with pytest.raises(TypeError, match="not a str"):
            io.get_jobs_list(jobs="job-42")  # type: ignore[arg-type]

        # check_job_ids=False and job_id_regex=None bypass regex check
        io.check_job_ids = False
        assert io.to_job_id("anything") == "anything"
        io.check_job_ids = True
        io.job_id_regex = None
        assert io.is_valid_job_id("anything") is True
        assert io.to_job_id(123) == "123"

    def test_cancel_get_job_and_list(self):
        io = DummyAPISchedulerIO()
        cancel_res = io.cancel(QJob(job_id="job-7"), reason="user abort")
        assert cancel_res.status == CancelStatus.SUCCESSFUL
        assert io.cancelled == [("job-7", "user abort")]

        job = io.get_job("job-9")
        assert job is not None
        assert job.job_id == "job-9"

        assert io.get_jobs_list(jobs=[]) == []
        jobs = io.get_jobs_list(jobs=["job-1", QJob(job_id="job-2")])
        assert [j.job_id for j in jobs] == ["job-1", "job-2"]
        assert io.get_jobs_list()[0].job_id == "job-1"

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        io = DummyAPISchedulerIO(endpoint="https://api.example.com")
        assert test_utils.is_msonable(io)

    def test_zero_counts_as_set(self):
        # max_retries=0 is a request, so an IO without max_retries rejects it.
        with pytest.raises(UnsupportedResourcesError, match="max_retries"):
            DummyAPISchedulerIO().submit(
                "echo hi", options=QCloudResources(max_retries=0)
            )
