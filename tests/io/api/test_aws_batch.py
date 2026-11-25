from __future__ import annotations

import builtins
import time
from unittest.mock import MagicMock

import pytest

from qtoolkit.core.data_objects import (
    CancelStatus,
    QCloudResources,
    QResources,
    QState,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import (
    APIError,
    InvalidJobIDError,
    UnsupportedResourcesError,
)
from qtoolkit.io.api import AWSBatchIO, AWSBatchState, api_scheduler_mapping

try:
    import monty
except ModuleNotFoundError:
    monty = None


@pytest.fixture()
def aws_batch_env(monkeypatch):
    """Set up a mocked AWS Batch compute environment, queue, and job definition."""
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    with moto.mock_aws(config={"batch": {"use_docker": False}}):
        iam = boto3.client("iam", region_name="us-east-1")
        ec2 = boto3.client("ec2", region_name="us-east-1")
        batch = boto3.client("batch", region_name="us-east-1")

        role = iam.create_role(
            RoleName="batch-service-role",
            AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
        )["Role"]["Arn"]
        iam.create_instance_profile(InstanceProfileName="ecs-instance-profile")
        iam.add_role_to_instance_profile(
            InstanceProfileName="ecs-instance-profile",
            RoleName="batch-service-role",
        )
        instance_profile_arn = iam.get_instance_profile(
            InstanceProfileName="ecs-instance-profile"
        )["InstanceProfile"]["Arn"]

        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc, CidrBlock="10.0.1.0/24")["Subnet"][
            "SubnetId"
        ]
        sg = ec2.create_security_group(
            GroupName="batch-sg", Description="test", VpcId=vpc
        )["GroupId"]

        ce_arn = batch.create_compute_environment(
            computeEnvironmentName="test-ce",
            type="MANAGED",
            state="ENABLED",
            serviceRole=role,
            computeResources={
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 16,
                "desiredvCpus": 0,
                "instanceTypes": ["optimal"],
                "subnets": [subnet],
                "securityGroupIds": [sg],
                "instanceRole": instance_profile_arn,
            },
        )["computeEnvironmentArn"]

        queue_name = "test-queue"
        batch.create_job_queue(
            jobQueueName=queue_name,
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
        )

        job_def_name = "test-job-def"
        batch.register_job_definition(
            jobDefinitionName=job_def_name,
            type="container",
            containerProperties={
                "image": "python:3.12-slim",
                "vcpus": 2,
                "memory": 2048,
                "command": ["echo", "default"],
            },
        )

        io = AWSBatchIO(
            job_queue=queue_name,
            job_definition=job_def_name,
            region_name="us-east-1",
        )
        yield io, batch, queue_name, job_def_name


class TestAWSBatchState:
    def test_state_mapping(self):
        assert AWSBatchState.SUBMITTED.qstate == QState.QUEUED
        assert AWSBatchState.PENDING.qstate == QState.QUEUED
        assert AWSBatchState.RUNNABLE.qstate == QState.QUEUED
        assert AWSBatchState.STARTING.qstate == QState.QUEUED
        assert AWSBatchState.RUNNING.qstate == QState.RUNNING
        assert AWSBatchState.SUCCEEDED.qstate == QState.DONE
        assert AWSBatchState.FAILED.qstate == QState.FAILED

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        for state in AWSBatchState:
            assert test_utils.is_msonable(state)


class TestAWSBatchIO:
    def test_mapping(self):
        assert api_scheduler_mapping["aws_batch"] is AWSBatchIO

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        io = AWSBatchIO(
            job_queue="my-queue",
            job_definition="my-def:1",
            region_name="us-west-2",
            profile_name="research",
            endpoint_url="http://localhost:5000",
            default_job_name="calc",
            shell_cmd="/bin/bash",
        )
        assert test_utils.is_msonable(io)
        # Ensure MSONable still works after _client is populated
        io._client = object()
        assert test_utils.is_msonable(io)

    def test_missing_boto3_raises_helpful_error(self, monkeypatch):
        io = AWSBatchIO(job_queue="q", job_definition="d")
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "boto3" or name.startswith("botocore"):
                raise ModuleNotFoundError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        valid_id = "12345678-1234-1234-1234-1234567890ab"
        with pytest.raises(ImportError, match="qtoolkit\\[aws\\]"):
            io.submit("echo hi")
        with pytest.raises(ImportError, match="qtoolkit\\[aws\\]"):
            io.cancel(valid_id)
        with pytest.raises(ImportError, match="qtoolkit\\[aws\\]"):
            io.get_job(valid_id)

    def test_format_command(self):
        io = AWSBatchIO(shell_cmd="/bin/bash")
        assert io.format_command("echo hi && pwd") == [
            "/bin/bash",
            "-c",
            "echo hi && pwd",
        ]
        assert io.format_command(["echo a", "", "echo b"]) == [
            "/bin/bash",
            "-c",
            "echo a\n\necho b",
        ]

        io_raw = AWSBatchIO(shell_cmd=None)
        argv = ["python", "script.py", "--prefix", "", "--sep", " "]
        formatted = io_raw.format_command(argv)
        assert formatted == argv
        assert formatted is not argv
        with pytest.raises(ValueError, match="shell_cmd is None"):
            io_raw.format_command("echo hi")
        with pytest.raises(ValueError, match="shell_cmd is None"):
            io_raw.format_command(["", "--flag"])

    def test_job_id_regex(self):
        io = AWSBatchIO()
        uid = "12345678-1234-1234-1234-1234567890ab"
        assert io.is_valid_job_id(uid) is True
        assert io.is_valid_job_id(f"{uid}:0") is True
        assert io.is_valid_job_id(f"{uid}#2") is True
        assert io.is_valid_job_id(f"{uid}:") is False
        assert io.is_valid_job_id(f"{uid}:abc") is False
        assert io.is_valid_job_id("not-a-uuid") is False

    def test_submit_get_and_list_with_moto(self, aws_batch_env):
        io, batch, queue_name, _job_def = aws_batch_env
        resources = QCloudResources(
            job_name="phonon-run",
            vcpus=4,
            memory=4096,
            gpus=1,
            time_limit=1800,
            max_retries=2,
            environment={"OMP_NUM_THREADS": "4"},
        )
        sub = io.submit("echo hello", options=resources)
        assert sub.status == SubmissionStatus.SUCCESSFUL
        assert sub.job_id is not None
        assert io.is_valid_job_id(str(sub.job_id))

        job = io.get_job(sub.job_id)
        assert job is not None
        assert job.job_id == sub.job_id
        assert job.name == "phonon-run"
        assert job.queue_name == queue_name
        assert job.state == QState.DONE
        assert job.sub_state == AWSBatchState.SUCCEEDED
        assert job.info is not None
        # moto with use_docker=False keeps the job definition's vcpus/memory (2 / 2048 MiB)
        assert job.info.cpus == 2
        assert job.info.memory == 2048 * 1024
        assert job.info.time_limit == 1800

        raw = batch.describe_jobs(jobs=[sub.job_id])["jobs"][0]
        assert raw["container"]["command"] == ["/bin/sh", "-c", "echo hello"]
        assert {"name": "OMP_NUM_THREADS", "value": "4"} in raw["container"][
            "environment"
        ]

        # Query by explicit job ID list (both str and QJob)
        listed = io.get_jobs_list(jobs=[sub.job_id])
        assert len(listed) == 1
        assert listed[0].job_id == sub.job_id

        listed_qjob = io.get_jobs_list(jobs=[job])
        assert len(listed_qjob) == 1
        assert listed_qjob[0].job_id == sub.job_id

        # Query by queue with state filter covering all states
        by_queue = io.get_jobs_list(queue=queue_name, states=list(QState))
        assert any(j.job_id == sub.job_id for j in by_queue)

    def test_submit_argv_and_cancel_with_moto(self, aws_batch_env):
        _io, batch, queue_name, job_def = aws_batch_env
        io_argv = AWSBatchIO(
            job_queue=queue_name,
            job_definition=job_def,
            region_name="us-east-1",
            shell_cmd=None,
        )
        sub = io_argv.submit(["python", "-c", "print('ok')"])
        assert sub.status == SubmissionStatus.SUCCESSFUL
        assert sub.job_id is not None

        raw = batch.describe_jobs(jobs=[sub.job_id])["jobs"][0]
        assert raw["container"]["command"] == ["python", "-c", "print('ok')"]

        cancel_res = io_argv.cancel(sub.job_id, reason="no longer needed")
        assert cancel_res.status == CancelStatus.SUCCESSFUL
        assert cancel_res.job_id == sub.job_id

    def test_get_job_not_found_with_moto(self, aws_batch_env):
        io, _batch, _queue_name, _job_def = aws_batch_env
        missing_id = "00000000-0000-0000-0000-000000000000"
        assert io.get_job(missing_id) is None

    def test_submit_validation_errors(self):
        io = AWSBatchIO()
        with pytest.raises(ValueError, match="jobQueue"):
            io.submit("echo hi")

        io_q = AWSBatchIO(job_queue="q")
        with pytest.raises(ValueError, match="jobDefinition"):
            io_q.submit("echo hi")

        io_empty_name = AWSBatchIO(
            job_queue="q", job_definition="d", default_job_name=""
        )
        with pytest.raises(ValueError, match="jobName"):
            io_empty_name.submit("echo hi")

        io_ok = AWSBatchIO(job_queue="q", job_definition="d")
        with pytest.raises(TypeError, match="containerOverrides"):
            io_ok.submit("echo hi", options={"containerOverrides": "bad"})
        with pytest.raises(ValueError, match="nodeOverrides"):
            io_ok.submit("echo hi", options={"nodeOverrides": {"numNodes": 2}})

    def test_convert_resources_and_deep_merge(self):
        io = AWSBatchIO(job_queue="default-q", job_definition="default-def")
        res = QCloudResources(
            queue_name="override-q",
            job_name="my-job",
            vcpus=0.5,
            memory=1024,
            gpus=0,
            time_limit=600,
            max_retries=2,
            environment={"A": "1", "B": "2"},
            priority=10,
            scheduler_kwargs={
                "tags": {"project": "phonon"},
                "shareIdentifier": "default",
                "containerOverrides": {
                    "resourceRequirements": [{"type": "MEMORY", "value": "2048"}],
                    "environment": [
                        {"name": "B", "value": "override"},
                        {"name": "C", "value": "3"},
                    ],
                },
            },
        )
        converted = io.check_convert_resources(res)
        assert converted["jobQueue"] == "override-q"
        assert converted["jobName"] == "my-job"
        assert converted["timeout"] == {"attemptDurationSeconds": 600}
        assert converted["retryStrategy"] == {"attempts": 3}
        assert converted["schedulingPriorityOverride"] == 10
        assert converted["tags"] == {"project": "phonon"}
        assert converted["shareIdentifier"] == "default"

        reqs = {
            r["type"]: r["value"]
            for r in converted["containerOverrides"]["resourceRequirements"]
        }
        assert reqs == {"VCPU": "0.5", "MEMORY": "2048"}

        env = {
            e["name"]: e["value"]
            for e in converted["containerOverrides"]["environment"]
        }
        assert env == {"A": "1", "B": "override", "C": "3"}

        # AWS Batch constraint checks
        with pytest.raises(ValueError, match=">= 60"):
            io.check_convert_resources(QCloudResources(time_limit=30))
        with pytest.raises(ValueError, match="max_retries <= 9"):
            io.check_convert_resources(QCloudResources(max_retries=10))
        with pytest.raises(ValueError, match="0 and 9999"):
            io.check_convert_resources(QCloudResources(priority=10000))
        with pytest.raises(TypeError, match="containerOverrides"):
            io.check_convert_resources(
                QCloudResources(scheduler_kwargs={"containerOverrides": "invalid"})
            )

        # Unsupported fields and types
        with pytest.raises(UnsupportedResourcesError, match="image"):
            io.check_convert_resources(QCloudResources(image="ubuntu:24.04"))
        with pytest.raises(UnsupportedResourcesError, match="QResources"):
            io.check_convert_resources(QResources(processes=4))

    def test_mocked_submit_cancel_and_errors(self):
        botocore_exc = pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client

        # Verify exact submit_job payload including explicit None fallback
        mock_client.submit_job.return_value = {
            "jobId": "12345678-1234-1234-1234-1234567890ab"
        }
        res = QCloudResources(
            vcpus=4,
            memory=8192,
            gpus=1,
            time_limit=3600,
            max_retries=1,
            priority=5,
            scheduler_kwargs={"shareIdentifier": "s1"},
        )
        sub_ok = io.submit(["echo step1", "echo step2"], options=res)
        assert sub_ok.status == SubmissionStatus.SUCCESSFUL
        sent = mock_client.submit_job.call_args.kwargs
        assert sent["jobQueue"] == "q"
        assert sent["jobDefinition"] == "d"
        assert sent["jobName"] == "qtoolkit-job"
        assert sent["timeout"] == {"attemptDurationSeconds": 3600}
        assert sent["retryStrategy"] == {"attempts": 2}
        assert sent["schedulingPriorityOverride"] == 5
        assert sent["shareIdentifier"] == "s1"
        assert sent["containerOverrides"]["command"] == [
            "/bin/sh",
            "-c",
            "echo step1\necho step2",
        ]
        assert {"type": "GPU", "value": "1"} in sent["containerOverrides"][
            "resourceRequirements"
        ]

        # Explicit None in dict falls back to defaults
        io.submit("echo hi", options={"jobQueue": None, "jobName": None})
        sent2 = mock_client.submit_job.call_args.kwargs
        assert sent2["jobQueue"] == "q"
        assert sent2["jobName"] == "qtoolkit-job"

        # Terminate job args (default and custom reason)
        valid_id = "12345678-1234-1234-1234-1234567890ab"
        io.cancel(valid_id)
        mock_client.terminate_job.assert_called_with(
            jobId=valid_id, reason="Cancelled by qtoolkit"
        )
        io.cancel(valid_id, reason="preempted")
        mock_client.terminate_job.assert_called_with(jobId=valid_id, reason="preempted")

        # ClientError handling across submit, cancel, get_job, get_jobs_list
        err = botocore_exc.ClientError(
            {"Error": {"Code": "ClientException", "Message": "Queue not found"}},
            "SubmitJob",
        )
        mock_client.submit_job.side_effect = err
        sub_fail = io.submit("echo hi")
        assert sub_fail.status == SubmissionStatus.FAILED
        assert sub_fail.job_id is None
        assert "Queue not found" in (sub_fail.stderr or "")

        mock_client.submit_job.side_effect = None
        mock_client.submit_job.return_value = {}
        assert io.submit("echo hi").status == SubmissionStatus.JOB_ID_UNKNOWN

        mock_client.terminate_job.side_effect = err
        cancel_fail = io.cancel(valid_id)
        assert cancel_fail.status == CancelStatus.FAILED
        assert "Queue not found" in (cancel_fail.stderr or "")

        mock_client.describe_jobs.side_effect = err
        with pytest.raises(APIError, match="Queue not found"):
            io.get_job(valid_id)
        with pytest.raises(APIError, match="Queue not found"):
            io.get_jobs_list(jobs=[valid_id])

        mock_client.list_jobs.side_effect = err
        with pytest.raises(APIError, match="Queue not found"):
            io.get_jobs_list(states=[QState.RUNNING])

        with pytest.raises(InvalidJobIDError):
            io.get_job("not-a-uuid")
        with pytest.raises(NotImplementedError, match="username"):
            io.get_jobs_list(user="alice")

        # Missing queue validation happens before boto3 client init
        io_no_queue = AWSBatchIO()
        with pytest.raises(ValueError, match="job queue"):
            io_no_queue.get_jobs_list()

        # Unmapped QState filter short-circuits without client calls
        mock_client.reset_mock()
        assert io.get_jobs_list(states=[QState.SUSPENDED]) == []
        assert io.get_jobs_list(jobs=[valid_id], states=[QState.SUSPENDED]) == []
        mock_client.list_jobs.assert_not_called()
        mock_client.describe_jobs.assert_not_called()

    def test_describe_jobs_chunking_dedup_filtering_and_pagination(self):
        pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client

        # 105 unique job IDs + duplicates -> deduplicated to 105, split into 100 + 5
        job_ids = [f"12345678-1234-1234-1234-{i:012d}" for i in range(105)]
        now_ms = int(time.time() * 1000) - 45_000
        mock_client.describe_jobs.side_effect = lambda jobs: {
            "jobs": [
                {
                    "jobId": jid,
                    "jobName": f"job-{jid[-3:]}",
                    "jobQueue": (
                        "arn:aws:batch:us-east-1:123456789012:job-queue/other-q"
                        if jid.endswith("000")
                        else "arn:aws:batch:us-east-1:123456789012:job-queue/q"
                    ),
                    "status": "SUCCEEDED" if jid.endswith("001") else "RUNNING",
                    "startedAt": now_ms,
                    "container": {"vcpus": 2, "memory": 4096},
                }
                for jid in jobs
            ]
        }

        results = io.get_jobs_list(jobs=[job_ids[0], *job_ids, job_ids[-1]])
        assert len(results) == 105
        assert mock_client.describe_jobs.call_count == 2
        assert len(mock_client.describe_jobs.call_args_list[0].kwargs["jobs"]) == 100
        assert len(mock_client.describe_jobs.call_args_list[1].kwargs["jobs"]) == 5
        # Running job runtime computed cleanly
        running_job = next(j for j in results if j.state == QState.RUNNING)
        assert running_job.runtime is not None
        assert running_job.runtime >= 40

        # Filtering by queue and states when jobs are passed explicitly
        filtered = io.get_jobs_list(
            jobs=job_ids[:3], queue="q", states=[QState.RUNNING]
        )
        assert [j.job_id for j in filtered] == [job_ids[2]]

        # Default get_jobs_list() queries all 5 active AWSBatchStates and returns [] when empty
        mock_client.reset_mock()
        mock_client.list_jobs.return_value = {"jobSummaryList": []}
        assert io.get_jobs_list() == []
        queried_statuses = [
            call.kwargs["jobStatus"] for call in mock_client.list_jobs.call_args_list
        ]
        assert queried_statuses == [
            "SUBMITTED",
            "PENDING",
            "RUNNABLE",
            "STARTING",
            "RUNNING",
        ]
        mock_client.describe_jobs.assert_not_called()

        # Pagination, deduplication, and re-filtering by states after describe_jobs
        mock_client.reset_mock()
        jid1, jid2 = job_ids[1], job_ids[2]  # jid1 is SUCCEEDED, jid2 is RUNNING
        mock_client.list_jobs.side_effect = [
            {"jobSummaryList": [{"jobId": jid1}], "nextToken": "tok-1"},
            {"jobSummaryList": [{"jobId": jid1}, {"jobId": jid2}]},
        ]
        listed = io.get_jobs_list(states=[QState.RUNNING])
        # jid1 transitioned to SUCCEEDED before describe_jobs, so it is filtered out
        assert [j.job_id for j in listed] == [jid2]
        assert mock_client.list_jobs.call_count == 2

    def test_parse_job_edge_cases(self):
        raw = {
            "jobId": "12345678-1234-1234-1234-1234567890ab",
            "jobName": "done-job",
            "jobQueue": "arn:aws:batch:us-east-1:123456789012:job-queue/gpu-queue",
            "status": "SUCCEEDED",
            "startedAt": 1_700_000_000_000,
            "stoppedAt": 1_700_000_125_000,
            "timeout": {"attemptDurationSeconds": 3600},
            "nodeProperties": {"numNodes": 2},
            "container": {
                "exitCode": 0,
                "resourceRequirements": [
                    {"type": "VCPU", "value": "0.5"},
                    {"type": "MEMORY", "value": "1024"},
                ],
            },
        }
        job = AWSBatchIO._parse_job(raw)
        assert job.queue_name == "gpu-queue"
        assert job.state == QState.DONE
        assert job.sub_state == AWSBatchState.SUCCEEDED
        assert job.exit_status == 0
        assert job.runtime == 125
        assert job.info is not None
        assert job.info.cpus is None
        assert job.info.memory == 1024 * 1024
        assert job.info.nodes == 2
        assert job.info.time_limit == 3600

        # Integer VCPU string, plain non-ARN queue, startedAt == 0 (never started)
        never_started = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890ab",
                "jobQueue": "plain-queue",
                "status": "FAILED",
                "startedAt": 0,
                "stoppedAt": 1_700_000_125_000,
                "container": {
                    "resourceRequirements": [
                        {"type": "VCPU", "value": "4"},
                        {"type": "MEMORY", "value": "bad-int"},
                    ]
                },
            }
        )
        assert never_started.queue_name == "plain-queue"
        assert never_started.runtime is None
        assert never_started.info is not None
        assert never_started.info.cpus == 4
        assert never_started.info.memory is None
        assert never_started.info.nodes == 1

        # Unknown status and container with None resourceRequirements
        unknown = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890ab",
                "status": "NEW_STATE",
                "container": {"resourceRequirements": None},
            }
        )
        assert unknown.state == QState.UNDETERMINED
        assert unknown.sub_state is None
        assert unknown.info is not None
        assert unknown.info.nodes == 1

        # Missing container -> nodes is None
        no_container = AWSBatchIO._parse_job(
            {"jobId": "12345678-1234-1234-1234-1234567890ab", "status": "SUBMITTED"}
        )
        assert no_container.info is not None
        assert no_container.info.nodes is None
