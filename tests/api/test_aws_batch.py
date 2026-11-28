from __future__ import annotations

import builtins
import copy
import pickle
import time
from unittest.mock import MagicMock

import pytest

from qtoolkit.api import AWSBatchIO, AWSBatchState, api_scheduler_mapping
from qtoolkit.api.aws_batch import sanitize_job_name
from qtoolkit.core.data_objects import (
    CancelStatus,
    QCloudResources,
    QState,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import APIError

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
            shell_cmd=["/bin/bash", "-l"],
            client_kwargs={"verify": False, "config": {"read_timeout": 120}},
        )
        assert test_utils.is_msonable(io)
        io._client = object()
        assert test_utils.is_msonable(io)

    def test_pickle_and_deepcopy_after_client_creation(self, aws_batch_env):
        io, _batch, _queue_name, _job_def = aws_batch_env
        io._get_client()
        pickled = pickle.loads(pickle.dumps(io))  # noqa: S301
        for clone in (pickled, copy.deepcopy(io)):
            assert clone._client is None
            assert clone.job_queue == io.job_queue
            assert clone.get_job("00000000-0000-0000-0000-000000000000") is None

    def test_client_forwards_session_and_endpoint_options(self, monkeypatch):
        boto3 = pytest.importorskip("boto3")
        mock_session = MagicMock()
        session_cls = MagicMock(return_value=mock_session)
        monkeypatch.setattr(boto3, "Session", session_cls)

        io = AWSBatchIO(
            job_queue="q",
            job_definition="d",
            region_name="us-east-2",
            profile_name="work",
            endpoint_url="http://localhost:4566",
        )
        io._get_client()
        session_cls.assert_called_once_with(
            region_name="us-east-2", profile_name="work"
        )
        mock_session.client.assert_called_once_with(
            "batch", endpoint_url="http://localhost:4566"
        )

    def test_client_kwargs(self, monkeypatch):
        boto3 = pytest.importorskip("boto3")
        from botocore.config import Config

        mock_session = MagicMock()
        monkeypatch.setattr(boto3, "Session", MagicMock(return_value=mock_session))
        io = AWSBatchIO(
            client_kwargs={
                "verify": False,
                "endpoint_url": "http://localhost:4566",
                "config": {"retries": {"mode": "adaptive"}, "read_timeout": 120},
            }
        )
        io._get_client()
        kwargs = mock_session.client.call_args.kwargs
        assert kwargs["verify"] is False
        assert kwargs["endpoint_url"] == "http://localhost:4566"
        assert isinstance(kwargs["config"], Config)
        assert kwargs["config"].read_timeout == 120
        # The stored kwargs keep the dict, so the object stays serializable.
        assert io.client_kwargs["config"] == {
            "retries": {"mode": "adaptive"},
            "read_timeout": 120,
        }

        with pytest.raises(ValueError, match="aws_secret_access_key"):
            AWSBatchIO(client_kwargs={"aws_secret_access_key": "x"})
        with pytest.raises(ValueError, match="region_name"):
            AWSBatchIO(client_kwargs={"region_name": "us-east-1"})
        with pytest.raises(ValueError, match="endpoint_url"):
            AWSBatchIO(
                endpoint_url="http://a", client_kwargs={"endpoint_url": "http://b"}
            )

    def test_client_kwargs_not_changed_by_client_creation(self):
        pytest.importorskip("boto3")
        client_kwargs = {"config": {"retries": {"max_attempts": 3}}}
        io = AWSBatchIO(region_name="us-east-1", client_kwargs=client_kwargs)
        io._get_client()
        assert client_kwargs == {"config": {"retries": {"max_attempts": 3}}}
        assert io.client_kwargs == client_kwargs

    def test_client_creation_error_raises_api_error(self, monkeypatch, tmp_path):
        pytest.importorskip("boto3")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "creds"))
        io = AWSBatchIO(job_queue="q", job_definition="d", profile_name="missing")
        with pytest.raises(APIError, match="Failed to create the AWS Batch client"):
            io.submit("echo hi")

    def test_sanitize_job_name(self):
        assert sanitize_job_name("Force field static 1/2") == "Force_field_static_1_2"
        assert sanitize_job_name("_hidden") == "job__hidden"
        assert sanitize_job_name("  my job  ") == "my_job"
        assert len(sanitize_job_name("a" * 200)) == 128

        io = AWSBatchIO(job_queue="q", job_definition="d")
        io._client = MagicMock()
        io._client.submit_job.return_value = {"jobId": "x"}
        io.submit("echo hi", options={"jobName": "relax 1/2"})
        assert io._client.submit_job.call_args.kwargs["jobName"] == "relax_1_2"

    def test_missing_boto3_raises_helpful_error(self, monkeypatch):
        io = AWSBatchIO(job_queue="q", job_definition="d")
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "boto3" or name.startswith("botocore"):
                raise ModuleNotFoundError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="qtoolkit\\[aws\\]"):
            io.submit("echo hi")

    def test_format_command(self):
        io = AWSBatchIO()
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
        io_login = AWSBatchIO(shell_cmd=["/bin/bash", "-l"])
        assert io_login.format_command("pwd") == ["/bin/bash", "-l", "-c", "pwd"]
        with pytest.raises(ValueError, match="shell_cmd"):
            AWSBatchIO(shell_cmd=None)  # type: ignore[arg-type]

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

        job = io.get_job(sub.job_id)
        assert job is not None
        assert job.job_id == sub.job_id
        assert job.name == "phonon-run"
        assert job.queue_name == queue_name
        assert job.state == QState.DONE
        assert job.sub_state == AWSBatchState.SUCCEEDED
        assert job.info is not None
        # moto ignores the resourceRequirements overrides and reports the job
        # definition values (2 vCPUs, 2048 MiB). The override payload itself is
        # checked in test_convert_resources_and_merge.
        assert job.info.cpus == 2
        assert job.info.memory == 2048 * 1024
        assert job.info.time_limit == 1800

        raw = batch.describe_jobs(jobs=[sub.job_id])["jobs"][0]
        assert raw["container"]["command"] == ["/bin/bash", "-c", "echo hello"]
        assert {"name": "OMP_NUM_THREADS", "value": "4"} in raw["container"][
            "environment"
        ]

        assert [j.job_id for j in io.get_jobs_list(jobs=[sub.job_id])] == [sub.job_id]
        assert [j.job_id for j in io.get_jobs_list(jobs=[job])] == [sub.job_id]
        by_queue = io.get_jobs_list(queue=queue_name, states=list(QState))
        assert any(j.job_id == sub.job_id for j in by_queue)

    def test_submit_without_commands_and_cancel_with_moto(self, aws_batch_env):
        io, batch, _queue_name, _job_def = aws_batch_env
        # The command given in the options runs as it is, without a shell.
        sub = io.submit(
            None,
            options={"containerOverrides": {"command": ["python", "-c", "print(1)"]}},
        )
        assert sub.status == SubmissionStatus.SUCCESSFUL
        raw = batch.describe_jobs(jobs=[sub.job_id])["jobs"][0]
        assert raw["container"]["command"] == ["python", "-c", "print(1)"]

        # Without any command, the job runs the command of the job definition.
        sub_default = io.submit(None)
        assert sub_default.status == SubmissionStatus.SUCCESSFUL
        raw = batch.describe_jobs(jobs=[sub_default.job_id])["jobs"][0]
        assert raw["container"]["command"] == ["echo", "default"]

        cancel_res = io.cancel(sub.job_id, reason="no longer needed")
        assert cancel_res.status == CancelStatus.SUCCESSFUL
        assert cancel_res.job_id == sub.job_id

    def test_submit_rejected_with_moto(self, aws_batch_env):
        io, _batch, _queue_name, _job_def = aws_batch_env
        sub = io.submit("echo hi", options={"jobDefinition": "missing-def"})
        assert sub.status == SubmissionStatus.FAILED
        assert "ClientException" in sub.stderr

    def test_get_job_not_found_with_moto(self, aws_batch_env):
        io, _batch, _queue_name, _job_def = aws_batch_env
        assert io.get_job("00000000-0000-0000-0000-000000000000") is None

    def test_submit_without_queue_or_definition(self):
        with pytest.raises(ValueError, match="jobQueue"):
            AWSBatchIO().submit("echo hi")
        with pytest.raises(ValueError, match="jobDefinition"):
            AWSBatchIO(job_queue="q").submit("echo hi")
        with pytest.raises(TypeError, match="containerOverrides"):
            AWSBatchIO(job_queue="q", job_definition="d").submit(
                "echo hi", options={"containerOverrides": "bad"}
            )

    @pytest.mark.parametrize(
        "override_key",
        [
            "arrayProperties",
            "nodeOverrides",
            "eksPropertiesOverride",
            "ecsPropertiesOverride",
        ],
    )
    def test_submit_rejects_multi_container_overrides(self, override_key):
        io = AWSBatchIO(job_queue="q", job_definition="d")
        with pytest.raises(ValueError, match=override_key):
            io.submit("echo hi", options={override_key: {"k": "v"}})

    def test_convert_resources_and_merge(self):
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
        converted = io.check_convert_qresources(res)
        assert converted["jobQueue"] == "override-q"
        assert converted["jobName"] == "my-job"
        assert converted["timeout"] == {"attemptDurationSeconds": 600}
        assert converted["retryStrategy"] == {"attempts": 3}
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

        # Whole-number float vcpus formats as integer string, and None containerOverrides is ignored
        whole = io.check_convert_qresources(
            QCloudResources(
                vcpus=2.0,
                gpus=1,
                scheduler_kwargs={"containerOverrides": None},
            )
        )
        whole_reqs = {
            r["type"]: r["value"]
            for r in whole["containerOverrides"]["resourceRequirements"]
        }
        assert whole_reqs == {"VCPU": "2", "GPU": "1"}

        # AWS Batch range checks and non-dict validation
        with pytest.raises(ValueError, match=">= 60"):
            io.check_convert_qresources(QCloudResources(time_limit=30))
        assert io.check_convert_qresources(QCloudResources(time_limit=60))[
            "timeout"
        ] == {"attemptDurationSeconds": 60}
        with pytest.raises(ValueError, match="max_retries <= 9"):
            io.check_convert_qresources(QCloudResources(max_retries=10))

    def test_mocked_submit_fallbacks_and_cancel(self):
        pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client

        valid_id = "12345678-1234-1234-1234-1234567890ab"
        mock_client.submit_job.return_value = {"jobId": valid_id}

        # jobQueue=None and a blank jobName fall back to the defaults.
        sub = io.submit(
            ["echo step1", "echo step2"],
            options={"jobQueue": None, "jobName": "   "},
        )
        assert sub.status == SubmissionStatus.SUCCESSFUL
        sent = mock_client.submit_job.call_args.kwargs
        assert sent["jobQueue"] == "q"
        assert sent["jobDefinition"] == "d"
        assert sent["jobName"] == "qtoolkit-job"
        assert sent["containerOverrides"]["command"] == [
            "/bin/bash",
            "-c",
            "echo step1\necho step2",
        ]

        # A command in both commands and the options is ambiguous.
        with pytest.raises(ValueError, match="not both"):
            io.submit("echo hi", options={"containerOverrides": {"command": ["ls"]}})

        # Without commands and without a command in the options, no command
        # override is sent.
        io.submit(None, options={"containerOverrides": {"environment": []}})
        assert (
            "command"
            not in mock_client.submit_job.call_args.kwargs["containerOverrides"]
        )
        io.submit(None, options={"containerOverrides": {}})
        assert "containerOverrides" not in mock_client.submit_job.call_args.kwargs

        mock_client.submit_job.return_value = {}
        assert io.submit("echo hi").status == SubmissionStatus.JOB_ID_UNKNOWN

        io.cancel(valid_id)
        mock_client.terminate_job.assert_called_with(
            jobId=valid_id, reason="Cancelled by qtoolkit"
        )
        io.cancel(valid_id, reason="preempted")
        mock_client.terminate_job.assert_called_with(jobId=valid_id, reason="preempted")

    @pytest.mark.parametrize(
        "error_kind",
        [
            "rejected",
            "param",
            "server",
            "throttled",
            "credentials",
            "connection",
            "timeout",
        ],
    )
    def test_boto_errors_mapped_across_operations(self, error_kind):
        botocore_exc = pytest.importorskip("botocore.exceptions")

        def client_error(code):
            return botocore_exc.ClientError(
                {"Error": {"Code": code, "Message": "msg"}}, "SubmitJob"
            )

        err = {
            "rejected": lambda: client_error("ClientException"),
            "param": lambda: botocore_exc.ParamValidationError(report="bad"),
            "server": lambda: client_error("ServerException"),
            "throttled": lambda: client_error("TooManyRequestsException"),
            "credentials": botocore_exc.NoCredentialsError,
            "connection": lambda: botocore_exc.EndpointConnectionError(
                endpoint_url="u"
            ),
            "timeout": lambda: botocore_exc.ReadTimeoutError(endpoint_url="u"),
        }[error_kind]()
        rejected = error_kind in ("rejected", "param")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client
        valid_id = "12345678-1234-1234-1234-1234567890ab"

        # Only a request rejected by AWS Batch is a FAILED result. Other errors
        # raise, since the request may have been accepted or may work later.
        mock_client.submit_job.side_effect = err
        mock_client.terminate_job.side_effect = err
        if rejected:
            sub_fail = io.submit("echo hi")
            assert sub_fail.status == SubmissionStatus.FAILED
            assert sub_fail.job_id is None
            assert io.cancel(valid_id).status == CancelStatus.FAILED
        else:
            with pytest.raises(APIError, match="Failed to submit") as exc_info:
                io.submit("echo hi")
            assert exc_info.value.__cause__ is err
            with pytest.raises(APIError, match="Failed to cancel"):
                io.cancel(valid_id)

        mock_client.describe_jobs.side_effect = err
        with pytest.raises(APIError):
            io.get_job(valid_id)
        with pytest.raises(APIError):
            io.get_jobs_list(jobs=[valid_id])

        mock_client.list_jobs.side_effect = err
        with pytest.raises(APIError):
            io.get_jobs_list(states=[QState.RUNNING])

    def test_get_jobs_list_validation_and_short_circuits(self):
        pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client
        valid_id = "12345678-1234-1234-1234-1234567890ab"

        with pytest.raises(NotImplementedError, match="username"):
            io.get_jobs_list(user="alice")
        with pytest.raises(ValueError, match="Set job_queue"):
            AWSBatchIO().get_jobs_list()

        assert io.get_jobs_list(states=[QState.SUSPENDED]) == []
        assert io.get_jobs_list(jobs=[valid_id], states=[QState.SUSPENDED]) == []
        mock_client.list_jobs.assert_not_called()
        mock_client.describe_jobs.assert_not_called()

        # Queue override works even when AWSBatchIO(job_queue=None)
        io_no_q = AWSBatchIO()
        io_no_q._client = mock_client
        mock_client.list_jobs.return_value = {"jobSummaryList": []}
        assert io_no_q.get_jobs_list(queue="override-q") == []
        assert mock_client.list_jobs.call_args.kwargs["jobQueue"] == "override-q"

    def test_describe_jobs_chunking_dedup_and_filters(self):
        pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client

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
        running_job = next(j for j in results if j.state == QState.RUNNING)
        assert running_job.runtime is not None
        assert running_job.runtime >= 40

        # Filtering by queue ARN and state
        filtered = io.get_jobs_list(
            jobs=job_ids[:3],
            queue="arn:aws:batch:us-east-1:123456789012:job-queue/q",
            states=[QState.RUNNING],
        )
        assert [j.job_id for j in filtered] == [job_ids[2]]

    def test_list_jobs_pagination_and_state_filter(self):
        pytest.importorskip("botocore.exceptions")
        io = AWSBatchIO(job_queue="q", job_definition="d")
        mock_client = MagicMock()
        io._client = mock_client

        jid_done = "12345678-1234-1234-1234-000000000001"
        jid_run = "12345678-1234-1234-1234-000000000002"
        mock_client.describe_jobs.return_value = {
            "jobs": [
                {"jobId": jid_done, "status": "SUCCEEDED", "jobQueue": "q"},
                {"jobId": jid_run, "status": "RUNNING", "jobQueue": "q"},
            ]
        }

        # State filter with two pages and a job ID listed on both.
        mock_client.list_jobs.side_effect = [
            {"jobSummaryList": [{"jobId": jid_done}], "nextToken": "tok-1"},
            {"jobSummaryList": [{"jobId": jid_done}, {"jobId": jid_run}]},
        ]
        listed = io.get_jobs_list(states=[QState.RUNNING])
        assert [j.job_id for j in listed] == [jid_run]
        assert mock_client.list_jobs.call_count == 2
        assert mock_client.list_jobs.call_args_list[1].kwargs["nextToken"] == "tok-1"

        # Default active listing filters out jobs that finished between list_jobs and describe_jobs
        mock_client.reset_mock()
        mock_client.list_jobs.side_effect = [
            {"jobSummaryList": []},
            {"jobSummaryList": []},
            {"jobSummaryList": []},
            {"jobSummaryList": []},
            {"jobSummaryList": [{"jobId": jid_done}, {"jobId": jid_run}]},
        ]
        assert [j.job_id for j in io.get_jobs_list()] == [jid_run]

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
        assert job.state_reason is None

        # statusReason and container.reason are both kept, without repeats.
        oom = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890af",
                "status": "FAILED",
                "statusReason": "Essential container in task exited",
                "container": {
                    "exitCode": 137,
                    "reason": "OutOfMemoryError: Container killed due to memory usage",
                },
            }
        )
        assert oom.state_reason == (
            "Essential container in task exited; "
            "OutOfMemoryError: Container killed due to memory usage"
        )
        spot = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890a0",
                "status": "FAILED",
                "statusReason": "Host EC2 (instance i-0abc) terminated.",
                "container": {"reason": "Host EC2 (instance i-0abc) terminated."},
            }
        )
        assert spot.state_reason == "Host EC2 (instance i-0abc) terminated."

        # startedAt after stoppedAt gives runtime 0. STARTING has no runtime.
        skewed = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890ac",
                "status": "FAILED",
                "startedAt": 2_000_000,
                "stoppedAt": 1_000_000,
                "container": {"exitCode": 1},
            }
        )
        assert skewed.runtime == 0

        starting = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890ad",
                "status": "STARTING",
                "startedAt": 1_700_000_000_000,
            }
        )
        assert starting.state == QState.QUEUED
        assert starting.runtime is None

        unknown = AWSBatchIO._parse_job(
            {"jobId": "12345678-1234-1234-1234-1234567890ae", "status": "NEW_STATUS"}
        )
        assert unknown.state == QState.UNDETERMINED
        assert unknown.sub_state is None

    def test_resource_bounds(self):
        convert = AWSBatchIO(job_queue="q", job_definition="d").check_convert_qresources
        assert convert(QCloudResources(max_retries=9))["retryStrategy"] == {
            "attempts": 10
        }
        assert convert(QCloudResources(max_retries=0))["retryStrategy"] == {
            "attempts": 1
        }

    def test_parse_job_memory_from_resource_requirements(self):
        job = AWSBatchIO._parse_job(
            {
                "jobId": "12345678-1234-1234-1234-1234567890ab",
                "status": "RUNNING",
                "container": {
                    "memory": 2048,
                    "vcpus": 2,
                    "resourceRequirements": [{"type": "MEMORY", "value": "4096"}],
                },
            }
        )
        assert job.info.memory == 4096 * 1024
        assert job.info.cpus == 2
        assert job.info.nodes == 1
