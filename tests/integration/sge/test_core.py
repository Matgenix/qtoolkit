# ruff: noqa: SLF001

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_errors(sge_host):
    from qtoolkit.core.exceptions import CommandFailedError, InvalidJobIDError
    from qtoolkit.io.sge import SGEIO
    from qtoolkit.manager import QueueManager

    sge_io = SGEIO()
    johndoe_host = sge_host("johndoe")

    qm = QueueManager(host=johndoe_host, scheduler_io=sge_io)

    stdout, stderr, returncode = qm.execute_cmd("qstat blabli")
    assert 'invalid option argument "blabli"' in stderr
    with pytest.raises(
        CommandFailedError,
        match=r"command qstat failed\:[\s\S]*SGE[\s\S]*usage\: qstat \[options\][\s\S]*invalid option argument \"blabli\"",
    ):
        qm.scheduler_io.parse_job_output(
            exit_code=returncode, stdout=stdout, stderr=stderr, job_id="dummy_id"
        )

    with pytest.raises(
        InvalidJobIDError, match=r"Job ID 'tatiana' is invalid for this scheduler"
    ):
        qm.get_job("tatiana")

    with pytest.raises(
        InvalidJobIDError, match=r"Job ID 'toutou' is invalid for this scheduler"
    ):
        qm.get_jobs_list(["toutou", "youtou"])

    stdout, stderr, returncode = qm.execute_cmd("qstat --bad")
    assert 'invalid option argument "--bad"' in stderr
    with pytest.raises(
        CommandFailedError,
        match=r"command qstat failed\:[\s\S]*SGE[\s\S]*usage\: qstat \[options\][\s\S]*invalid option argument \"--bad\"",
    ):
        qm.scheduler_io.parse_jobs_list_output(
            exit_code=returncode, stdout=stdout, stderr=stderr, job_ids=[]
        )
