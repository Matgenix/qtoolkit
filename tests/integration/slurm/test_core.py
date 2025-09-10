# ruff: noqa: SLF001

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_submission(slurm_host):
    from qtoolkit.core.exceptions import CommandFailedError, InvalidJobIDError
    from qtoolkit.io.slurm import SlurmIO
    from qtoolkit.manager import QueueManager

    slurm_io = SlurmIO()
    johndoe_host = slurm_host("johndoe")

    qm = QueueManager(host=johndoe_host, scheduler_io=slurm_io)

    stdout, stderr, returncode = qm.execute_cmd("scontrol toto")
    assert stderr == "invalid keyword: toto\n"
    with pytest.raises(
        CommandFailedError, match=r"command scontrol failed: invalid keyword: toto"
    ):
        qm.scheduler_io.parse_job_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    with pytest.raises(
        InvalidJobIDError, match=r"Job ID 'tatiana' is invalid for this scheduler"
    ):
        qm.get_job("tatiana")

    with pytest.raises(
        InvalidJobIDError, match=r"Job ID 'toutou' is invalid for this scheduler"
    ):
        qm.get_jobs_list(["toutou", "youtou"])
