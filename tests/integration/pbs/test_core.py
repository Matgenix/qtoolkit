# ruff: noqa: SLF001

import os

import pytest

pytestmark = [
    pytest.mark.skipif(
        not os.environ.get("CI"),
        reason="Only run integration tests in CI, unless forced with 'CI' env var",
    ),
    pytest.mark.usefixtures("skip_if_no_pbs"),
]


def test_errors(pbs_host):
    from qtoolkit.core.exceptions import CommandFailedError, InvalidJobIDError
    from qtoolkit.io.pbs import PBSIO
    from qtoolkit.manager import QueueManager

    pbs_io = PBSIO()
    johndoe_host = pbs_host("johndoe")

    qm = QueueManager(host=johndoe_host, scheduler_io=pbs_io)

    stdout, stderr, returncode = qm.execute_cmd("qstat blabli")
    assert "qstat: Unknown queue destination blabli" in stderr

    stdout, stderr, returncode = qm.execute_cmd("qstat -b")
    assert "qstat: invalid option -- 'b'" in stderr

    with pytest.raises(
        InvalidJobIDError, match=r"Job ID \'a wrong id\' is invalid for this scheduler"
    ):
        qm.get_jobs_list(["a wrong id"])

    pbs_io.check_job_ids = False

    with pytest.raises(CommandFailedError):
        qm.get_jobs_list(["a wrong id"])

    number = "9999999"
    jobs_list = qm.get_jobs_list([number])
    assert jobs_list == []

    pbs_io.check_job_ids = True

    sr = qm.submit("sleep 5", work_dir=johndoe_host.config.root_dir)
    job_id = sr.job_id
    suffix = job_id.split(".", 1)[1]

    jobs_list = qm.get_jobs_list([job_id])
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 1
    assert jobs_list[0].job_id == job_id

    jobs_list = qm.get_jobs_list([f"9999999.{suffix}"])
    assert jobs_list == []

    import time

    time.sleep(10)
    jobs_list = qm.get_jobs_list([job_id])
    assert jobs_list == []
