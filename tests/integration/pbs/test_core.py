# ruff: noqa: SLF001

import os
import re

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_submission(pbs_host):
    from qtoolkit.core.data_objects import QJob, SubmissionResult, SubmissionStatus
    from qtoolkit.io.pbs import PBSIO
    from qtoolkit.manager import QueueManager

    johndoe_host = pbs_host("johndoe")
    sarahking_host = pbs_host("sarahking")

    qm_johndoe = QueueManager(scheduler_io=PBSIO(), host=johndoe_host)
    qm_sarahking = QueueManager(scheduler_io=PBSIO(), host=sarahking_host)

    sr = qm_johndoe.submit(commands="sleep 60", work_dir=johndoe_host.config.root_dir)
    assert isinstance(sr, SubmissionResult)
    assert sr.exit_code == 0
    assert sr.status == SubmissionStatus.SUCCESSFUL
    assert sr.stderr == ""
    assert re.fullmatch(r"\d+\.[\w.-]+\n", sr.stdout)

    job_id_johndoe = sr.job_id

    job_list = qm_johndoe.get_jobs_list()
    assert len(job_list) == 1
    qjob = job_list[0]
    assert isinstance(qjob, QJob)
    assert qjob.job_id == job_id_johndoe

    sr = qm_sarahking.submit(
        commands="sleep 60", work_dir=sarahking_host.config.root_dir
    )
    assert isinstance(sr, SubmissionResult)
    assert sr.exit_code == 0
    assert sr.status == SubmissionStatus.SUCCESSFUL
    assert sr.stderr == ""
    job_id_sarahking = sr.job_id

    job_list = qm_johndoe.get_jobs_list()
    assert len(job_list) == 2

    job_list = qm_johndoe.get_jobs_list(jobs=[job_id_johndoe])
    assert len(job_list) == 1
    assert job_list[0].username == "johndoe"
    job_list = qm_johndoe.get_jobs_list(jobs=[job_id_sarahking])
    assert len(job_list) == 1
    assert job_list[0].username == "sarahking"

    job_list = qm_johndoe.get_jobs_list(user="sarahking")
    assert len(job_list) == 1
    assert job_list[0].username == "sarahking"

    job_list = qm_johndoe.get_jobs_list(user="johndoe")
    assert len(job_list) == 1
    assert job_list[0].username == "johndoe"
