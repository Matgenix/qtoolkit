# ruff: noqa: SLF001

import os
import pytest
import re


pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_submission(slurm_host):
    from qtoolkit.manager import QueueManager
    from qtoolkit.io.slurm import SlurmIO
    from qtoolkit.core.data_objects import SubmissionResult, SubmissionStatus, QJob

    qm = QueueManager(scheduler_io=SlurmIO(), host=slurm_host)

    sr = qm.submit(commands="sleep 60", work_dir="/home/qtoolkit")
    assert isinstance(sr, SubmissionResult)
    assert sr.exit_code == 0
    assert sr.status == SubmissionStatus.SUCCESSFUL
    assert sr.stderr == ""

    assert re.fullmatch(r"Submitted batch job \d+\n", sr.stdout)
    job_id = sr.job_id

    job_list = qm.get_jobs_list()
    assert len(job_list) == 1
    qjob = job_list[0]
    assert isinstance(qjob, QJob)
    assert qjob.job_id == job_id