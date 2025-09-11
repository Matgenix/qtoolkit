# ruff: noqa: SLF001

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


@pytest.mark.parametrize(
    "get_host_kwargs",
    [{"type": "slurm"}, {"type": "pbs"}, {"type": "sge"}],
    ids=["slurm", "pbs", "sge"],
)
def test_submission(get_host, get_host_kwargs):
    from qtoolkit.core.data_objects import (
        CancelResult,
        CancelStatus,
        QJob,
        SubmissionResult,
        SubmissionStatus,
    )
    from qtoolkit.manager import QueueManager

    host_factory, scheduler_io = get_host

    johndoe_host = host_factory("johndoe")
    sarahking_host = host_factory("sarahking")

    qm_johndoe = QueueManager(scheduler_io=scheduler_io, host=johndoe_host)
    qm_sarahking = QueueManager(scheduler_io=scheduler_io, host=sarahking_host)

    sr = qm_johndoe.submit(commands="sleep 60", work_dir=johndoe_host.config.root_dir)
    assert isinstance(sr, SubmissionResult)
    assert sr.exit_code == 0
    assert sr.status == SubmissionStatus.SUCCESSFUL
    assert sr.stderr == ""

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

    qjob = qm_sarahking.get_job(job_id_johndoe)
    assert isinstance(qjob, QJob)
    assert qjob.job_id == job_id_johndoe

    # This should fail as the job belongs to johndoe
    cr = qm_sarahking.cancel(qjob)
    assert isinstance(cr, CancelResult)
    assert cr.exit_code != 0
    assert cr.status == CancelStatus.FAILED

    # don't check the format of the job id passed, we just want to see that we get no job
    qm_johndoe.scheduler_io.check_job_ids = False
    nojob = qm_johndoe.get_job(job="99999")
    assert nojob is None

    with pytest.raises(FileNotFoundError):
        qm_johndoe.submit(
            commands="sleep 60",
            work_dir=os.path.join(johndoe_host.config.root_dir, "nonexistingdir"),
        )

    sr = qm_johndoe.submit(
        commands="sleep 60",
        work_dir=os.path.join(johndoe_host.config.root_dir, "nonexistingdir"),
        create_submit_dir=True,
    )
    assert isinstance(sr, SubmissionResult)
    assert sr.exit_code == 0
    assert sr.status == SubmissionStatus.SUCCESSFUL
    assert sr.stderr == ""

    with pytest.raises(RuntimeError, match=r"failed to create directory"):
        qm_johndoe.submit(
            commands="sleep 60", work_dir="/home/noaccess", create_submit_dir=True
        )
