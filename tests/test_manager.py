import pytest


def test_get_submission_script():
    from qtoolkit.io.slurm import SlurmIO
    from qtoolkit.manager import QueueManager

    qm = QueueManager(SlurmIO())
    submission_script = qm.get_submission_script(
        commands="run my command",
        work_dir="/working_directory",
        pre_run="echo this is a prerun",
        post_run="echo this is a postrun",
        environment={
            "modules": ["compiler", "some_library", "mycode"],
            "source_files": ["/home/local/activate"],
            "conda_environment": "my_qtoolkit_conda",
            "environ": {"VAR1": "value1", "SOMEVAR": "somevalue"},
        },
    )
    assert (
        submission_script
        == """#!/bin/bash
module purge
module load compiler
module load some_library
module load mycode
source /home/local/activate
conda activate my_qtoolkit_conda
export VAR1=value1
export SOMEVAR=somevalue
cd /working_directory
echo this is a prerun
run my command
echo this is a postrun"""
    )
    submission_script = qm.get_submission_script(
        commands="",
        work_dir="/working_directory",
        pre_run="echo this is a prerun",
        post_run="echo this is a postrun",
        environment={"conda_environment": "my_qtoolkit_conda"},
    )
    assert (
        submission_script
        == """#!/bin/bash
conda activate my_qtoolkit_conda
cd /working_directory
echo this is a prerun
echo this is a postrun"""
    )
    with pytest.raises(ValueError, match=r"commands should be a str or a list of str"):
        qm.get_submission_script(commands=None)
    submission_script = qm.get_submission_script(
        commands="",
        work_dir="/working_directory",
        pre_run="echo this is a prerun",
        post_run="echo this is a postrun",
        environment={"source_files": ["/home/local/activate"]},
    )
    assert (
        submission_script
        == """#!/bin/bash
source /home/local/activate
cd /working_directory
echo this is a prerun
echo this is a postrun"""
    )
    with pytest.raises(ValueError, match=r"commands should be a str or a list of str"):
        qm.get_submission_script(commands=None)
