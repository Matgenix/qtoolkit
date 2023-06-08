import pytest

try:
    import monty
except ModuleNotFoundError:
    monty = None


from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QJob,
    QState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.io.shell import ShellIO, ShellState


@pytest.fixture(scope="module")
def shell_io():
    return ShellIO()


class TestShellState:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        shell_state = ShellState.DEFUNCT
        assert test_utils.is_msonable(shell_state)

    def test_states_list(self):
        all_states = [state.value for state in ShellState]
        assert set(all_states) == {"D", "R", "S", "T", "t", "W", "X", "Z"}

    def test_qstate(self):
        shell_state = ShellState.UNINTERRUPTIBLE_SLEEP
        assert shell_state.qstate == QState.RUNNING
        shell_state = ShellState.RUNNING
        assert shell_state.qstate == QState.RUNNING
        shell_state = ShellState.INTERRUPTIBLE_SLEEP
        assert shell_state.qstate == QState.RUNNING
        shell_state = ShellState.STOPPED
        assert shell_state.qstate == QState.SUSPENDED
        shell_state = ShellState.STOPPED_DEBUGGER
        assert shell_state.qstate == QState.SUSPENDED
        shell_state = ShellState.PAGING
        assert shell_state.qstate == QState.RUNNING
        shell_state = ShellState.DEAD
        assert shell_state.qstate == QState.DONE
        shell_state = ShellState.DEFUNCT
        assert shell_state.qstate == QState.DONE


class TestShellIO:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils, shell_io):
        assert test_utils.is_msonable(shell_io)

    def test_get_submit_cmd(self):
        shell_io = ShellIO(blocking=True)
        submit_cmd = shell_io.get_submit_cmd(script_file="myscript.sh")
        assert submit_cmd == "bash myscript.sh > stdout 2> stderr"
        shell_io = ShellIO(blocking=False)
        submit_cmd = shell_io.get_submit_cmd(script_file="myscript.sh")
        assert submit_cmd == "nohup bash myscript.sh > stdout 2> stderr & echo $!"

    def test_parse_submit_output(self, shell_io):
        sr = shell_io.parse_submit_output(exit_code=0, stdout="13647\n", stderr="")
        assert sr == SubmissionResult(
            job_id="13647",
            step_id=None,
            exit_code=0,
            stdout="13647\n",
            stderr="",
            status=SubmissionStatus.SUCCESSFUL,
        )
        sr = shell_io.parse_submit_output(exit_code=0, stdout=b"13647\n", stderr=b"")
        assert sr == SubmissionResult(
            job_id="13647",
            step_id=None,
            exit_code=0,
            stdout="13647\n",
            stderr="",
            status=SubmissionStatus.SUCCESSFUL,
        )
        sr = shell_io.parse_submit_output(exit_code=104, stdout="tata", stderr=b"titi")
        assert sr == SubmissionResult(
            job_id=None,
            step_id=None,
            exit_code=104,
            stdout="tata",
            stderr="titi",
            status=SubmissionStatus.FAILED,
        )
        sr = shell_io.parse_submit_output(exit_code=0, stdout=b"\n", stderr="")
        assert sr == SubmissionResult(
            job_id=None,
            step_id=None,
            exit_code=0,
            stdout="\n",
            stderr="",
            status=SubmissionStatus.JOB_ID_UNKNOWN,
        )

    def test_parse_cancel_output(self, shell_io):
        cr = shell_io.parse_cancel_output(exit_code=0, stdout="", stderr="")
        assert cr == CancelResult(
            job_id=None,
            step_id=None,
            exit_code=0,
            stdout="",
            stderr="",
            status=CancelStatus.SUCCESSFUL,
        )
        cr = shell_io.parse_cancel_output(
            exit_code=1,
            stdout=b"",
            stderr=b"/bin/sh: line 1: kill: (14020) - No such process\n",
        )
        assert cr == CancelResult(
            job_id=None,
            step_id=None,
            exit_code=1,
            stdout="",
            stderr="/bin/sh: line 1: kill: (14020) - No such process\n",
            status=CancelStatus.FAILED,
        )

    def test_get_job_cmd(self, shell_io):
        get_job_cmd = shell_io.get_job_cmd(123)
        assert get_job_cmd == "ps -o pid,user,etimes,state,comm -p 123"
        get_job_cmd = shell_io.get_job_cmd(456)
        assert get_job_cmd == "ps -o pid,user,etimes,state,comm -p 456"
        get_job_cmd = shell_io.get_job_cmd(QJob(job_id="789"))
        assert get_job_cmd == "ps -o pid,user,etimes,state,comm -p 789"

    def test_get_jobs_list_cmd(self, shell_io):
        get_jobs_list_cmd = shell_io.get_jobs_list_cmd(
            jobs=[QJob(job_id=125), 126, "127"], user=None
        )
        assert get_jobs_list_cmd == "ps -o pid,user,etimes,state,comm -p 125,126,127"

    def test_parse_jobs_list_output(self, shell_io):
        joblist = shell_io.parse_jobs_list_output(
            exit_code=0,
            stdout="    PID USER     ELAPSED S COMMAND\n  18092 davidwa+     465 S bash\n  18112 davidwa+     461 S bash\n",
            stderr="",
        )
        assert joblist == [
            QJob(
                job_id="18092",
                runtime=465,
                name="bash",
                state=QState.RUNNING,
                sub_state=ShellState.INTERRUPTIBLE_SLEEP,
            ),
            QJob(
                job_id="18112",
                runtime=461,
                name="bash",
                state=QState.RUNNING,
                sub_state=ShellState.INTERRUPTIBLE_SLEEP,
            ),
        ]
