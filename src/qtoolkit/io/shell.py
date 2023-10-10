from __future__ import annotations

from pathlib import Path

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QJob,
    QResources,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import (
    CommandFailedError,
    OutputParsingError,
    UnsupportedResourcesError,
)
from qtoolkit.io.base import BaseSchedulerIO

# States in from ps command, extracted from man ps.
# D    uninterruptible sleep (usually IO)
# R    running or runnable (on run queue)
# S    interruptible sleep (waiting for an event to complete)
# T    stopped by job control signal
# t    stopped by debugger during the tracing
# W    paging (not valid since the 2.6.xx kernel)
# X    dead (should never be seen)
# Z    defunct ("zombie") process, terminated but not reaped by its parent


class ShellState(QSubState):
    UNINTERRUPTIBLE_SLEEP = "D"
    RUNNING = "R"
    INTERRUPTIBLE_SLEEP = "S"
    STOPPED = "T"
    STOPPED_DEBUGGER = "t"
    PAGING = "W"
    DEAD = "X"
    DEFUNCT = "Z"

    @property
    def qstate(self) -> QState:
        return _STATUS_MAPPING[self]  # type: ignore


_STATUS_MAPPING = {
    ShellState.UNINTERRUPTIBLE_SLEEP: QState.RUNNING,
    ShellState.RUNNING: QState.RUNNING,
    ShellState.INTERRUPTIBLE_SLEEP: QState.RUNNING,
    ShellState.STOPPED: QState.SUSPENDED,
    ShellState.STOPPED_DEBUGGER: QState.SUSPENDED,
    ShellState.PAGING: QState.RUNNING,
    ShellState.DEAD: QState.DONE,
    ShellState.DEFUNCT: QState.DONE,  # TODO should be failed?
}


class ShellIO(BaseSchedulerIO):
    header_template: str = """
exec > $${qout_path}
exec 2> $${qerr_path}

echo $${job_name}
$${qverbatim}
"""

    CANCEL_CMD: str | None = "kill -9"

    def __init__(self, blocking=False, stdout_path="stdout", stderr_path="stderr"):
        """Construct the ShellIO object.

        Parameters
        ----------
        blocking: bool
            Whether the execution should be blocking.
        stdout_path: str or Path
            Path to the standard output file.
        stderr_path: str or Path
            Path to the standard error file.
        """
        self.blocking = blocking
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path

    def get_submit_cmd(self, script_file: str | Path | None = "submit.script") -> str:
        """
        Get the command used to submit a given script to the queue.

        Parameters
        ----------
        script_file: str or Path
            Path of the script file to use.
        """
        script_file = script_file or ""

        # nohup and the redirection of the outputs is needed when running through fabric
        # see https://www.fabfile.org/faq.html#why-can-t-i-run-programs-in-the-background-with-it-makes-fabric-hang  # noqa
        command = f"bash {script_file} > {self.stdout_path} 2> {self.stderr_path}"
        if not self.blocking:
            command = f"nohup {command} & echo $!"
        return command

    def parse_submit_output(self, exit_code, stdout, stderr) -> SubmissionResult:
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()
        if exit_code != 0:
            return SubmissionResult(
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                status=SubmissionStatus("FAILED"),
            )
        job_id = stdout.strip() or None
        status = (
            SubmissionStatus("SUCCESSFUL")
            if job_id
            else SubmissionStatus("JOB_ID_UNKNOWN")
        )
        return SubmissionResult(
            job_id=job_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def parse_cancel_output(self, exit_code, stdout, stderr) -> CancelResult:
        """Parse the output of the kill command."""
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()
        if exit_code != 0:
            return CancelResult(
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                status=CancelStatus("FAILED"),
            )

        status = CancelStatus("SUCCESSFUL")
        return CancelResult(
            job_id=None,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def _get_job_cmd(self, job_id: str):

        cmd = self._get_jobs_list_cmd(job_ids=[job_id])

        return cmd

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        """Parse the output of the ps command and return the corresponding QJob object.

        If the ps command returns multiple shell jobs, only the first corresponding
        QJob is returned.
        #TODO: should we check that there is only one job here ?

        Parameters
        ----------
        exit_code : int
            Exit code of the ps command.
        stdout : str
            Standard output of the ps command.
        stderr : str
            Standard error of the ps command.
        """
        out = self.parse_jobs_list_output(exit_code, stdout, stderr)
        if out:
            return out[0]
        return None

    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:

        if user and job_ids:
            msg = (
                "Cannot query by user and job(s) with ps, "
                "as the user option will override the ids list"
            )
            raise ValueError(msg)

        # use etime instead of etimes for compatibility
        command = [
            "ps",
            "-o pid,user,etime,state,comm",
        ]

        if user:
            command.append(f"-U {user}")

        if job_ids:
            command.append("-p " + ",".join(job_ids))

        return " ".join(command)

    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        """Parse the output of the ps command to list jobs.

        Parameters
        ----------
        exit_code : int
            Exit code of the ps command.
        stdout : str
            Standard output of the ps command.
        stderr : str
            Standard error of the ps command.
        """
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        # if asking only for pid that are not running the exit code is != 0,
        # so check also on stderr for failing
        if exit_code != 0 and stderr.strip():
            msg = f"command ps failed: stdout: {stdout}. stderr: {stderr}"
            raise CommandFailedError(msg)

        jobs_list = []
        for row in stdout.splitlines()[1:]:
            if not row.strip():
                continue

            data = row.split()

            qjob = QJob()
            qjob.job_id = data[0]
            qjob.username = data[1]
            qjob.runtime = self._convert_str_to_time(data[2])
            qjob.name = data[4]

            try:
                shell_job_state = ShellState(data[3][0])
            except ValueError:
                msg = f"Unknown job state {data[3]} for job id {qjob.job_id}"
                raise OutputParsingError(msg)
            qjob.sub_state = shell_job_state
            qjob.state = shell_job_state.qstate

            jobs_list.append(qjob)

        return jobs_list

    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a QResources instance to a dict that will be used to fill in the
        header of the submission script.
        Not implemented for ShellIO
        """
        raise UnsupportedResourcesError  # pragma: no cover

    @property
    def supported_qresources_keys(self) -> list:
        """
        List of attributes of QResources that are correctly handled by the
        _convert_qresources method. It is used to validate that the user
        does not pass an unsupported value, expecting to have an effect.
        """
        return []

    @staticmethod
    def _convert_str_to_time(time_str: str | None) -> int | None:
        """
        Convert a string in the format used in etime [[DD-]hh:]mm:ss to a
        number of seconds.
        """

        if not time_str:
            return None

        time_split = time_str.split(":")

        days = hours = 0

        try:
            if "-" in time_split[0]:
                split_day = time_split[0].split("-")
                days = int(split_day[0])
                time_split = [split_day[1]] + time_split[1:]

            if len(time_split) == 3:
                hours, minutes, seconds = (int(v) for v in time_split)
            elif len(time_split) == 2:
                minutes, seconds = (int(v) for v in time_split)
            else:
                raise OutputParsingError()

        except ValueError:
            raise OutputParsingError()

        return days * 86400 + hours * 3600 + minutes * 60 + seconds
