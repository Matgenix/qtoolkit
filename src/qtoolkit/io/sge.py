from __future__ import annotations

import re
import xml.dom.minidom
import xml.parsers.expat
from typing import ClassVar

from qtoolkit.core.data_objects import QJob, QJobInfo, QResources, QState, QSubState
from qtoolkit.core.exceptions import CommandFailedError, OutputParsingError
from qtoolkit.io.pbs_base import PBSIOBase

# https://wiki.nikhil.io/Ancient_Sysadmin_Stuff/Sun_Grid_Engine_States/
# https://manpages.ubuntu.com/manpages/jammy/en/man5/sge_status.5.html
# Jobs Status:
#     'qw' - Queued and waiting,
#     'w' - Job waiting,
#     's' - Job suspended,
#     't' - Job transferring and about to start,
#     'r' - Job running,
#     'h' - Job hold,
#     'R' - Job restarted,
#     'd' - Job has been marked for deletion,
#     'Eqw' - An error occurred with the job.
# 'z' - finished
#
# Category     State     SGE Letter Code
# Pending:     pending     qw
# Pending:     pending, user hold     qw
# Pending:     pending, system hold     hqw
# Pending:     pending, user and system hold     hqw
# Pending:     pending, user hold, re-queue     hRwq
# Pending:     pending, system hold, re-queue     hRwq
# Pending:     pending, user and system hold, re-queue     hRwq
# Pending:     pending, user hold     qw
# Pending:     pending, user hold     qw
# Running     running     r
# Running     transferring     t
# Running     running, re-submit     Rr
# Running     transferring, re-submit     Rt
# Suspended     job suspended     s, ts
# Suspended     queue suspended     S, tS
# Suspended     queue suspended by alarm     T, tT
# Suspended     all suspended with re-submit     Rs, Rts, RS, RtS, RT, RtT
# Error     all pending states with error     Eqw, Ehqw, EhRqw
# Deleted     all running and suspended states with deletion     dr, dt, dRr, dRt,
#                                                                ds, dS, dT, dRs,
#                                                                dRS, dRT


class SGEState(QSubState):
    # Job states
    FINISHED = "z"
    QUEUED_WAITING = "qw"
    WAITING = "w"
    JOB_SUSPENDED = "s"
    TRANSFERRING = "t"
    RUNNING = "r"
    HOLD = "hqw"
    RESTARTED = "R"
    DELETION = "d"
    ERROR_PENDING = "Eqw"
    ERROR_PENDING_HOLD = "Ehqw"
    ERROR_PENDING_HOLD_REQUEUE = "EhRqw"
    DELETION_RUNNING = "dr"
    DELETION_TRANSFERRING = "dt"
    DELETION_RUNNING_RESUBMIT = "dRr"
    DELETION_TRANSFERRING_RESUBMIT = "dRt"
    DELETION_SUSPENDED_JOB = "ds"
    DELETION_SUSPENDED_QUEUE = "dS"
    DELETION_SUSPENDED_ALARM = "dT"
    DELETION_SUSPENDED_RESUBMIT_JOB = "dRs"
    DELETION_SUSPENDED_RESUBMIT_QUEUE = "dRS"
    DELETION_SUSPENDED_RESUBMIT_ALARM = "dRT"

    @property
    def qstate(self) -> QState:
        return _STATUS_MAPPING[self]  # type: ignore


_STATUS_MAPPING = {
    SGEState.FINISHED: QState.DONE,
    SGEState.QUEUED_WAITING: QState.QUEUED,
    SGEState.WAITING: QState.QUEUED,
    SGEState.HOLD: QState.QUEUED_HELD,
    SGEState.ERROR_PENDING: QState.FAILED,
    SGEState.ERROR_PENDING_HOLD: QState.FAILED,
    SGEState.ERROR_PENDING_HOLD_REQUEUE: QState.FAILED,
    SGEState.RUNNING: QState.RUNNING,
    SGEState.TRANSFERRING: QState.RUNNING,
    SGEState.RESTARTED: QState.RUNNING,
    SGEState.JOB_SUSPENDED: QState.SUSPENDED,
    SGEState.DELETION: QState.FAILED,
    SGEState.DELETION_RUNNING: QState.FAILED,
    SGEState.DELETION_TRANSFERRING: QState.FAILED,
    SGEState.DELETION_RUNNING_RESUBMIT: QState.FAILED,
    SGEState.DELETION_TRANSFERRING_RESUBMIT: QState.FAILED,
    SGEState.DELETION_SUSPENDED_JOB: QState.SUSPENDED,
    SGEState.DELETION_SUSPENDED_QUEUE: QState.SUSPENDED,
    SGEState.DELETION_SUSPENDED_ALARM: QState.SUSPENDED,
    SGEState.DELETION_SUSPENDED_RESUBMIT_JOB: QState.SUSPENDED,
    SGEState.DELETION_SUSPENDED_RESUBMIT_QUEUE: QState.SUSPENDED,
    SGEState.DELETION_SUSPENDED_RESUBMIT_ALARM: QState.SUSPENDED,
}


class SGEIO(PBSIOBase):
    header_template: str
    header_template_file: str = "sge"

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"
    job_id_regex: str | None = r"^\d+(\.[\w.-]+)?(\[\d+(-\d+)?(,\d+)*\])?$"
    system_name: str = "SGE"
    default_unit: str = "M"
    power_labels: ClassVar[dict] = {"k": 0, "m": 1, "g": 2, "t": 3}
    _qresources_mapping: ClassVar[dict] = {
        "queue_name": "queue",
        "job_name": "job_name",
        "priority": "priority",
        "output_filepath": "qout_path",
        "error_filepath": "qerr_path",
        "project": "group_list",
    }

    def __init__(self, get_job_executable: str = "qstat"):
        super().__init__()
        self.get_job_executable = get_job_executable

    def extract_job_id(self, stdout: str) -> str | None:
        """
        Extract the job ID from the submission output.
        """
        match = re.search(r'Your job (\d+) \(".*?"\) has been submitted', stdout)
        if not match:
            raise OutputParsingError(
                "Failed to parse job ID from stdout"
            )  # pragma: no cover - trivial
        return match.group(1)

    def extract_job_id_from_cancel(self, stderr: str) -> str | None:
        """
        Extract the job ID from the cancellation output.
        """
        match = re.search(r"qdel: job (\d+) deleted", stderr)
        if not match:
            raise OutputParsingError(
                "Failed to parse job ID from stdout"
            )  # pragma: no cover - trivial
        return match.group(1)

    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        command = self._get_qstat_base_command()

        if user:
            command.append(f"-u {user}")
        else:
            # by default sge show only the jobs for the current user, to make it consistent
            # with other schedulers, we add this.
            command.append('-u "*"')

        return " ".join(command)

    def parse_job_output(
        self,
        exit_code: int,
        stdout: str | bytes,
        stderr: str | bytes,
        job_id: str | None = None,
    ) -> QJob | None:
        """
        Parse the output of the qstat command for a single job.
        """
        # aiida style
        if job_id is None:
            raise RuntimeError("job_id should be passed for sge.")
        out = self.parse_jobs_list_output(exit_code, stdout, stderr, job_ids=[job_id])
        if out:
            if len(out) == 1:
                return out[0]
            if len(out) > 1:
                raise RuntimeError(
                    "Should not happen."
                )  # pragma: no cover - should not happen
        return None

    def _get_element_text(self, parent, tag_name):
        elements = parent.getElementsByTagName(tag_name)
        if elements:
            return elements[0].childNodes[0].data.strip()
        return None

    def _safe_int(self, value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _get_qstat_base_command(self) -> list[str]:
        return ["qstat", "-ext", "-urg", "-xml"]

    def _get_job_ids_flag(self, job_ids_str: str) -> str:
        raise NotImplementedError("Querying by job IDs is not supported for SGE.")

    def _get_job_cmd(self, job_id: str):
        return " ".join(self._get_qstat_base_command()) + ' -u "*"'

    def parse_jobs_list_output(
        self,
        exit_code: int,
        stdout: str | bytes,
        stderr: str | bytes,
        job_ids: list[str] | None = None,
    ) -> list[QJob]:
        """
        Parse the output of the qstat command for a list of jobs.
        """
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()
        if exit_code != 0:
            msg = f"command {self.get_job_executable or 'qacct'} failed: {stderr}"
            raise CommandFailedError(msg)

        if not stdout:
            return []

        try:
            xmldata = xml.dom.minidom.parseString(stdout)  # noqa: S318
        except xml.parsers.expat.ExpatError as exc:
            raise OutputParsingError("XML parsing of stdout failed") from exc

        # Ensure <job_info> elements exist
        # (==> xml file created via -u option,
        # tag doesn't exist when created with -j option)
        root_element = xmldata.documentElement
        if root_element.tagName != "job_info":
            raise OutputParsingError(
                f"Unexpected root element '{root_element.tagName}', expected 'job_info'. "
                f"Ensure -u option was used for generating the xml file."
            )

        job_elements = xmldata.getElementsByTagName("job_list")

        jobs_list = []

        for job_element in job_elements:
            qjob = QJob()
            qjob.job_id = self._get_element_text(job_element, "JB_job_number")
            if job_ids is not None and qjob.job_id not in job_ids:
                continue
            job_state_string = self._get_element_text(job_element, "state")

            try:
                sge_job_state = SGEState(job_state_string)
            except ValueError as exc:  # pragma: no cover
                raise OutputParsingError(
                    f"Unknown job state {job_state_string} for job id {qjob.job_id}"
                ) from exc

            qjob.sub_state = sge_job_state
            qjob.state = sge_job_state.qstate
            qjob.username = self._get_element_text(job_element, "JB_owner")
            qjob.name = self._get_element_text(job_element, "JB_name")

            info = QJobInfo()
            info.nodes = self._safe_int(
                self._get_element_text(job_element, "num_nodes")
            )
            info.cpus = self._safe_int(self._get_element_text(job_element, "num_proc"))
            info.memory_per_cpu = self._convert_memory_str(
                self._get_element_text(job_element, "hard resource_list.mem_free")
            )
            info.partition = self._get_element_text(job_element, "queue")
            info.time_limit = self._convert_str_to_time(
                self._get_element_text(job_element, "hard resource_list.h_rt")
            )

            qjob.info = info

            jobs_list.append(qjob)

        return jobs_list

    @staticmethod
    def _convert_str_to_time(time_str: str | None) -> int | None:
        """
        Convert a string in the format used by SGE to a number of seconds.

        Parameters
        ----------
        time_str
            Time string from SGE.

        Returns
        -------
        int or None
            Time in seconds, or None if input is None.
        """
        if time_str is None:
            return None

        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = parts
        elif len(parts) == 2:
            hours, minutes = "0", parts[0]
            seconds = parts[1]
        elif len(parts) == 1:
            hours, minutes, seconds = "0", "0", parts[0]
        else:
            raise OutputParsingError(f"Invalid time format: {time_str}")

        try:
            return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        except ValueError as exc:
            raise OutputParsingError(f"Invalid time format: {time_str}") from exc

    def _add_soft_walltime(self, header_dict: dict, resources: QResources):
        header_dict["soft_walltime"] = self._convert_time_to_str(
            resources.time_limit * 0.99
        )

    def sanitize_options(self, options: dict) -> dict:
        """
        Sanitize the values in the options used to generate the header.

        Parameters
        ----------
        options
            Dictionary of options to sanitize.

        Returns
        -------
        dict
            Sanitized options.
        """
        if "job_name" in options:
            options = dict(options)
            options["job_name"] = re.sub(r"[\s/@*\\:]", "_", options["job_name"])
        return options
