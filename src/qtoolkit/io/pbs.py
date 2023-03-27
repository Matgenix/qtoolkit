from __future__ import annotations

import re

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QJob,
    QJobInfo,
    QResources,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import OutputParsingError
from qtoolkit.io.base import BaseSchedulerIO

# States in PBS from qstat's man.
# B  Array job: at least one subjob has started.
#
# E  Job is exiting after having run.
#
# F  Job is finished.
#
# H  Job is held.
#
# M  Job was moved to another server.
#
# Q  Job is queued.
#
# R  Job is running.
#
# S  Job is suspended.
#
# T  Job is being moved to new location.
#
# U  Cycle-harvesting job is suspended due to keyboard activity.
#
# W  Job is waiting for its submitter-assigned start time to be reached.
#
# X  Subjob has completed execution or has been deleted.


class PBSState(QSubState):
    ARRAY_RUNNING = "B"
    EXITING = "E"
    FINISHED = "F"
    HELD = "H"
    MOVED = "M"
    QUEUED = "Q"
    RUNNING = "R"
    SUSPENDED = "S"
    TRANSITING = "T"
    SUSPENDED_KEYBOARD = "U"
    WAITING = "W"
    ARRAY_FINISHED = "X"

    @property
    def qstate(self) -> QState:
        return _STATUS_MAPPING[self]  # type: ignore


_STATUS_MAPPING = {
    PBSState.ARRAY_RUNNING: QState.RUNNING,
    PBSState.EXITING: QState.RUNNING,
    PBSState.FINISHED: QState.DONE,
    PBSState.HELD: QState.QUEUED_HELD,
    PBSState.MOVED: QState.REQUEUED,
    PBSState.QUEUED: QState.QUEUED,
    PBSState.RUNNING: QState.RUNNING,
    PBSState.SUSPENDED: QState.SUSPENDED,
    PBSState.TRANSITING: QState.REQUEUED,
    PBSState.SUSPENDED_KEYBOARD: QState.SUSPENDED,
    PBSState.WAITING: QState.QUEUED,
    PBSState.ARRAY_FINISHED: QState.DONE,
}


class PBSIO(BaseSchedulerIO):
    header_template: str = """
#!/bin/bash
#PBS -q $${queue}
#PBS -N $${job_name}
#PBS -A $${account}
#PBS -l $${select}
#PBS -l walltime=$${walltime}
#PBS -l model=$${model}
#PBS -l place=$${place}
#PBS -W group_list=$${group_list}
#PBS -M $${mail_user}
#PBS -m $${mail_type}
#PBS -o $${qout_path}
#PBS -e $${qerr_path}
#PBS -p $${priority}
$${qverbatim}"""

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"

    def __int__(
        self, get_job_executable: str = "scontrol", split_separator: str = "<><>"
    ):
        self.get_job_executable = get_job_executable
        self.split_separator = split_separator

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
        job_id = stdout.strip()
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
        """Parse the output of the scancel command."""
        # Possible error messages:
        # qdel: Unknown Job Id 100
        # qdel: Job has finished 1004
        # Correct execution: no output
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

        # PBS does not return the job id if the job is successfully deleted
        status = CancelStatus("SUCCESSFUL")
        return CancelResult(
            job_id=None,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def _get_job_cmd(self, job_id: str):

        cmd = f"qstat -f {job_id}"

        return cmd

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        out = self.parse_jobs_list_output(exit_code, stdout, stderr)
        if out:
            return out[0]
        return None

    def _get_jobs_list_cmd(self, job_ids: list[str] | None, user: str | None) -> str:

        if user and job_ids:
            raise ValueError("Cannot query by user and job(s) in PBS")

        command = [
            "qstat",
            "-f",
        ]

        if user:
            command.append(f"-u {user}")

        if job_ids:

            command.append(" ".join(job_ids))

        return " ".join(command)

    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:

        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        # if some jobs of the list do not exist the exit code is not zero, but
        # the data for other jobs is still present. Some the exit code is ignored here

        # The error messages are included in the stderr and could be of the form:
        # qstat: Unknown Job Id 10000.c2cf5fbe1102
        # qstat: 1008.c2cf5fbe1102 Job has finished, use -x or -H to
        #   obtain historical job information
        # TODO raise if these two kinds of error are not present and exit_code != 0?

        # Split by the beginning of "Job Id:" and iterate on the different chunks.
        # Matching the beginning of the line to avoid problems in case the "Job Id"
        # string is present elsewhere.
        jobs_chunks = re.split(r"^\s*Job Id: ", stdout, flags=re.MULTILINE)

        # regex to split the key-values pairs separated by " = "
        # Explanation:
        #  - \s*([A-Za-z_.]+)\s+=\s+ matches the key in the key-value pair,
        #       allowing for leading and trailing whitespace before and after the
        #       equals sign, and allowing for a dot in the key.
        #  - ([\s\S]*?) matches the value in the key-value pair, allowing for any
        #       character including newlines.
        #  - (?=\n\s*[A-Za-z_.]+\s+=|\Z) is a positive lookahead that matches a
        #       newline followed by a key with optional leading and trailing
        #       whitespace and an equals sign or the end of the string,
        #       without including the lookahead match in the result.
        # The key_pattern is separated in case needs to be updated.
        key_pattern = r"[A-Za-z_.]+"
        values_regex = re.compile(
            rf"\s*({key_pattern})\s+=\s+([\s\S]*?)(?=\n\s*{key_pattern}\s+=|\Z)"
        )

        jobs_list = []
        for chunk in jobs_chunks:
            chunk = chunk.strip()
            if not chunk:
                continue

            # first line is the id:
            job_id, chunk_data = chunk.split("\n", 1)
            job_id = job_id.strip()
            results = values_regex.findall(chunk_data)
            if not results:
                continue
            data = dict(results)

            qjob = QJob()
            qjob.job_id = job_id

            job_state_string = data["job_state"]

            try:
                pbs_job_state = PBSState(job_state_string)
            except ValueError:
                msg = f"Unknown job state {job_state_string} for job id {qjob.job_id}"
                raise OutputParsingError(msg)
            qjob.sub_state = pbs_job_state
            qjob.state = pbs_job_state.qstate

            qjob.username = data["Job_Owner"]

            info = QJobInfo()

            try:
                info.nodes = int(data.get("Resource_List.nodect"))
            except ValueError:
                info.nodes = None

            try:
                info.cpus = int(data.get("Resource_List.ncpus"))
            except ValueError:
                info.cpus = None

            try:
                info.memory_per_cpu = self._convert_memory_str(
                    data.get("Resource_List.mem")
                )
            except OutputParsingError:
                info.memory_per_cpu = None

            info.partition = data["queue"]

            # TODO here _convert_time_str can raise. If parsing errors are accepted
            # handle differently
            info.time_limit = self._convert_time_str(data.get("Resource_List.walltime"))

            try:
                runtime_str = data.get("resources_used.walltime")
                if runtime_str:
                    qjob.runtime = self._convert_time_str(runtime_str)
            except OutputParsingError:
                qjob.runtime = None

            qjob.name = data.get("Job_Name")
            qjob.info = info

            # I append to the list of jobs to return
            jobs_list.append(qjob)

        return jobs_list

    def _convert_time_str(self, time_str):
        """
        Convert a string in the format used by SLURM DD:HH:MM:SS to a number of seconds.
        It may contain only H:M:S, only M:S or only S.
        """

        if not time_str:
            return None

        time_split = time_str.split(":")

        # array containing seconds, minutes, hours and days
        time = [0] * 4

        try:
            for i, v in enumerate(reversed(time_split)):
                time[i] = int(v)

        except ValueError:
            raise OutputParsingError()

        return time[3] * 86400 + time[2] * 3600 + time[1] * 60 + time[0]

    def _convert_memory_str(self, memory: str) -> int | None:
        if not memory:
            return None

        match = re.match(r"([0-9]+)([a-zA-Z]*)", memory)
        if not match:
            raise OutputParsingError("No numbers and units parsed")
        memory, units = match.groups()

        power_labels = {"kb": 0, "mb": 1, "gb": 2, "tb": 3}

        if not units:
            units = "mb"
        elif units not in power_labels:
            raise OutputParsingError(f"Unknown units {units}")
        try:
            v = int(memory)
        except ValueError:
            raise OutputParsingError

        return v * (1024 ** power_labels[units])

    # helper attribute to match the values defined in QResources and
    # the dictionary that should be passed to the template
    _qresources_mapping = {
        "queue_name": "queue",
        "job_name": "job_name",
        "time_limit": "walltime",
        "account": "account",
        "priority": "priority",
        "output_filepath": "qout_path",
        "error_filepath": "qerr_path",
    }

    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a QResources instance to a dict that will be used to fill in the
        header of the submission script.
        #TODO incomplete
        """

        header_dict = {}
        for qr_field, slurm_field in self._qresources_mapping.items():
            val = getattr(resources, qr_field)
            if val is not None:
                header_dict[slurm_field] = val

        return header_dict

    @property
    def supported_qresources_keys(self) -> list:
        """
        List of attributes of QResources that are correctly handled by the
        _convert_qresources method. It is used to validate that the user
        does not pass an unsupported value, expecting to have an effect.
        """
        return list(self._qresources_mapping.keys())
