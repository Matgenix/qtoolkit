from __future__ import annotations

import re

from qtoolkit.core.data_objects import QJob, QJobInfo, QState, QSubState
from qtoolkit.core.exceptions import OutputParsingError
from qtoolkit.io.pbs_base import PBSIOBase

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


class PBSIO(PBSIOBase):
    header_template: str = """
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
#PBS -r $${rerunnable}
#PBS -J $${array}
$${qverbatim}"""

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"

    def __init__(self):
        super().__init__()
        self._qresources_mapping = {
            "queue_name": "queue",
            "job_name": "job_name",
            "account": "account",
            "priority": "priority",
            "output_filepath": "qout_path",
            "error_filepath": "qerr_path",
            "project": "group_list",
        }

    def extract_job_id(self, stdout):
        return stdout.strip()

    def extract_job_id_from_cancel(self, stderr):
        # PBS doesn't return the job ID if successfully canceled, so return None
        return None

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        out = self.parse_jobs_list_output(exit_code, stdout, stderr)
        if out:
            return out[0]
        return None

    def _get_qstat_base_command(self) -> list[str]:
        return ["qstat", "-f"]

    def _get_job_ids_flag(self, job_ids_str: str) -> str:
        return job_ids_str

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
            info.time_limit = self._convert_str_to_time(
                data.get("Resource_List.walltime")
            )

            try:
                runtime_str = data.get("resources_used.walltime")
                if runtime_str:
                    qjob.runtime = self._convert_str_to_time(runtime_str)
            except OutputParsingError:
                qjob.runtime = None

            qjob.name = data.get("Job_Name")
            qjob.info = info

            # I append to the list of jobs to return
            jobs_list.append(qjob)

        return jobs_list

    @staticmethod
    def _convert_str_to_time(time_str: str | None):
        """
        Convert a string in the format used by PBS DD:HH:MM:SS to a number of seconds.
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

    @staticmethod
    def get_power_labels() -> dict:
        return {"kb": 0, "mb": 1, "gb": 2, "tb": 3}

    @staticmethod
    def get_default_unit() -> str:
        return "mb"

    def get_system_name(self) -> str:
        return "PBS"
