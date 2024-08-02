from __future__ import annotations

import re
import xml.dom.minidom
import xml.parsers.expat
from datetime import timedelta

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    ProcessPlacement,
    QJob,
    QJobInfo,
    QResources,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import OutputParsingError, UnsupportedResourcesError
from qtoolkit.io.base import BaseSchedulerIO

# 'http://www.loni.ucla.edu/twiki/bin/view/Infrastructure/GridComputing?skin=plain':
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
#
# 'http://confluence.rcs.griffith.edu.au:8080/display/v20zCluster/
# Sun+Grid+Engine+SGE+state+letter+symbol+codes+meanings':
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
    # Queue states
    UNKNOWN = "u"
    ALARM = "a"
    SUSPEND_THRESHOLD = "A"
    SUSPENDED_BY_USER_ADMIN = "s"
    DISABLED_BY_USER_ADMIN = "d"
    SUSPENDED_BY_CALENDAR = "C"
    DISABLED_BY_CALENDAR = "D"
    SUSPENDED_BY_SUBORDINATION = "S"
    ERROR = "E"

    # Job states
    QUEUED_WAITING = "qw"
    WAITING = "w"
    JOB_SUSPENDED = "s"
    TRANSFERRING = "t"
    RUNNING = "r"
    HOLD = "h"
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
    SGEState.SUSPENDED_BY_USER_ADMIN: QState.SUSPENDED,
    SGEState.SUSPENDED_BY_SUBORDINATION: QState.SUSPENDED,
    SGEState.ALARM: QState.SUSPENDED,
    SGEState.ERROR: QState.FAILED,
    SGEState.DELETION: QState.DONE,
    SGEState.DELETION_RUNNING: QState.DONE,
    SGEState.DELETION_TRANSFERRING: QState.DONE,
    SGEState.DELETION_RUNNING_RESUBMIT: QState.DONE,
    SGEState.DELETION_TRANSFERRING_RESUBMIT: QState.DONE,
    SGEState.DELETION_SUSPENDED_JOB: QState.DONE,
    SGEState.DELETION_SUSPENDED_QUEUE: QState.DONE,
    SGEState.DELETION_SUSPENDED_ALARM: QState.DONE,
    SGEState.DELETION_SUSPENDED_RESUBMIT_JOB: QState.DONE,
    SGEState.DELETION_SUSPENDED_RESUBMIT_QUEUE: QState.DONE,
    SGEState.DELETION_SUSPENDED_RESUBMIT_ALARM: QState.DONE,
}


class SGEIO(BaseSchedulerIO):
    header_template: str = """
#$ -cwd
#$ -q $${queue}
#$ -N $${job_name}
#$ -P $${account}
#$ -l $${select}
#$ -l h_rt=$${walltime}
#$ -l s_rt=$${soft_walltime}
#$ -pe $${model}
#$ -binding $${place}
#$ -W group_list=$${group_list}
#$ -M $${mail_user}
#$ -m $${mail_type}
#$ -o $${qout_path}
#$ -e $${qerr_path}
#$ -p $${priority}
#$ -r $${rerunnable}
#$ -t $${array}
$${qverbatim}"""

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"

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
        """Parse the output of the qdel command."""
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
        cmd = f"qstat -j {job_id}"
        return cmd

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        if exit_code != 0:
            raise OutputParsingError(f"Error in job output parsing: {stderr}")
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        try:
            xmldata = xml.dom.minidom.parseString(stdout)
        except xml.parsers.expat.ExpatError:
            raise OutputParsingError("XML parsing of stdout failed")

        job_list = xmldata.getElementsByTagName("job_list")
        if not job_list:
            return None

        job_element = job_list[0]

        qjob = QJob()
        qjob.job_id = self._get_element_text(job_element, "JB_job_number")
        job_state_string = self._get_element_text(job_element, "state")

        try:
            sge_job_state = SGEState(job_state_string)
        except ValueError:
            raise OutputParsingError(
                f"Unknown job state {job_state_string} for job id {qjob.job_id}"
            )

        qjob.sub_state = sge_job_state
        qjob.state = sge_job_state.qstate
        qjob.username = self._get_element_text(job_element, "JB_owner")
        qjob.name = self._get_element_text(job_element, "JB_name")

        info = QJobInfo()
        info.nodes = self._safe_int(self._get_element_text(job_element, "num_nodes"))
        info.cpus = self._safe_int(self._get_element_text(job_element, "num_proc"))
        info.memory_per_cpu = self._convert_memory_str(
            self._get_element_text(job_element, "hard resource_list.mem_free")
        )
        info.partition = self._get_element_text(job_element, "queue_name")
        info.time_limit = self._convert_str_to_time(
            self._get_element_text(job_element, "hard resource_list.h_rt")
        )

        qjob.info = info

        return qjob

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

    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        if job_ids:
            raise UnsupportedResourcesError("Cannot query by job id in SGE")

        command = "qstat -ext -urg -xml "

        if user:
            command += f"-u {user!s}"
        else:
            command += "-u '*'"

        return command

    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        if exit_code != 0:
            raise OutputParsingError(f"Error in jobs list output parsing: {stderr}")
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        try:
            xmldata = xml.dom.minidom.parseString(stdout)
        except xml.parsers.expat.ExpatError:
            raise OutputParsingError("XML parsing of stdout failed")

        job_elements = xmldata.getElementsByTagName("job_list")
        jobs_list = []

        for job_element in job_elements:
            qjob = QJob()
            qjob.job_id = self._get_element_text(job_element, "JB_job_number")
            job_state_string = self._get_element_text(job_element, "state")

            try:
                sge_job_state = SGEState(job_state_string)
            except ValueError:
                raise OutputParsingError(
                    f"Unknown job state {job_state_string} for job id {qjob.job_id}"
                )

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
            info.partition = self._get_element_text(job_element, "queue_name")
            info.time_limit = self._convert_str_to_time(
                self._get_element_text(job_element, "hard resource_list.h_rt")
            )

            qjob.info = info

            jobs_list.append(qjob)

        return jobs_list

    @staticmethod
    def _convert_str_to_time(time_str: str | None):
        if not time_str:
            return None

        time_split = time_str.split(":")
        time = [0] * 3

        try:
            for i, v in enumerate(reversed(time_split)):
                time[i] = int(v)
        except ValueError:
            raise OutputParsingError()

        return time[2] * 3600 + time[1] * 60 + time[0]

    @staticmethod
    def _convert_memory_str(memory: str | None) -> int | None:
        if not memory:
            return None

        match = re.match(r"([0-9]+)([a-zA-Z]*)", memory)
        if not match:
            raise OutputParsingError("No numbers and units parsed")
        memory, units = match.groups()

        power_labels = {"k": 0, "m": 1, "g": 2, "t": 3}

        if not units:
            units = "m"
        elif units not in power_labels:
            raise OutputParsingError(f"Unknown units {units}")
        try:
            v = int(memory)
        except ValueError:
            raise OutputParsingError

        return v * (1024 ** power_labels[units])

    _qresources_mapping = {
        "queue_name": "queue",
        "job_name": "job_name",
        "account": "account",
        "priority": "priority",
        "output_filepath": "qout_path",
        "error_filepath": "qerr_path",
        "project": "group_list",
    }

    @staticmethod
    def _convert_time_to_str(time: int | float | timedelta) -> str:
        if not isinstance(time, timedelta):
            time = timedelta(seconds=time)

        hours, remainder = divmod(int(time.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        time_str = f"{hours}:{minutes}:{seconds}"
        return time_str

    def _convert_qresources(self, resources: QResources) -> dict:
        header_dict = {}
        for qr_field, sge_field in self._qresources_mapping.items():
            val = getattr(resources, qr_field)
            if val is not None:
                header_dict[sge_field] = val

        if resources.njobs and resources.njobs > 1:
            header_dict["array"] = f"1-{resources.njobs}"

        if resources.time_limit:
            header_dict["walltime"] = self._convert_time_to_str(resources.time_limit)
            header_dict["soft_walltime"] = self._convert_time_to_str(
                resources.time_limit * 0.9
            )

        if resources.rerunnable is not None:
            header_dict["rerunnable"] = "y" if resources.rerunnable else "n"

        nodes, processes, processes_per_node = resources.get_processes_distribution()
        select = None
        if resources.process_placement == ProcessPlacement.NO_CONSTRAINTS:
            select = f"select={processes}"
            if resources.threads_per_process:
                select += f":ncpus={resources.threads_per_process}"
                select += f":ompthreads={resources.threads_per_process}"
            if resources.memory_per_thread:
                threads_per_process = resources.threads_per_process or 1
                select += f":mem={threads_per_process * resources.memory_per_thread}mb"
        elif resources.process_placement in (
            ProcessPlacement.EVENLY_DISTRIBUTED,
            ProcessPlacement.SAME_NODE,
            ProcessPlacement.SCATTERED,
        ):
            select = f"select={nodes}"
            if resources.threads_per_process and resources.threads_per_process > 1:
                cpus = resources.threads_per_process * processes_per_node
                ompthreads = resources.threads_per_process
            else:
                cpus = processes_per_node
                ompthreads = None
            select += f":ncpus={cpus}"
            select += f":mpiprocs={processes_per_node}"
            if ompthreads:
                select += f":ompthreads={ompthreads}"
            if resources.memory_per_thread:
                mem = cpus * resources.memory_per_thread
                select += f":mem={mem}mb"

            if resources.process_placement in (
                ProcessPlacement.EVENLY_DISTRIBUTED,
                ProcessPlacement.SCATTERED,
            ):
                header_dict["place"] = "scatter"
            elif resources.process_placement == ProcessPlacement.SAME_NODE:
                header_dict["place"] = "pack"
        else:
            msg = f"process placement {resources.process_placement} is not supported for SGE"
            raise UnsupportedResourcesError(msg)

        header_dict["select"] = select

        if resources.email_address:
            header_dict["mail_user"] = resources.email_address
            header_dict["mail_type"] = "abe"

        if resources.scheduler_kwargs:
            header_dict.update(resources.scheduler_kwargs)

        return header_dict

    @property
    def supported_qresources_keys(self) -> list:
        supported = list(self._qresources_mapping.keys())
        supported += [
            "njobs",
            "time_limit",
            "processes",
            "processes_per_node",
            "process_placement",
            "nodes",
            "threads_per_process",
            "email_address",
            "scheduler_kwargs",
        ]
        return supported
