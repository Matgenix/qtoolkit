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
from qtoolkit.core.exceptions import (
    CommandFailedError,
    OutputParsingError,
    UnsupportedResourcesError,
)
from qtoolkit.io.base import BaseSchedulerIO

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


class SGEIO(BaseSchedulerIO):
    header_template: str = """
#$ -cwd $${cwd}
#$ -q $${queue}
#$ -N $${job_name}
#$ -P $${device}
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

    def __init__(self, get_job_executable: str = "qstat"):
        self.get_job_executable = get_job_executable

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
        match = re.search(r'Your job (\d+) \(".*?"\) has been submitted', stdout)
        if not match:
            raise OutputParsingError("Failed to parse job ID from stdout")
        job_id = match.group(1)
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
        match = re.search(r"qdel: job (\d+) deleted", stderr)
        if not match:
            raise OutputParsingError("Failed to parse job ID from stdout")
        job_id = match.group(1)
        status = CancelStatus("SUCCESSFUL")
        return CancelResult(
            job_id=job_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def _get_job_cmd(self, job_id: str):
        cmd = f"qstat -j {job_id}"
        return cmd

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:  # aiida style
        if exit_code != 0:
            msg = f"command {self.get_job_executable or 'qacct'} failed: {stderr}"
            raise CommandFailedError(msg)
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        # Check for specific error messages in stderr or stdout
        error_patterns = [
            re.compile(
                r"Primary job\s+terminated normally, but\s+(\d+)\s+process returned a non-zero exit code",
                re.IGNORECASE,
            ),
            re.compile(
                r"mpiexec detected that one or more processes exited with non-zero status",
                re.IGNORECASE,
            ),
            re.compile(r"An error occurred in MPI_Allreduce", re.IGNORECASE),
            re.compile(
                r"Error: mca_pml_ucx_send_nbr failed: -25, Connection reset by remote peer",
                re.IGNORECASE,
            ),
            re.compile(r"mpi_errors_are_fatal", re.IGNORECASE),
        ]

        for pattern in error_patterns:
            if pattern.search(stderr) or pattern.search(stdout):
                msg = f"command {self.get_job_executable or 'qacct'} failed: {stderr}"
                raise CommandFailedError(msg)

        if not stdout.strip():
            return None

        # Check if stdout is in XML format
        try:
            xmldata = xml.dom.minidom.parseString(stdout)
            job_info = xmldata.getElementsByTagName("job_list")[0]
            job_id = job_info.getElementsByTagName("JB_job_number")[
                0
            ].firstChild.nodeValue
            job_name = job_info.getElementsByTagName("JB_name")[0].firstChild.nodeValue
            owner = job_info.getElementsByTagName("JB_owner")[0].firstChild.nodeValue
            state = job_info.getElementsByTagName("state")[0].firstChild.nodeValue
            queue_name = job_info.getElementsByTagName("queue_name")[
                0
            ].firstChild.nodeValue
            slots = job_info.getElementsByTagName("slots")[0].firstChild.nodeValue
            tasks = job_info.getElementsByTagName("tasks")[0].firstChild.nodeValue

            sge_state = SGEState(state)
            job_state = sge_state.qstate

            try:
                cpus = int(slots)
                nodes = int(tasks)
            except ValueError:
                cpus = None
                nodes = None

            return QJob(
                name=job_name,
                job_id=job_id,
                state=job_state,
                sub_state=sge_state,
                account=owner,
                queue_name=queue_name,
                info=QJobInfo(nodes=nodes, cpus=cpus),
            )
        except Exception:
            # Not XML, fallback to plain text
            job_info = {}
            for line in stdout.split("\n"):
                if ":" in line:
                    key, value = line.split(":", 1)
                    job_info[key.strip()] = value.strip()

            try:
                cpus = int(job_info.get("slots", 1))
                nodes = int(job_info.get("tasks", 1))
            except ValueError:
                cpus = None
                nodes = None

            state_str = job_info.get("state")
            sge_state = SGEState(state_str) if state_str else None
            job_state = sge_state.qstate

            return QJob(
                name=job_info.get("job_name"),
                job_id=job_info.get("job_id"),
                state=job_state,
                sub_state=sge_state,
                account=job_info.get("owner"),
                queue_name=job_info.get("queue_name"),
                info=QJobInfo(nodes=nodes, cpus=cpus),
            )

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
            raise ValueError("Cannot query by job ids list in SGE")
        user = user if user else "*"
        return f"qstat -ext -urg -xml -u {user}"

    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        if exit_code != 0:
            msg = f"command {self.get_job_executable or 'qacct'} failed: {stderr}"
            raise CommandFailedError(msg)
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
    def _convert_str_to_time(time_str: str | None) -> int | None:
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
        except ValueError:
            raise OutputParsingError(f"Invalid time format: {time_str}")

    @staticmethod
    def _convert_memory_str(memory: str | None) -> int | None:
        if not memory:
            return None

        match = re.match(r"([0-9]+)([a-zA-Z]*)", memory)
        if not match:
            raise OutputParsingError("No numbers and units parsed")
        memory, units = match.groups()

        power_labels = {"K": 0, "M": 1, "G": 2, "T": 3}

        if not units:
            units = "M"
        elif units not in power_labels:
            raise OutputParsingError(f"Unknown units {units}")
        try:
            v = int(memory)
        except ValueError:
            raise OutputParsingError

        return v * (1024 ** power_labels[units.upper()])

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
            "memory_per_thread",
            "time_limit",
            "processes",
            "processes_per_node",
            "process_placement",
            "nodes",
            "threads_per_process",
            "email_address",
            "scheduler_kwargs",
            "gpus_per_job",
        ]
        return supported
