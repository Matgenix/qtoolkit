from __future__ import annotations

import re
from datetime import timedelta

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
from qtoolkit.core.exceptions import CommandFailedError, OutputParsingError
from qtoolkit.io.base import BaseSchedulerIO

# States in Slurm from squeue's manual. We currently only take the most important ones.
#     JOB STATE CODES
#        Jobs  typically pass through several states in the course of their execution.
#        The typical states are PENDING, RUNNING, SUSPENDED, COMPLETING, and COMPLETED.
#        An explanation of each state follows.
#
#        BF  BOOT_FAIL       Job terminated due to launch failure, typically due to a
#                            hardware failure (e.g. unable  to
#                            boot the node or block and the job can not be requeued).
#
#        CA  CANCELLED       Job  was explicitly cancelled by the user or system
#                            administrator.  The job may or may not
#                            have been initiated.
#
#        CD  COMPLETED       Job has terminated all processes on all nodes with
#                            an exit code of zero.
#
#        CF  CONFIGURING     Job has been allocated resources, but are waiting for
#                            them to become ready for  use  (e.g. booting).
#
#        CG  COMPLETING      Job is in the process of completing.
#                            Some processes on some nodes may still be active.
#
#        DL  DEADLINE        Job terminated on deadline.
#
#        F   FAILED          Job terminated with non-zero exit code or other
#                            failure condition.
#
#        NF  NODE_FAIL       Job terminated due to failure of one or more
#                            allocated nodes.
#
#        OOM OUT_OF_MEMORY   Job experienced out of memory error.
#
#        PD  PENDING         Job is awaiting resource allocation.
#
#        PR  PREEMPTED       Job terminated due to preemption.
#
#        R   RUNNING         Job currently has an allocation.
#
#        RD  RESV_DEL_HOLD   Job is being held after requested reservation was deleted.
#
#        RF  REQUEUE_FED     Job is being requeued by a federation.
#
#        RH  REQUEUE_HOLD    Held job is being requeued.
#
#        RQ  REQUEUED        Completing job is being requeued.
#
#        RS  RESIZING        Job is about to change size.
#
#        RV  REVOKED         Sibling was removed from cluster due to other cluster
#                            starting the job.
#
#        SI  SIGNALING       Job is being signaled.
#
#        SE  SPECIAL_EXIT    The job was requeued in a special state. This state
#                            can be set by users, typically in Epi‐
#                            logSlurmctld, if the job has terminated with a particular
#                            exit value.
#
#        SO  STAGE_OUT       Job is staging out files.
#
#        ST  STOPPED         Job has an allocation, but execution has been stopped
#                            with SIGSTOP signal. CPUS have been retained by this job.
#
#        S   SUSPENDED       Job  has  an  allocation, but execution has been
#                            suspended and CPUs have been released for other jobs.
#
#        TO  TIMEOUT         Job terminated upon reaching its time limit.


class SlurmState(QSubState):
    CANCELLED = "CANCELLED", "CA"
    COMPLETING = "COMPLETING", "CG"
    COMPLETED = "COMPLETED", "CD"
    CONFIGURING = "CONFIGURING", "CF"
    DEADLINE = "DEADLINE", "DL"
    FAILED = "FAILED", "F"
    NODE_FAIL = "NODE_FAIL", "NF"
    OUT_OF_MEMORY = "OUT_OF_MEMORY", "OOM"
    PENDING = "PENDING", "PD"
    RUNNING = "RUNNING", "R"
    SUSPENDED = "SUSPENDED", "S"
    TIMEOUT = "TIMEOUT", "TO"

    @property
    def qstate(self) -> QState:
        # the type:ignore is required due to the dynamic class creation of QTKEnum
        return _STATUS_MAPPING[self]  # type: ignore


_STATUS_MAPPING = {
    SlurmState.CANCELLED: QState.SUSPENDED,  # Should this be failed ?
    SlurmState.COMPLETING: QState.RUNNING,
    SlurmState.COMPLETED: QState.DONE,
    SlurmState.CONFIGURING: QState.QUEUED,
    SlurmState.DEADLINE: QState.FAILED,
    SlurmState.FAILED: QState.FAILED,
    SlurmState.NODE_FAIL: QState.FAILED,
    SlurmState.OUT_OF_MEMORY: QState.FAILED,
    SlurmState.PENDING: QState.QUEUED,
    SlurmState.RUNNING: QState.RUNNING,
    SlurmState.SUSPENDED: QState.SUSPENDED,
    SlurmState.TIMEOUT: QState.FAILED,
}


class SlurmIO(BaseSchedulerIO):
    header_template: str = """
#SBATCH --partition=$${partition}
#SBATCH --job-name=$${job_name}
#SBATCH --nodes=$${nodes}
#SBATCH --ntasks=$${ntasks}
#SBATCH --ntasks-per-node=$${ntasks_per_node}
#SBATCH --cpus-per-task=$${cpus_per_task}
#SBATCH --mem=$${mem}
#SBATCH --mem-per-cpu=$${mem_per_cpu}
#SBATCH --hint=$${hint}
#SBATCH --time=$${time}
#SBATCH	--exclude=$${exclude_nodes}
#SBATCH --account=$${account}
#SBATCH --mail-user=$${mail_user}
#SBATCH --mail-type=$${mail_type}
#SBATCH --constraint=$${constraint}
#SBATCH --gres=$${gres}
#SBATCH --requeue=$${requeue}
#SBATCH --nodelist=$${nodelist}
#SBATCH --propagate=$${propagate}
#SBATCH --licenses=$${licenses}
#SBATCH --output=$${qout_path}
#SBATCH --error=$${qerr_path}
#SBATCH --qos=$${qos}
#SBATCH --priority=$${priority}
#SBATCH --array=$${array}
#SBATCH --exclusive=$${exclusive}
$${qverbatim}"""

    SUBMIT_CMD: str | None = "sbatch"
    CANCEL_CMD: str | None = (
        "scancel -v"  # The -v is needed as the default is to report nothing
    )

    squeue_fields = [
        ("%i", "job_id"),  # job or job step id
        ("%t", "state_raw"),  # job state in compact form
        ("%r", "annotation"),  # reason for the job being in its current state
        ("%j", "job_name"),  # job name (title)
        ("%u", "username"),  # username
        ("%P", "partition"),  # partition (queue) of the job
        ("%l", "time_limit"),  # time limit in days-hours:minutes:seconds
        ("%D", "number_nodes"),  # number of nodes allocated
        ("%C", "number_cpus"),  # number of allocated cores (if already running)
        ("%M", "time_used"),  # Time used by the job in days-hours:minutes:seconds
        ("%m", "min_memory"),  # Minimum size of memory (in MB) requested by the job
    ]

    def __init__(
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
        _SLURM_SUBMITTED_REGEXP = re.compile(
            r"(.*:\s*)?([Gg]ranted job allocation|"
            r"[Ss]ubmitted batch job)\s+(?P<jobid>\d+)"
        )
        match = _SLURM_SUBMITTED_REGEXP.match(stdout.strip())
        job_id = match.group("jobid") if match else None
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
        # scancel: error: No job identification provided
        # scancel: error: Kill job error on job id 958: Invalid job id specified
        # scancel: error: Kill job error on job id 69:
        #                   Job/step already completing or completed
        # Correct execution:
        # scancel: Terminating job 80
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
        _SLURM_CANCELLED_REGEXP = re.compile(
            r"(.*:\s*)?(Terminating job)\s+(?P<jobid>\d+)"
        )
        match = _SLURM_CANCELLED_REGEXP.match(stderr.strip())
        job_id = match.group("jobid") if match else None
        status = (
            CancelStatus("SUCCESSFUL") if job_id else CancelStatus("JOB_ID_UNKNOWN")
        )
        # TODO: when cancelling a job already completed or cancelled, exit_code is 0
        #  should we set the CancelStatus to FAILED ? Same if the job does not exist.
        return CancelResult(
            job_id=job_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def _get_job_cmd(self, job_id: str):
        # TODO: there are two options to get info on a job in slurm:
        #  - scontrol show job JOB_ID
        #  - sacct -j JOB_ID
        #  sacct is only available when a database is running (slurmdbd).
        #  I guess most of the time, the clusters
        #  will have that in place. scontrol is only available for queued
        #  or running jobs (not completed ones),
        #  at least it disappears rapidly. Currently I am only
        #  using/implementing scontrol.

        if self.get_job_executable == "scontrol":
            # -o is to get the output as a one-liner
            cmd = f"SLURM_TIME_FORMAT='standard' scontrol show job -o {job_id}"
        elif self.get_job_executable == "sacct":  # pragma: no cover
            raise NotImplementedError("sacct for get_job not yet implemented.")
        else:  # pragma: no cover
            raise RuntimeError(
                f'"{self.get_job_executable}" is not a valid get_job_executable.'
            )

        return cmd

    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()
        if exit_code != 0:
            msg = f"command {self.get_job_executable} failed: {stderr}"
            raise CommandFailedError(msg)

        if self.get_job_executable == "scontrol":
            parsed_output = self._parse_scontrol_cmd_output(stdout=stdout)
        elif self.get_job_executable == "sacct":  # pragma: no cover
            raise NotImplementedError("sacct for get_job not yet implemented.")
        else:  # pragma: no cover
            raise RuntimeError(
                f'"{self.get_job_executable}" is not a valid get_job_executable.'
            )

        if not parsed_output:
            return None

        slurm_state = SlurmState(parsed_output["JobState"])
        job_state = slurm_state.qstate

        try:
            memory_per_cpu = self._convert_memory_str(parsed_output["MinMemoryCPU"])
        except (OutputParsingError, KeyError):
            memory_per_cpu = None

        try:
            nodes = int(parsed_output["NumNodes"])
        except (ValueError, KeyError):
            nodes = None

        try:
            cpus = int(parsed_output["NumCPUs"])
        except (ValueError, KeyError):
            cpus = None

        try:
            cpus_task = int(parsed_output["CPUs/Task"])
        except (ValueError, KeyError):
            cpus_task = None

        try:
            time_limit = self._convert_str_to_time(parsed_output["TimeLimit"])
        except (OutputParsingError, KeyError):
            time_limit = None

        info = QJobInfo(
            memory=memory_per_cpu,
            nodes=nodes,
            cpus=cpus,
            threads_per_process=cpus_task,
            time_limit=time_limit,
        )
        return QJob(
            name=parsed_output["JobName"],
            job_id=parsed_output["JobId"],
            state=job_state,
            sub_state=slurm_state,
            info=info,
            account=parsed_output["UserId"],
            queue_name=parsed_output["Partition"],
        )

    def _parse_scontrol_cmd_output(self, stdout):
        return {
            data.split("=", maxsplit=1)[0]: data.split("=", maxsplit=1)[1]
            for data in stdout.split()
        }

    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        if user and job_ids:
            raise ValueError("Cannot query by user and job(s) in SLURM")

        # also leave one empty space to clarify how the split happens in case
        # some columns are empty
        fields = f"{self.split_separator} ".join(f[0] for f in self.squeue_fields)

        command = [
            "SLURM_TIME_FORMAT='standard'",
            "squeue",
            "--noheader",
            f"-o '{fields}'",
        ]

        if user:
            command.append(f"-u {user}")

        if job_ids:
            # Trick copied from aiida-core: When asking for a single job,
            # append the same job once more.
            if len(job_ids) == 1:
                job_ids += [job_ids[0]]

            command.append(f"--jobs={','.join(job_ids)}")

        return " ".join(command)

    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()

        if exit_code != 0:
            msg = f"command {self.get_job_executable} failed: {stderr}"
            raise CommandFailedError(msg)

        num_fields = len(self.squeue_fields)

        # assume the split chosen does not appear in the output. (e.g. in the
        # name of the job)
        jobdata_raw = [
            chunk.split(self.split_separator)
            for chunk in stdout.splitlines()
            if self.split_separator in chunk
        ]

        # Create dictionary and parse specific fields
        jobs_list = []
        for data in jobdata_raw:
            if len(data) != num_fields:
                msg = f"Wrong number of fields. Found {len(jobdata_raw)}, expected {num_fields}"
                # TODO should this raise or just continue? and should there be
                # a logging of the errors?
                raise OutputParsingError(msg)

            thisjob_dict = {k[1]: v.strip() for k, v in zip(self.squeue_fields, data)}

            qjob = QJob()
            qjob.job_id = thisjob_dict["job_id"]

            job_state_string = thisjob_dict["state_raw"]

            try:
                slurm_job_state = SlurmState(job_state_string)
            except ValueError:
                msg = f"Unknown job state {job_state_string} for job id {qjob.job_id}"
                raise OutputParsingError(msg)
            qjob.sub_state = slurm_job_state
            qjob.state = slurm_job_state.qstate

            qjob.username = thisjob_dict["username"]

            info = QJobInfo()

            try:
                info.nodes = int(thisjob_dict["number_nodes"])
            except ValueError:
                info.nodes = None

            try:
                info.cpus = int(thisjob_dict["number_cpus"])
            except ValueError:
                info.cpus = None

            try:
                info.memory_per_cpu = self._convert_memory_str(
                    thisjob_dict["min_memory"]
                )
            except OutputParsingError:
                info.memory_per_cpu = None

            info.partition = thisjob_dict["partition"]

            # TODO here _convert_time_str can raise. If parsing errors are accepted
            # handle differently
            info.time_limit = self._convert_str_to_time(thisjob_dict["time_limit"])

            try:
                qjob.runtime = self._convert_str_to_time(thisjob_dict["time_used"])
            except OutputParsingError:
                # if the job did not start usually it is set to 00:00, but if it is
                # empty it should be fine.
                qjob.runtime = None

            qjob.name = thisjob_dict["job_name"]
            qjob.info = info

            # I append to the list of jobs to return
            jobs_list.append(qjob)

        return jobs_list

    @staticmethod
    def _convert_str_to_time(time_str: str | None) -> int | None:
        """
        Convert a string in the format used by SLURM DD-HH:MM:SS to a number of seconds.
        """

        if not time_str:
            return None

        if time_str in ["UNLIMITED", "NOT_SET"]:
            return None

        time_split = time_str.split(":")

        days = hours = minutes = seconds = 0

        try:
            if "-" in time_split[0]:
                split_day = time_split[0].split("-")
                days = int(split_day[0])
                time_split = [split_day[1]] + time_split[1:]

            if len(time_split) == 3:
                hours, minutes, seconds = (int(v) for v in time_split)
            elif len(time_split) == 2:
                minutes, seconds = (int(v) for v in time_split)
            elif len(time_split) == 1:
                minutes = int(time_split[0])
            else:
                raise OutputParsingError()

        except ValueError:
            raise OutputParsingError()

        return days * 86400 + hours * 3600 + minutes * 60 + seconds

    @staticmethod
    def _convert_memory_str(memory: str | None) -> int | None:
        if not memory:
            return None

        # TODO: @GP not sure I get what is this line here
        #  Shouldn't it be all(u not in memory for u in ("K", "M", "G", "T"))?
        #  Or not any(u in memory for u in ("K", "M", "G", "T"))?
        if all(u in memory for u in ("K", "M", "G", "T")):
            # assume Mb
            units = "M"
        else:
            units = memory[-1]
            memory = memory[:-1]
        try:
            v = int(memory)
        except ValueError:
            raise OutputParsingError
        power_labels = {"K": 0, "M": 1, "G": 2, "T": 3}

        return v * (1024 ** power_labels[units])

    @staticmethod
    def _convert_time_to_str(time: int | timedelta) -> str:
        if isinstance(time, int):
            time = timedelta(seconds=time)

        days = time.days
        hours, remainder = divmod(time.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        time_str = f"{days}-{hours}:{minutes}:{seconds}"
        return time_str

    # helper attribute to match the values defined in QResources and
    # the dictionary that should be passed to the template
    _qresources_mapping = {
        "queue_name": "partition",
        "job_name": "job_name",
        "memory_per_thread": "mem-per-cpu",
        "account": "account",
        "qos": "qos",
        "priority": "priority",
        "output_filepath": "qout_path",
        "error_filepath": "qerr_path",
    }

    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a Qresources instance to a dict that will be used to fill in the
        header of the submission script.
        """

        header_dict = {}
        for qr_field, slurm_field in self._qresources_mapping.items():
            val = getattr(resources, qr_field)
            if val is not None:
                header_dict[slurm_field] = val

        if resources.njobs and resources.njobs > 1:
            header_dict["array"] = f"1-{resources.njobs}"

        if resources.time_limit:
            header_dict["time"] = self._convert_time_to_str(resources.time_limit)

        nodes, processes, processes_per_node = resources.get_processes_distribution()
        if processes:
            header_dict["ntasks"] = processes
        if processes_per_node:
            header_dict["ntasks_per_node"] = processes_per_node
        if nodes:
            header_dict["nodes"] = nodes

        if resources.threads_per_process:
            header_dict["cpus_per_task"] = resources.threads_per_process

        if resources.gpus_per_job:
            header_dict["gres"] = f"gpu:{resources.gpus_per_job}"

        if resources.email_address:
            header_dict["mail_user"] = resources.email_address
            header_dict["mail_type"] = "ALL"

        if resources.scheduler_kwargs:
            header_dict.update(resources.scheduler_kwargs)

        return header_dict

    @property
    def supported_qresources_keys(self) -> list:
        """
        List of attributes of QResources that are correctly handled by the
        _convert_qresources method. It is used to validate that the user
        does not pass an unsupported value, expecting to have an effect.
        """
        supported = list(self._qresources_mapping.keys())
        supported += [
            "njobs",
            "time_limit",
            "processes",
            "processes_per_node",
            "process_placement",
            "nodes",
            "threads_per_process",
            "gpus_per_job",
            "email_address",
            "scheduler_kwargs",
        ]
        return supported
