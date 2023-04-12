from __future__ import annotations

import re
from dataclasses import dataclass

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
from qtoolkit.queue.base import BaseQueue

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


@dataclass
class SlurmQueue(BaseQueue):
    header_template: str = """
#SBATCH --partition=$${queue_name}
#SBATCH --job-name=$${job_name}
#SBATCH --nodes=$${number_of_nodes}
#SBATCH --ntasks=$${number_of_tasks}
#SBATCH --ntasks-per-node=$${ntasks_per_node}
#SBATCH --cpus-per-task=$${cpus_per_task}
#####SBATCH --mem=$${mem}
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
#SBATCH --output=$${_qout_path}
#SBATCH --error=$${_qerr_path}
#SBATCH --qos=$${qos}
$${qverbatim}"""

    SUBMIT_CMD: str = "sbatch"
    CANCEL_CMD = "scancel -v"  # The -v is needed as the default is to report nothing

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

    def _parse_submit_cmd_output(self, exit_code, stdout, stderr):
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

    def _parse_cancel_cmd_output(self, exit_code, stdout, stderr):
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
        return CancelResult(
            job_id=job_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    def get_job(self, job: QJob | int | str, inplace=False, get_job_cmd="scontrol"):
        # TODO: there are two options to get info on a job in slurm:
        #  - scontrol show job JOB_ID
        #  - sacct -j JOB_ID
        #  sacct is only available when a database is running (slurmdbd).
        #  I guess most of the time, the clusters
        #  will have that in place. scontrol is only available for queued
        #  or running jobs (not completed ones),
        #  at least it disappears rapidly. Currently I am only
        #  using/implementing scontrol.
        if inplace and not isinstance(job, QJob):
            raise RuntimeError(
                "Cannot update job information inplace when the QJob is not provided."
            )
        job_id = job.job_id if isinstance(job, QJob) else job
        if get_job_cmd == "scontrol":
            # -o is to get the output as a one-liner
            cmd = f"scontrol show job -o {job_id}"
        elif get_job_cmd == "sacct":
            raise NotImplementedError("sacct for get_job not yet implemented.")
        else:
            raise RuntimeError(f'"{get_job_cmd}" is not a valid get_job_cmd.')
        stdout, stderr, returncode = self.execute_cmd(cmd)
        scontrol_out = self._parse_scontrol_cmd_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )
        slurm_state = SlurmState(scontrol_out["JobState"])
        job_state = self._STATUS_MAPPING[slurm_state]
        print(type(job_state))
        print("\n".join([f"{k}: {v}" for k, v in scontrol_out.items()]))
        resources = QResources(
            queue_name=scontrol_out["Partition"],
            memory=scontrol_out["MinMemoryCPU"],
            # TODO: clarify here what are tasks, cpus, cores, etc ...
            #  and whether this makes sense
            nodes=scontrol_out["NumNodes"],
            cpus_per_node=scontrol_out["NumTasks"],
            cores_per_cpu=scontrol_out["CPUs/Task"],
        )
        return QJob(
            name=scontrol_out["JobName"],
            job_id=scontrol_out["JobId"],
            state=job_state,  # type: ignore # mypy thinks job_state is a str
            sub_state=slurm_state,
            resources=resources,
        )

    def _parse_scontrol_cmd_output(self, exit_code, stdout, stderr):
        if isinstance(stdout, bytes):
            stdout = stdout.decode()
        if isinstance(stderr, bytes):
            stderr = stderr.decode()
        if exit_code != 0:
            return {}
        return {
            data.split("=", maxsplit=1)[0]: data.split("=", maxsplit=1)[1]
            for data in stdout.split()
        }
