from __future__ import annotations

import abc
import re
from abc import ABC
from datetime import timedelta

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    ProcessPlacement,
    QResources,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import OutputParsingError, UnsupportedResourcesError
from qtoolkit.io.base import BaseSchedulerIO


class PBSIOBase(BaseSchedulerIO, ABC):
    """Abstract class for PBS and SGE schedulers."""

    header_template: str

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"

    def __init__(self):
        self._qresources_mapping = None

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
        job_id = self.extract_job_id(stdout)
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

    @abc.abstractmethod
    def extract_job_id(self, stdout):
        pass

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

        job_id = self.extract_job_id_from_cancel(stderr)
        status = CancelStatus("SUCCESSFUL")
        return CancelResult(
            job_id=job_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            status=status,
        )

    @abc.abstractmethod
    def extract_job_id_from_cancel(self, stderr):
        pass

    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        if user and job_ids:
            self._check_user_and_job_ids_conflict()

        command = self._get_qstat_base_command()

        if user:
            command.append(f"-u {user}")

        if job_ids:
            job_ids_str = ",".join(job_ids)
            command.append(self._get_job_ids_flag(job_ids_str))

        return " ".join(command)

    def _check_user_and_job_ids_conflict(self):
        # Use get_system_name() for more informative error messages
        raise ValueError(f"Cannot query by user and job(s) in {self.get_system_name()}")

    @abc.abstractmethod
    def _get_qstat_base_command(self) -> list[str]:
        pass

    @abc.abstractmethod
    def _get_job_ids_flag(self, job_ids_str: str) -> str:
        pass

    def _get_job_cmd(self, job_id: str):
        cmd = f"qstat -j {job_id}"
        return cmd

    def _convert_memory_str(self, memory: str | None) -> int | None:
        if not memory:
            return None

        match = re.match(r"([0-9]+)([a-zA-Z]*)", memory)
        if not match:
            raise OutputParsingError("No numbers and units parsed")
        memory, units = match.groups()

        # Now we call the methods specific to the child class (PBSIO or SGEIO)
        power_labels = self.get_power_labels()

        if not units:
            units = self.get_default_unit()
        elif units.lower() not in power_labels:
            raise OutputParsingError(f"Unknown units {units}")

        try:
            v = int(memory)
        except ValueError:
            raise OutputParsingError

        return v * (1024 ** power_labels[units.lower()])

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
        for qr_field, system_field in self._qresources_mapping.items():
            val = getattr(resources, qr_field)
            if val is not None:
                header_dict[system_field] = val

        if resources.njobs and resources.njobs > 1:
            header_dict["array"] = f"1-{resources.njobs}"

        if resources.time_limit:
            header_dict["walltime"] = self._convert_time_to_str(resources.time_limit)
            self._add_soft_walltime(header_dict, resources)

        if resources.rerunnable is not None:
            header_dict["rerunnable"] = "y" if resources.rerunnable else "n"

        # Build select clause logic directly within _convert_qresources
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
            raise UnsupportedResourcesError(
                f"process placement {resources.process_placement} is not supported for {self.get_system_name()}"
            )

        header_dict["select"] = select

        if resources.email_address:
            header_dict["mail_user"] = resources.email_address
            header_dict["mail_type"] = "abe"

        if resources.scheduler_kwargs:
            header_dict.update(resources.scheduler_kwargs)

        return header_dict

    @abc.abstractmethod
    def _add_soft_walltime(self, header_dict: dict, resources: QResources):
        """Add soft_walltime if required by child classes (SGE)."""

    @abc.abstractmethod
    def get_system_name(self) -> str:
        """This should return the system name (PBS or SGE) for error messages."""

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
