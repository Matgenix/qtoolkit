from __future__ import annotations

import abc
import re
from abc import ABC
from datetime import timedelta
from typing import ClassVar

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
    header_template_file: str | None = None

    SUBMIT_CMD: str | None = "qsub"
    CANCEL_CMD: str | None = "qdel"
    job_id_regex: str | None = r"^\d+\.[\w.-]+(\[\d+(-\d+)?(,\d+)*\])?$"
    _qresources_mapping: ClassVar[dict]
    system_name: str
    default_unit: str
    power_labels: ClassVar[dict]

    def parse_submit_output(
        self, exit_code: int, stdout: str | bytes, stderr: str | bytes
    ) -> SubmissionResult:
        """
        Parse the output of the qsub command.
        """
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
    def extract_job_id(self, stdout: str) -> str | None:
        """
        Extract the job ID from the submission output.
        """
        # pragma: no cover - implementation in subclasses
        raise NotImplementedError

    def parse_cancel_output(
        self, exit_code: int, stdout: str | bytes, stderr: str | bytes
    ) -> CancelResult:
        """
        Parse the output of the qdel command.
        """
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
    def extract_job_id_from_cancel(self, stderr: str) -> str | None:
        """
        Extract the job ID from the cancellation output.
        """
        # pragma: no cover - implementation in subclasses
        raise NotImplementedError

    @abc.abstractmethod
    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        raise NotImplementedError

    def _check_user_and_job_ids_conflict(self) -> None:
        """
        Check if both user and job IDs are provided, which is not supported by some schedulers.
        """
        # Use system_name for more informative error messages
        raise ValueError(f"Cannot query by user and job(s) in {self.system_name}")

    @abc.abstractmethod
    def _get_qstat_base_command(self) -> list[str]:
        """
        Get the base qstat command parts.
        """
        # pragma: no cover - implementation in subclasses
        raise NotImplementedError

    @abc.abstractmethod
    def _get_job_ids_flag(self, job_ids_str: str) -> str:
        """
        Get the flag used to specify job IDs in qstat.
        """
        # pragma: no cover - implementation in subclasses
        raise NotImplementedError

    @abc.abstractmethod
    def _get_job_cmd(self, job_id: str) -> str:
        """
        Get the command to retrieve information for a specific job.
        """
        # pragma: no cover - implementation in subclasses
        raise NotImplementedError

    def _convert_memory_str(self, memory: str | None) -> int | None:
        """
        Convert a PBS/SGE memory string to an integer in bytes.
        """
        if not memory:
            return None

        match = re.match(r"([0-9]+)([a-zA-Z]*)", memory)
        if not match:
            raise OutputParsingError("No numbers and units parsed")
        memory, units = match.groups()

        # Now we call the methods specific to the child class (PBSIO or SGEIO)
        power_labels = self.power_labels

        if not units:
            units = self.default_unit
        elif units.lower() not in power_labels:
            raise OutputParsingError(f"Unknown units {units}")

        try:
            v = int(memory)
        except (
            ValueError
        ) as exc:  # pragma: no cover - should not happen (matching numbers above)
            raise OutputParsingError from exc

        return v * (1024 ** power_labels[units.lower()])

    @staticmethod
    def _convert_time_to_str(time: float | timedelta) -> str:
        """
        Convert a time duration to the PBS/SGE format (HH:MM:SS).

        Parameters
        ----------
        time
            Time in seconds or a timedelta object.

        Returns
        -------
        str
            Formatted time string.
        """
        if not isinstance(time, timedelta):
            time = timedelta(seconds=time)

        hours, remainder = divmod(int(time.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours}:{minutes}:{seconds}"

    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Convert a QResources object to a dictionary of PBS/SGE options.
        """
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
            else:  # ProcessPlacement.SAME_NODE
                header_dict["place"] = "pack"
        else:
            raise UnsupportedResourcesError(
                f"process placement {resources.process_placement} is not supported for {self.system_name}"
            )

        header_dict["select"] = select

        if resources.email_address:
            header_dict["mail_user"] = resources.email_address
            header_dict["mail_type"] = "abe"

        if resources.scheduler_kwargs:
            header_dict.update(resources.scheduler_kwargs)

        return header_dict

    def _add_soft_walltime(self, header_dict: dict, resources: QResources) -> None:
        """
        Add soft_walltime if required by child classes (e.g., SGE).
        """

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
            "memory_per_thread",
            "email_address",
            "scheduler_kwargs",
            "rerunnable",
        ]
        return supported
