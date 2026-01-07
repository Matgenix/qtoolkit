from __future__ import annotations

import abc
import difflib
import re
import shlex
from dataclasses import fields
from pathlib import Path
from string import Template

from qtoolkit.core.base import QTKObject
from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.core.exceptions import InvalidJobIDError, UnsupportedResourcesError

MODULE_DIR = Path(__file__).absolute().parent
TEMPLATE_DIR = Path(f"{MODULE_DIR}/templates")


class QTemplate(Template):
    delimiter = "$$"

    def get_identifiers(self) -> list:
        """
        Returns a list of the valid identifiers in the template,
        in the order they first appear, ignoring any invalid identifiers.
        Imported from implementation in python 3.11 for backward compatibility.
        """
        ids = []
        for mo in self.pattern.finditer(self.template):
            named = mo.group("named") or mo.group("braced")
            if named is not None and named not in ids:
                # add a named group only the first time it appears
                ids.append(named)
            elif (
                named is None
                and mo.group("invalid") is None
                and mo.group("escaped") is None
            ):  # pragma: no cover - no complex patterns, part of python stdlib 3.11
                # If all the groups are None, there must be
                # another group we're not expecting
                raise ValueError("Unrecognized named group in pattern", self.pattern)
        return ids


class BaseSchedulerIO(QTKObject, abc.ABC):
    """Base class for job queues."""

    header_template: str
    header_template_file: str | None = None

    SUBMIT_CMD: str | None
    CANCEL_CMD: str | None

    shebang: str = "#!/bin/bash"

    sanitize_job_name: bool = False

    job_id_regex: str | None = None
    check_job_ids: bool = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.header_template_file:
            with open(TEMPLATE_DIR / cls.header_template_file) as f:
                cls.header_template = f.read()

    def get_submission_script(
        self,
        commands: str | list[str],
        options: dict | QResources | None = None,
    ) -> str:
        """Get the submission script for the given commands and options."""
        script_blocks = [self.shebang]
        if header := self.generate_header(options):
            script_blocks.append(header)

        run_commands = self.generate_run_commands(commands)
        script_blocks.append(run_commands)

        if footer := self.generate_footer():
            script_blocks.append(footer)

        return "\n".join(script_blocks)

    def generate_header(self, options: dict | QResources | None) -> str:
        """
        Generate the header (directives) for the submission script.

        Parameters
        ----------
        options
            Scheduler options or QResources object.

        Returns
        -------
        str
            The generated header string.
        """
        # needs info from self.meta_info (email, job name [also execution])
        # queuing_options (priority, account, qos and submit as hold)
        # execution (rerunnable)
        # resources (nodes, cores, memory, time, [gpus])
        # default values for (almost) everything in the object ?

        options = options or {}

        if isinstance(options, QResources):
            options = self.check_convert_qresources(options)

        template = QTemplate(self.header_template)

        # check that all the options are present in the template
        keys = set(options.keys())
        all_identifiers = template.get_identifiers()
        extra = keys.difference(all_identifiers)
        if extra:
            close_matches = {}
            for extra_val in extra:
                m = difflib.get_close_matches(
                    extra_val, all_identifiers, n=3, cutoff=0.65
                )
                if m:
                    close_matches[extra_val] = m
            msg = (
                f"The following keys are not present in the template: {', '.join(sorted(extra))}. "
                f"Check the template in {type(self).__module__}.{type(self).__qualname__}.header_template."
            )
            if close_matches:
                msg += " Possible replacements:"
                for extra_val in sorted(close_matches):
                    replacements = " or ".join(
                        f"'{m}'" for m in close_matches[extra_val]
                    )
                    msg += f" {replacements} instead of '{extra_val}'."
            raise ValueError(msg)

        options = self.sanitize_options(options)
        unclean_header = template.safe_substitute(options)
        # Remove lines with leftover $$.
        clean_header = [line for line in unclean_header.split("\n") if "$$" not in line]

        return "\n".join(clean_header)

    def generate_run_commands(self, commands: list[str] | str) -> str:
        if isinstance(commands, list):
            commands = "\n".join(commands)

        return commands

    def generate_footer(self) -> str:
        """
        Generate the footer for the submission script.

        Returns
        -------
        str
            The generated footer string.
        """
        return ""

    def generate_ids_list(
        self, jobs: list[QJob | int | str] | None
    ) -> list[str] | None:
        if jobs is None:
            return None
        ids_list = []
        for j in jobs:
            if isinstance(j, QJob):
                ids_list.append(str(j.job_id))
            else:
                ids_list.append(str(j))
        self._check_job_ids(ids_list)
        return ids_list

    def get_submit_cmd(self, script_file: str | Path | None = "submit.script") -> str:
        """
        Get the command used to submit a given script to the queue.

        Parameters
        ----------
        script_file: (str) path of the script file to use.
        """
        script_file = script_file or ""
        return f"{self.SUBMIT_CMD} {script_file}"

    @abc.abstractmethod
    def parse_submit_output(
        self, exit_code: int, stdout: str | bytes, stderr: str | bytes
    ) -> SubmissionResult:
        """
        Parse the output of a submission command.

        Parameters
        ----------
        exit_code : int
            Exit code of the command.
        stdout : str or bytes
            Standard output of the command.
        stderr : str or bytes
            Standard error of the command.

        Returns
        -------
        SubmissionResult
            The parsed submission result.
        """
        # pragma: no cover - implementation in subclasses

    def get_cancel_cmd(self, job: QJob | int | str) -> str:
        """
        Get the command used to cancel a given job.

        Parameters
        ----------
        job: (str) job to be cancelled.
        """
        job_id = job.job_id if isinstance(job, QJob) else job
        if job_id is None or job_id == "":
            received = None if job_id is None else "'' (empty string)"
            raise ValueError(
                f"The id of the job to be cancelled should be defined. Received: {received}"
            )
        self._check_job_ids(str(job_id))
        return f"{self.CANCEL_CMD} {job_id}"

    @abc.abstractmethod
    def parse_cancel_output(
        self, exit_code: int, stdout: str | bytes, stderr: str | bytes
    ) -> CancelResult:
        """
        Parse the output of a cancellation command.

        Parameters
        ----------
        exit_code
            Exit code of the command.
        stdout
            Standard output of the command.
        stderr
            Standard error of the command.

        Returns
        -------
        CancelResult
            The parsed cancellation result.
        """
        # pragma: no cover - implementation in subclasses

    def get_job_cmd(self, job: QJob | int | str) -> str:
        """
        Get the command used to retrieve information about a given job.

        Parameters
        ----------
        job
            Job identifier.

        Returns
        -------
        str
            The command string.
        """
        job_id = self.generate_ids_list([job])[0]
        shlex.quote(job_id)
        return self._get_job_cmd(job_id)

    @abc.abstractmethod
    def _get_job_cmd(self, job_id: str) -> str:
        pass  # pragma: no cover - implementation in subclasses

    @abc.abstractmethod
    def parse_job_output(
        self,
        exit_code: int,
        stdout: str | bytes,
        stderr: str | bytes,
        job_id: str | None = None,
    ) -> QJob | None:
        """Parse the output of a command to get a job and return the corresponding QJob object.

        Parameters
        ----------
        exit_code
            Exit code of the command.
        stdout
            Standard output of the command.
        stderr
            Standard error of the command.
        job_id
            Job ID of the parsed job.

        Returns
        -------
        QJob or None
            The parsed QJob object, or None if not found.
        """
        # pragma: no cover - implementation in subclasses

    def check_convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a Qresources instance to a dict that will be used to fill in the
        header of the submission script.
        Also checks that passed values are declared to be handled by the corresponding
        subclass.
        """
        not_empty = set()
        for field in fields(resources):
            if getattr(resources, field.name):
                not_empty.add(field.name)

        unsupported_options = not_empty.difference(self.supported_qresources_keys)

        if unsupported_options:
            msg = f"Keys not supported: {', '.join(sorted(unsupported_options))}"
            raise UnsupportedResourcesError(msg)

        return self._convert_qresources(resources)

    @abc.abstractmethod
    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a QResources instance to a dict that will be used to fill in the
        header of the submission script.
        A subclass does not strictly need to support all the options available in
        QResources. For this reason a list of supported attributes should be
        maintained and the supported attributes in the implementation of this
        method should match the list of values defined in  supported_qresources_keys.
        """

    @property
    def supported_qresources_keys(self) -> list:
        """
        List of attributes of QResources that are correctly handled by the
        _convert_qresources method. It is used to validate that the user
        does not pass an unsupported value, expecting to have an effect.
        """
        return []  # pragma: no cover - trivial and usually overwritten in subclasses

    def get_jobs_list_cmd(
        self, jobs: list[QJob | int | str] | None, user: str | None
    ) -> str:
        """
        Get the command used to list jobs, optionally filtered by IDs or user.

        Parameters
        ----------
        jobs
            List of job identifiers.
        user
            Username to filter by.

        Returns
        -------
        str
            The command string.
        """
        job_ids = self.generate_ids_list(jobs)
        if user:
            user = shlex.quote(user)
        return self._get_jobs_list_cmd(job_ids, user)

    @abc.abstractmethod
    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        pass  # pragma: no cover - implementation in subclasses

    @abc.abstractmethod
    def parse_jobs_list_output(
        self,
        exit_code: int,
        stdout: str | bytes,
        stderr: str | bytes,
        job_ids: list[str] | None = None,
    ) -> list[QJob]:
        """
        Parse the output of a command that lists jobs.

        Parameters
        ----------
        exit_code : int
            Exit code of the command.
        stdout : str or bytes
            Standard output of the command.
        stderr : str or bytes
            Standard error of the command.
        job_ids : list of str, optional
            List of expected job IDs.

        Returns
        -------
        list of QJob
            List of parsed QJob objects.
        """
        # pragma: no cover - implementation in subclasses

    def sanitize_options(self, options: dict) -> dict:
        """
        A function to sanitize the values in the options used to generate the
        header. Subclasses should implement their own sanitizations.

        Parameters
        ----------
        options
            Dictionary of options to sanitize.

        Returns
        -------
        dict
            Sanitized options.
        """
        return options

    def is_valid_job_id(self, job_id: str) -> bool:
        """
        Check if a given job identifier is valid for the current scheduler.

        Parameters
        ----------
        job_id
            The job identifier to check.

        Returns
        -------
        bool
            True if the job ID is valid, False otherwise.
        """
        if self.job_id_regex is None:
            return True
        return re.fullmatch(self.job_id_regex, job_id) is not None

    def _check_job_ids(self, job_ids: str | list[str]) -> None:
        """
        Check a list of job IDs for validity.

        Parameters
        ----------
        job_ids
            Job IDs to check.

        Raises
        ------
        InvalidJobIDError
            If any of the job IDs is invalid.
        """
        if not isinstance(job_ids, list):
            job_ids = [job_ids]
        if self.check_job_ids and self.job_id_regex:
            for job_id in job_ids:
                if not self.is_valid_job_id(job_id=job_id):
                    raise InvalidJobIDError(
                        f"Job ID '{job_id}' is invalid for this scheduler"
                    )
