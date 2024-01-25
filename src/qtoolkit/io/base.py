from __future__ import annotations

import abc
import shlex
from dataclasses import fields
from pathlib import Path
from string import Template

from qtoolkit.core.base import QTKObject
from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.core.exceptions import UnsupportedResourcesError


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

    SUBMIT_CMD: str | None
    CANCEL_CMD: str | None

    shebang: str = "#!/bin/bash"

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
        extra = keys.difference(template.get_identifiers())
        if extra:
            msg = f"The following keys are not present in the template: {', '.join(sorted(extra))}"
            raise ValueError(msg)

        unclean_header = template.safe_substitute(options)
        # Remove lines with leftover $$.
        clean_header = []
        for line in unclean_header.split("\n"):
            if "$$" not in line:
                clean_header.append(line)

        return "\n".join(clean_header)

    def generate_run_commands(self, commands: list[str] | str) -> str:
        if isinstance(commands, list):
            commands = "\n".join(commands)

        return commands

    def generate_footer(self) -> str:
        return ""

    def generate_ids_list(self, jobs: list[QJob | int | str] | None) -> list[str]:
        if jobs is None:
            return None
        ids_list = []
        for j in jobs:
            if isinstance(j, QJob):
                ids_list.append(str(j.job_id))
            else:
                ids_list.append(str(j))

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
    def parse_submit_output(self, exit_code, stdout, stderr) -> SubmissionResult:
        pass

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
        return f"{self.CANCEL_CMD} {job_id}"

    @abc.abstractmethod
    def parse_cancel_output(self, exit_code, stdout, stderr) -> CancelResult:
        pass

    def get_job_cmd(self, job: QJob | int | str) -> str:
        job_id = self.generate_ids_list([job])[0]
        shlex.quote(job_id)
        return self._get_job_cmd(job_id)

    @abc.abstractmethod
    def _get_job_cmd(self, job_id: str) -> str:
        pass

    @abc.abstractmethod
    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        pass

    def check_convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a Qresources instance to a dict that will be used to fill in the
        header of the submission script.
        Also checks that passed values are declared to be handled by the corresponding
        subclass.
        """
        not_none = set()
        for field in fields(resources):
            if getattr(resources, field.name) is not None:
                not_none.add(field.name)

        unsupported_options = not_none.difference(self.supported_qresources_keys)

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
        return []

    def get_jobs_list_cmd(
        self, jobs: list[QJob | int | str] | None, user: str | None
    ) -> str:
        job_ids = self.generate_ids_list(jobs)
        if user:
            user = shlex.quote(user)
        return self._get_jobs_list_cmd(job_ids, user)

    @abc.abstractmethod
    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        pass

    @abc.abstractmethod
    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        pass
