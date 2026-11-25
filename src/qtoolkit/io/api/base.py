from __future__ import annotations

import abc
import re
from dataclasses import fields

from qtoolkit.core.base import QTKObject
from qtoolkit.core.data_objects import (
    CancelResult,
    QCloudResources,
    QJob,
    QResources,
    QState,
    SubmissionResult,
)
from qtoolkit.core.exceptions import InvalidJobIDError, UnsupportedResourcesError


class BaseAPISchedulerIO(QTKObject, abc.ABC):
    """Base class for schedulers controlled directly through a Python or HTTP API.

    Unlike :class:`~qtoolkit.io.base.BaseSchedulerIO`, which generates shell
    submission scripts and CLI commands to run via a :class:`~qtoolkit.manager.QueueManager`
    and :class:`~qtoolkit.host.base.BaseHost`, subclasses of ``BaseAPISchedulerIO``
    communicate directly with a scheduler API (such as AWS Batch, Google Cloud Batch,
    Azure Batch, or a REST HPC endpoint) to submit, inspect, list, and cancel jobs
    while returning standard :class:`~qtoolkit.QJob`, :class:`~qtoolkit.QState`,
    :class:`~qtoolkit.core.data_objects.SubmissionResult`, and
    :class:`~qtoolkit.core.data_objects.CancelResult` objects.

    Backend API failures during read operations (:meth:`get_job`,
    :meth:`get_jobs_list`) should raise :class:`~qtoolkit.core.exceptions.APIError`.
    """

    job_id_regex: str | None = None
    check_job_ids: bool = True
    supported_resources_cls: tuple[type, ...] = (QCloudResources, QResources)

    def submit(
        self,
        commands: str | list[str],
        options: dict | QCloudResources | QResources | None = None,
    ) -> SubmissionResult:
        """Submit a job through the scheduler API.

        Parameters
        ----------
        commands
            Command or list of command lines to execute, matching
            :class:`~qtoolkit.io.base.BaseSchedulerIO` conventions unless a
            backend is configured for raw container argument lists.
        options
            Backend-specific options dictionary or a resource object
            (:class:`~qtoolkit.QCloudResources` or :class:`~qtoolkit.QResources`).

        Returns
        -------
        SubmissionResult
            The result of the submission.
        """
        if isinstance(commands, str):
            if not commands.strip():
                raise ValueError("commands must not be empty.")
        elif isinstance(commands, list):
            if (
                not commands
                or any(not isinstance(c, str) for c in commands)
                or not any(c.strip() for c in commands)
            ):
                raise ValueError(
                    "commands must be a non-empty list of strings with at least one non-empty entry."
                )
        else:
            raise TypeError("commands must be a str or a list of str.")

        if options is None:
            converted_options: dict = {}
        elif isinstance(options, dict):
            converted_options = dict(options)
        elif isinstance(
            options, (QCloudResources, QResources, *self.supported_resources_cls)
        ):
            converted_options = self.check_convert_resources(options)
        else:
            raise TypeError(
                "options must be a dict, QCloudResources, QResources, or None."
            )

        return self._submit(commands=commands, options=converted_options)

    @abc.abstractmethod
    def _submit(self, commands: str | list[str], options: dict) -> SubmissionResult:
        """Backend-specific job submission implementation."""

    def cancel(self, job: QJob | int | str, reason: str | None = None) -> CancelResult:
        """Cancel or terminate a job through the scheduler API.

        Parameters
        ----------
        job
            Job identifier or :class:`~qtoolkit.QJob` object.
        reason
            Optional cancellation reason forwarded to backends that support one.

        Returns
        -------
        CancelResult
            The result of the cancellation.
        """
        job_id = self.extract_job_id(job)
        return self._cancel(job_id=job_id, reason=reason)

    @abc.abstractmethod
    def _cancel(self, job_id: str, reason: str | None = None) -> CancelResult:
        """Backend-specific job cancellation implementation."""

    def get_job(self, job: QJob | int | str) -> QJob | None:
        """Retrieve details of a single job.

        Parameters
        ----------
        job
            Job identifier or :class:`~qtoolkit.QJob` object.

        Returns
        -------
        QJob or None
            The :class:`~qtoolkit.QJob` object, or ``None`` if not found.
        """
        job_id = self.extract_job_id(job)
        return self._get_job(job_id=job_id)

    @abc.abstractmethod
    def _get_job(self, job_id: str) -> QJob | None:
        """Backend-specific single-job lookup implementation."""

    def get_jobs_list(
        self,
        jobs: list[QJob | int | str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
    ) -> list[QJob]:
        """Retrieve a list of jobs from the scheduler API.

        When ``jobs`` is ``None`` and ``states`` is ``None``, backends should
        default to returning active (non-terminal) jobs, matching ``squeue``
        and ``qstat`` behavior.

        Parameters
        ----------
        jobs
            Optional list of job identifiers to fetch. If an empty list is
            provided, an empty list is returned without querying the backend.
        user
            Optional username filter (for backends that support it).
        queue
            Optional queue or pool name filter.
        states
            Optional list of :class:`~qtoolkit.QState` values to filter by.

        Returns
        -------
        list of QJob
            The matching jobs.
        """
        if jobs is not None:
            if isinstance(jobs, (str, bytes)):
                raise TypeError(
                    "jobs must be a list of job identifiers, not str/bytes."
                )
            if len(jobs) == 0:
                return []
            job_ids = self.generate_ids_list(jobs)
        else:
            job_ids = None
        return self._get_jobs_list(
            job_ids=job_ids, user=user, queue=queue, states=states
        )

    @abc.abstractmethod
    def _get_jobs_list(
        self,
        job_ids: list[str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
    ) -> list[QJob]:
        """Backend-specific job listing implementation."""

    def check_convert_resources(self, resources: QCloudResources | QResources) -> dict:
        """Validate and convert a resource object to backend options.

        Parameters
        ----------
        resources
            Resource specification object.

        Returns
        -------
        dict
            Backend-specific options dictionary.

        Raises
        ------
        UnsupportedResourcesError
            If the resource class or any populated attribute is unsupported.
        """
        if not isinstance(resources, self.supported_resources_cls):
            supported = ", ".join(cls.__name__ for cls in self.supported_resources_cls)
            raise UnsupportedResourcesError(
                f"{type(self).__name__} does not support {type(resources).__name__}. "
                f"Supported resource types: {supported}."
            )

        not_empty = set()
        for field in fields(resources):
            val = getattr(resources, field.name)
            if val is not None and not (isinstance(val, dict) and len(val) == 0):
                not_empty.add(field.name)

        unsupported_options = not_empty.difference(self.supported_resources_keys)
        if unsupported_options:
            msg = f"Keys not supported: {', '.join(sorted(unsupported_options))}"
            raise UnsupportedResourcesError(msg)

        return self._convert_resources(resources)

    @abc.abstractmethod
    def _convert_resources(self, resources: QCloudResources | QResources) -> dict:
        """Convert a validated resource object into a backend options dictionary."""

    @property
    def supported_resources_keys(self) -> list[str]:
        """Attribute names handled by :meth:`_convert_resources`."""
        return []  # pragma: no cover

    def extract_job_id(self, job: QJob | int | str) -> str:
        """Extract and validate a single job ID from a ``QJob``, ``int``, or ``str``."""
        raw_id = job.job_id if isinstance(job, QJob) else job
        if raw_id is None:
            raise ValueError("Job ID must be defined. Received: None")
        if isinstance(raw_id, bool) or not isinstance(raw_id, (int, str)):
            raise TypeError(
                f"Job ID must be a QJob, int, or str. Received: {type(raw_id).__name__}"
            )
        job_id_str = str(raw_id)
        if not job_id_str.strip():
            raise ValueError("Job ID must be defined. Received: '' (empty string)")
        self._check_job_ids(job_id_str)
        return job_id_str

    def generate_ids_list(
        self, jobs: list[QJob | int | str] | None
    ) -> list[str] | None:
        """Normalize and validate a list of job identifiers."""
        if jobs is None:
            return None
        if isinstance(jobs, (str, bytes)):
            raise TypeError("jobs must be a list of job identifiers, not str/bytes.")
        return [self.extract_job_id(j) for j in jobs]

    def is_valid_job_id(self, job_id: str) -> bool:
        """Check whether ``job_id`` matches :attr:`job_id_regex`."""
        if self.job_id_regex is None:
            return True
        return re.fullmatch(self.job_id_regex, job_id) is not None

    def _check_job_ids(self, job_ids: str | list[str]) -> None:
        """Validate one or more job IDs against :attr:`job_id_regex`."""
        if not isinstance(job_ids, list):
            job_ids = [job_ids]
        if self.check_job_ids and self.job_id_regex:
            for job_id in job_ids:
                if not self.is_valid_job_id(job_id=job_id):
                    raise InvalidJobIDError(
                        f"Job ID '{job_id}' is invalid for this scheduler"
                    )
