from __future__ import annotations

import abc

from qtoolkit.core.data_objects import (
    BaseQResources,
    CancelResult,
    QCloudResources,
    QJob,
    QResources,
    QState,
    SubmissionResult,
)
from qtoolkit.core.exceptions import UnsupportedResourcesError
from qtoolkit.io.base import _BaseScheduler


class BaseAPISchedulerIO(_BaseScheduler):
    """Base class for scheduler backends controlled directly through an API.

    Unlike :class:`~qtoolkit.io.base.BaseSchedulerIO`, subclasses execute submit,
    cancel, get, and list operations directly instead of generating shell scripts.
    Read methods (:meth:`get_job`, :meth:`get_jobs_list`) raise
    :class:`~qtoolkit.core.exceptions.APIError` on backend failures, while
    :meth:`submit` and :meth:`cancel` return ``FAILED`` result objects.
    All four methods raise :class:`~qtoolkit.core.exceptions.APIError` if
    creating the API client fails with a backend error (for example, an
    unknown credentials profile or no configured region).
    """

    supported_resources_cls: tuple[type, ...] = (QCloudResources, QResources)

    def submit(
        self,
        commands: str | list[str],
        options: dict | BaseQResources | None = None,
    ) -> SubmissionResult:
        """Submit a job through the scheduler API.

        Parameters
        ----------
        commands
            Command or list of command lines to execute.
        options
            Backend options dictionary or an instance of
            :attr:`supported_resources_cls`.

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
        elif isinstance(options, BaseQResources):
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
        """Cancel or terminate a job through the scheduler API."""
        return self._cancel(job_id=self.extract_job_id(job), reason=reason)

    @abc.abstractmethod
    def _cancel(self, job_id: str, reason: str | None = None) -> CancelResult:
        """Backend-specific job cancellation implementation."""

    def get_job(self, job: QJob | int | str) -> QJob | None:
        """Retrieve details of a single job, or ``None`` if not found."""
        return self._get_job(job_id=self.extract_job_id(job))

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
        """Retrieve jobs from the scheduler API.

        When ``jobs`` and ``states`` are both ``None``, backends default to
        active (non-terminal) jobs. An empty ``jobs`` list returns ``[]``
        without querying the backend.
        """
        job_ids = self.generate_ids_list(jobs)
        if job_ids == []:
            return []
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

    def check_convert_resources(self, resources: BaseQResources) -> dict:
        """Validate and convert a resource object into backend options."""
        if not isinstance(resources, self.supported_resources_cls):
            supported = ", ".join(cls.__name__ for cls in self.supported_resources_cls)
            raise UnsupportedResourcesError(
                f"{type(self).__name__} does not support {type(resources).__name__}. "
                f"Supported resource types: {supported}."
            )

        self._validate_resource_fields(
            resources, self.supported_resources_keys, ignore_empty_dict=True
        )
        return self._convert_resources(resources)

    @abc.abstractmethod
    def _convert_resources(self, resources: BaseQResources) -> dict:
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
