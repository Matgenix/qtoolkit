from __future__ import annotations

import abc
from typing import Any

from qtoolkit.core.base import BaseScheduler
from qtoolkit.core.data_objects import (
    BaseQResources,
    CancelResult,
    QCloudResources,
    QJob,
    QState,
    SubmissionResult,
)
from qtoolkit.core.exceptions import UnsupportedResourcesError


class BaseAPISchedulerIO(BaseScheduler):
    """Base class for schedulers driven through an API instead of shell commands.

    If the scheduler rejects a request, for example because of an invalid
    parameter, :meth:`submit` and :meth:`cancel` return a result with status
    ``FAILED``. Errors that do not say whether the request was accepted or
    that may go away on a retry, such as connection errors, timeouts,
    throttling, server errors, and missing credentials, raise
    :class:`~qtoolkit.core.exceptions.APIError`. :meth:`get_job` and
    :meth:`get_jobs_list` raise ``APIError`` for all failed requests.

    The API client is created on first use by :meth:`_create_client` and is
    not kept when the object is pickled or copied, so the object can be sent
    to other processes.

    Parameters
    ----------
    shell_cmd
        Shell that runs the commands of a job as ``[*shell_cmd, "-c", script]``.
        A ``str`` is the path of the shell executable. A ``list[str]`` also
        holds options, for example ``["/bin/bash", "-l"]`` for a login shell.
        The default is ``"/bin/bash"``, the same shell as the submission
        scripts of the HPC schedulers. The container image must provide it.
    """

    supported_resources_cls: tuple[type, ...] = (QCloudResources,)

    def __init__(self, shell_cmd: str | list[str] = "/bin/bash") -> None:
        if isinstance(shell_cmd, str):
            valid = bool(shell_cmd.strip())
        else:
            valid = (
                isinstance(shell_cmd, list)
                and bool(shell_cmd)
                and all(isinstance(arg, str) and arg.strip() for arg in shell_cmd)
            )
        if not valid:
            raise ValueError(
                "shell_cmd must be a non-empty str or a non-empty list of non-empty str."
            )
        self.shell_cmd = shell_cmd
        self._client: Any = None

    def submit(
        self,
        commands: str | list[str] | None,
        options: dict | BaseQResources | None = None,
    ) -> SubmissionResult:
        """Submit a job through the scheduler API.

        Parameters
        ----------
        commands
            Command or list of command lines that run in :attr:`shell_cmd`, as
            in the submission script of an HPC scheduler. With ``None`` the
            job runs the command set in ``options`` or the default command of
            the job definition or image, if the backend supports it.
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
                    "commands must be a list of str with at least one non-empty entry."
                )
        elif commands is not None:
            raise TypeError("commands must be a str, a list of str, or None.")

        if options is None:
            converted_options: dict = {}
        elif isinstance(options, dict):
            converted_options = dict(options)
        elif isinstance(options, BaseQResources):
            converted_options = self.check_convert_qresources(options)
        else:
            supported = ", ".join(cls.__name__ for cls in self.supported_resources_cls)
            raise TypeError(f"options must be None, a dict, or one of: {supported}.")

        return self._submit(commands=commands, options=converted_options)

    @abc.abstractmethod
    def _submit(
        self, commands: str | list[str] | None, options: dict
    ) -> SubmissionResult:
        """Backend-specific job submission implementation."""

    def cancel(self, job: QJob | int | str, reason: str | None = None) -> CancelResult:
        """Cancel or terminate a job through the scheduler API."""
        return self._cancel(job_id=self.to_job_id(job), reason=reason)

    @abc.abstractmethod
    def _cancel(self, job_id: str, reason: str | None = None) -> CancelResult:
        """Backend-specific job cancellation implementation."""

    def get_job(self, job: QJob | int | str) -> QJob | None:
        """Retrieve details of a single job, or ``None`` if not found."""
        return self._get_job(job_id=self.to_job_id(job))

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
        if isinstance(jobs, str):
            raise TypeError("jobs must be a list, not a str.")
        if jobs is not None and not jobs:
            return []
        job_ids = None if jobs is None else [self.to_job_id(job) for job in jobs]
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

    def check_convert_qresources(self, resources: BaseQResources) -> dict:
        """Validate and convert a resource object into backend options."""
        if not isinstance(resources, self.supported_resources_cls):
            supported = ", ".join(cls.__name__ for cls in self.supported_resources_cls)
            raise UnsupportedResourcesError(
                f"{type(self).__name__} does not support {type(resources).__name__}. "
                f"Supported resource types: {supported}."
            )

        self._validate_resource_fields(
            resources, self.supported_qresources_keys, zero_is_set=True
        )
        return self._convert_qresources(resources)

    @abc.abstractmethod
    def _convert_qresources(self, resources: BaseQResources) -> dict:
        """Convert a validated resource object into a backend options dictionary."""

    @property
    def supported_qresources_keys(self) -> list[str]:
        """Attribute names handled by :meth:`_convert_qresources`."""
        return []  # pragma: no cover

    def to_job_id(self, job: QJob | int | str) -> str:
        """Return the job ID of a ``QJob``, ``int``, or ``str`` as a string."""
        job_id = job.job_id if isinstance(job, QJob) else job
        if job_id is None or not str(job_id).strip():
            raise ValueError(f"Job ID must not be empty. Received: {job_id!r}")
        job_id = str(job_id)
        self._check_job_ids(job_id)
        return job_id

    def format_command(self, commands: str | list[str]) -> list[str]:
        """Return ``commands`` as the argument list that runs them in :attr:`shell_cmd`.

        The command lines are joined into one script that runs as
        ``[*shell_cmd, "-c", script]``.
        """
        shell = [self.shell_cmd] if isinstance(self.shell_cmd, str) else self.shell_cmd
        return [*shell, "-c", self.generate_run_commands(commands)]

    @abc.abstractmethod
    def _create_client(self) -> Any:
        """Create the API client of the backend.

        Raises
        ------
        APIError
            If the client cannot be created.
        """

    def _get_client(self) -> Any:
        """Return the API client, created on the first call."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def __getstate__(self) -> dict:
        # API clients usually cannot be pickled or copied. The copy creates
        # its own client on first use.
        state = self.__dict__.copy()
        state["_client"] = None
        return state
