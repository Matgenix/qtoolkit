from __future__ import annotations

import abc
import re
from dataclasses import fields
from enum import Enum
from typing import TYPE_CHECKING

from qtoolkit.core.exceptions import InvalidJobIDError, UnsupportedResourcesError

if TYPE_CHECKING:
    from qtoolkit.core.data_objects import BaseQResources

try:
    from monty.json import MSONable

    supercls = MSONable
    enum_superclses = (MSONable, Enum)

except ModuleNotFoundError:
    supercls = object
    enum_superclses = (Enum,)  # type: ignore


class QTKObject(supercls):  # type: ignore
    pass


class QTKEnum(*enum_superclses):  # type: ignore
    @classmethod
    def _validate_monty(cls, __input_value):
        """
        Override the original pydantic Validator for MSONable pattern.
        If not would not allow to deserialize as a standard Enum in pydantic,
        that just needs the value.
        """
        try:
            return super()._validate_monty(__input_value)
        except ValueError as exc:
            try:
                return cls(__input_value)
            except Exception:
                raise exc  # noqa: B904


class BaseScheduler(QTKObject, abc.ABC):
    """Shared job-ID, command, and resource-field helpers for scheduler IO backends."""

    job_id_regex: str | None = None
    check_job_ids: bool = True

    def generate_run_commands(self, commands: list[str] | str) -> str:
        if isinstance(commands, list):
            commands = "\n".join(commands)

        return commands

    def _validate_resource_fields(
        self,
        resources: BaseQResources,
        supported_keys: list[str],
        zero_is_set: bool = False,
    ) -> None:
        not_empty = set()
        for field in fields(resources):
            val = getattr(resources, field.name)
            if zero_is_set:
                # Only None and {} count as unset, so max_retries=0 is kept.
                if val is not None and val != {}:
                    not_empty.add(field.name)
            elif val:
                not_empty.add(field.name)

        unsupported_options = not_empty.difference(supported_keys)
        if unsupported_options:
            msg = f"Keys not supported: {', '.join(sorted(unsupported_options))}"
            raise UnsupportedResourcesError(msg)

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
