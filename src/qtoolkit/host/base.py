from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING

from qtoolkit.core.base import QTKObject

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class HostConfig(QTKObject):
    root_dir: str | Path


class BaseHost(QTKObject):
    """Base Host class."""

    def __init__(self, config: HostConfig | None = None) -> None:
        self.config = config

    @abc.abstractmethod
    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
    ) -> tuple[str, str, int]:
        """Execute the given command on the host.

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str
        workdir: str or None
            path where the command will be executed.

        Returns
        -------
        tuple[str, str, int]
            Standard output, standard error, and exit code of the command.
        """
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """Create directory on the host."""
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError

    @abc.abstractmethod
    def write_text_file(self, filepath: str | Path, content: str) -> None:
        """Write content to a file on the host."""
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError
