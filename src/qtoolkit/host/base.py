from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path

from qtoolkit.core.base import QTKObject


@dataclass
class HostConfig(QTKObject):
    root_dir: str | Path


class BaseHost(QTKObject):
    """Base Host class."""

    # def __init__(self, config, user):
    def __init__(self, config: HostConfig | None = None) -> None:
        self.config = config

    #     self.user = user

    @abc.abstractmethod
    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        # stdin=None,
        # stdout=None,
        # stderr=None,
    ):
        """Execute the given command on the host

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str
        workdir: str or None
            path where the command will be executed.
        stdin: None, PIPE or file-like
            Standard input, /dev/null if None
        stdout: None, PIPE or file-like
            Standard output, /dev/null if None
        stderr: None, PIPE, DEVNULL or file-like
            Standard error, same as stdout if None

        Returns
        -------
        :py:class:`subprocess.Popen` object or None
            Local process object associated to the connection, if dryrun is False,
            else None
        """
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self, directory, recursive: bool = True, exist_ok: bool = True) -> bool:
        """Create directory on the host."""
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError

    @abc.abstractmethod
    def write_text_file(self, filepath, content):
        """Write content to a file on the host."""
        # TODO: define a common error that is raised or a returned in case the procedure
        # fails to avoid handling different kind of errors for the different hosts
        raise NotImplementedError
