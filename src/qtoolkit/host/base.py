from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path

from qtoolkit.core.base import QBase


@dataclass
class HostConfig(QBase):
    root_dir: str | Path


class BaseHost(QBase):
    """Base Host class."""

    # def __init__(self, config, user):
    def __init__(self, config: HostConfig | None = None) -> None:
        self.config = config

    #     self.user = user

    @abc.abstractmethod
    def execute(self, command: str | list[str], stdin=None, stdout=None, stderr=None):
        """Execute the given command on the host

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str
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
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self, directory, recursive: bool = True, exist_ok: bool = True):
        """Create directory on the host."""
        raise NotImplementedError
