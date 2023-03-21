from __future__ import annotations

import subprocess
from pathlib import Path

from qtoolkit.host.base import BaseHost
from qtoolkit.utils import cd


class LocalHost(BaseHost):
    # def __init__(self, config):
    #     self.config = config
    def execute(self, command: str | list[str], workdir: str | Path | None = None):
        """Execute the given command on the host

        Note that the command is executed with shell=True, so commands can
        be exposed to command injection. Consider whether to escape part of
        the input if it comes from external users.

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str

        Returns
        -------
        stdout : str
            Standard output of the command
        stderr : str
            Standard error of the command
        exit_code : int
            Exit code of the command.
        """
        if isinstance(command, (list, tuple)):
            command = " ".join(command)
        if not workdir:
            workdir = Path.cwd()
        else:
            workdir = str(workdir)
        with cd(workdir):
            proc = subprocess.run(command, capture_output=True, shell=True)
        return proc.stdout.decode(), proc.stderr.decode(), proc.returncode

    def mkdir(self, directory, recursive=True, exist_ok=True) -> bool:
        try:
            Path(directory).mkdir(parents=recursive, exist_ok=exist_ok)
        except OSError:
            return False
        return True

    def write_text_file(self, filepath, content) -> None:
        Path(filepath).write_text(content)
