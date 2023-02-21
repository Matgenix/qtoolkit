from qtoolkit.host.base import BaseHost


class RemoteHost(BaseHost):
    def execute(self, command):
        """Execute the given command on the host

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str.

        Returns
        -------
        stdout : str
            Standard output of the command
        stderr : str
            Standard error of the command
        exit_code : int
            Exit code of the command.
        """
        raise NotImplementedError
