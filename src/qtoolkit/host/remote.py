from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path

import fabric

from qtoolkit.host.base import BaseHost, HostConfig

# from fabric import Connection, Config


@dataclass
class RemoteConfig(HostConfig):
    # Fabric's Connection init args:
    host: str
    user: str = None
    port: int = None
    # Here we could just provide a config_filename
    config: fabric.Config = field(default_factory=fabric.Config)
    gateway: fabric.Connection | str = None
    forward_agent: bool = None
    connect_timeout: int = None
    connect_kwargs: dict = None
    inline_ssh_env: bool = True


# connect_kwargs in paramiko:
# hostname,
# port = SSH_PORT,
# username = None,
# password = None,
# pkey = None,
# key_filename = None,
# timeout = None,
# allow_agent = True,
# look_for_keys = True,
# compress = False,
# sock = None,
# gss_auth = False,
# gss_kex = False,
# gss_deleg_creds = True,
# gss_host = None,
# banner_timeout = None,
# auth_timeout = None,
# gss_trust_dns = True,
# passphrase=None,
# disabled_algorithms=None,
"""
Connect to an SSH server and authenticate to it.  The server's host key
is checked against the system host keys (see `load_system_host_keys`)
and any local host keys (`load_host_keys`).  If the server's hostname
is not found in either set of host keys, the missing host key policy
is used (see `set_missing_host_key_policy`).  The default policy is
to reject the key and raise an `.SSHException`.

Authentication is attempted in the following order of priority:

    - The ``pkey`` or ``key_filename`` passed in (if any)

      - ``key_filename`` may contain OpenSSH public certificate paths
        as well as regular private-key paths; when files ending in
        ``-cert.pub`` are found, they are assumed to match a private
        key, and both components will be loaded. (The private key
        itself does *not* need to be listed in ``key_filename`` for
        this to occur - *just* the certificate.)

    - Any key we can find through an SSH agent
    - Any "id_rsa", "id_dsa" or "id_ecdsa" key discoverable in
      ``~/.ssh/``

      - When OpenSSH-style public certificates exist that match an
        existing such private key (so e.g. one has ``id_rsa`` and
        ``id_rsa-cert.pub``) the certificate will be loaded alongside
        the private key and used for authentication.

    - Plain username/password auth, if a password was given

If a private key requires a password to unlock it, and a password is
passed in, that password will be used to attempt to unlock the key.

:param str hostname: the server to connect to
:param int port: the server port to connect to
:param str username:
    the username to authenticate as (defaults to the current local
    username)
:param str password:
    Used for password authentication; is also used for private key
    decryption if ``passphrase`` is not given.
:param str passphrase:
    Used for decrypting private keys.
:param .PKey pkey: an optional private key to use for authentication
:param str key_filename:
    the filename, or list of filenames, of optional private key(s)
    and/or certs to try for authentication
:param float timeout:
    an optional timeout (in seconds) for the TCP connect
:param bool allow_agent:
    set to False to disable connecting to the SSH agent
:param bool look_for_keys:
    set to False to disable searching for discoverable private key
    files in ``~/.ssh/``
:param bool compress: set to True to turn on compression
:param socket sock:
    an open socket or socket-like object (such as a `.Channel`) to use
    for communication to the target host
:param bool gss_auth:
    ``True`` if you want to use GSS-API authentication
:param bool gss_kex:
    Perform GSS-API Key Exchange and user authentication
:param bool gss_deleg_creds: Delegate GSS-API client credentials or not
:param str gss_host:
    The targets name in the kerberos database. default: hostname
:param bool gss_trust_dns:
    Indicates whether or not the DNS is trusted to securely
    canonicalize the name of the host being connected to (default
    ``True``).
:param float banner_timeout: an optional timeout (in seconds) to wait
    for the SSH banner to be presented.
:param float auth_timeout: an optional timeout (in seconds) to wait for
    an authentication response.
:param dict disabled_algorithms:
    an optional dict passed directly to `.Transport` and its keyword
    argument of the same name."""


class RemoteHost(BaseHost):
    """
    Execute commands on a remote host.
    For some commands assumes the remote can run unix
    """

    def __init__(self, config: RemoteConfig):
        self.config = config
        self._connection = fabric.Connection(
            host=self.config.host,
            user=self.config.user,
        )

    @property
    def connection(self):
        return self._connection

    def execute(self, command: str | list[str], workdir: str | Path | None = None):
        """Execute the given command on the host

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str.
        workdir: str or None
            path where the command will be executed.

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

        # TODO: check here if we use the context manager. What happens if we provide the
        #  connection from outside (not through a config) and we want to keep it alive ?

        # TODO: check if this works:
        if not workdir:
            workdir = "."
        else:
            workdir = str(workdir)
        with self.connection.cd(workdir):
            out = self.connection.run(command, hide=True, warn=True)

        return out.stdout, out.stderr, out.exited

    def mkdir(self, directory, recursive: bool = True, exist_ok: bool = True) -> bool:
        """Create directory on the host."""
        command = "mkdir "
        if recursive:
            command += "-p "
        command += str(directory)
        try:
            stdout, stderr, returncode = self.execute(command)
            return returncode == 0
        except Exception:
            return False

    def write_text_file(self, filepath, content):
        """Write content to a file on the host."""
        f = io.StringIO(content)

        self.connection.put(f, str(filepath))
