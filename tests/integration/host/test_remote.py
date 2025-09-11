# ruff: noqa: SLF001

import os
import re

import pytest
from python_on_whales import DockerException

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_remote(slurm_host, mocker, compose_containers):
    host = slurm_host("johndoe")

    connection_spy = mocker.spy(host.connection, "run")

    stdout, stderr, return_code = host.execute(["test", "-e", "/home/johndoe"])
    connection_spy.assert_called_once()
    args, kwargs = connection_spy.call_args
    # Here we remove multiple spaces as
    assert re.sub(r"\s+", " ", args[0]).strip() == "bash -l -c 'test -e /home/johndoe'"
    assert stdout == ""
    assert stderr == ""
    assert return_code == 0

    docker_client = compose_containers
    # Make sure the directory does not exist yet
    with pytest.raises(DockerException):
        docker_client.execute(
            "qtoolkit_slurm", ["test", "-e", "/home/johndoe/remote_tests"]
        )

    assert host.mkdir("/home/johndoe/remote_tests") is True
    # Now we check that the directory was created
    output = docker_client.execute(
        "qtoolkit_slurm", ["test", "-d", "/home/johndoe/remote_tests"], user="johndoe"
    )
    assert output == ""
    assert host.mkdir("/home/johndoe/remote_tests", exist_ok=False) is False
    assert (
        host.mkdir("/home/johndoe/remote_tests", exist_ok=False, recursive=False)
        is False
    )
    assert (
        host.mkdir(
            "/home/johndoe/remote_tests/subdir/subsubdir",
            exist_ok=False,
            recursive=False,
        )
        is False
    )
    # Now we check that the directory was not created
    with pytest.raises(DockerException):
        docker_client.execute(
            "qtoolkit_slurm",
            ["test", "-e", "/home/johndoe/remote_tests/subdir/subsubdir"],
        )

    connection_spy.reset_mock()
    host.config.shell_cmd = ""
    stdout, stderr, return_code = host.execute(["pwd"])
    connection_spy.assert_called_once()
    args, kwargs = connection_spy.call_args
    assert args[0].strip() == "pwd"
    assert stdout.strip() == "/home/johndoe"
    assert stderr == ""
    assert return_code == 0

    connection_spy.reset_mock()
    host.config.shell_cmd = "bash"
    host.config.login_shell = False
    stdout, stderr, return_code = host.execute(["pwd"])
    connection_spy.assert_called_once()
    args, kwargs = connection_spy.call_args
    assert re.sub(r"\s+", " ", args[0]).strip() == "bash -c pwd"
    assert stdout.strip() == "/home/johndoe"
    assert stderr == ""
    assert return_code == 0

    assert (
        host.mkdir("/home/johndoe/remote_tests/subdir/subsubdir", recursive=False)
        is False
    )
    # Now we check that the directory was not created
    with pytest.raises(DockerException):
        docker_client.execute(
            "qtoolkit_slurm",
            ["test", "-e", "/home/johndoe/remote_tests/subdir/subsubdir"],
        )

    assert (
        host.mkdir("/home/johndoe/remote_tests/subdir/subsubdir", recursive=True)
        is True
    )
    # Now we check that the directory was created
    output = docker_client.execute(
        "qtoolkit_slurm",
        ["test", "-d", "/home/johndoe/remote_tests/subdir/subsubdir"],
        user="johndoe",
    )
    assert output == ""

    assert (
        host.mkdir(
            "/home/johndoe/remote_tests/subdir/subsubdir",
            recursive=True,
            exist_ok=False,
        )
        is False
    )
    assert (
        host.mkdir(
            "/home/johndoe/remote_tests/subdir/subsubdir", recursive=True, exist_ok=True
        )
        is True
    )
