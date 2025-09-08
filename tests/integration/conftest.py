from __future__ import annotations

import socket
import tempfile
import time
from pathlib import Path

import pytest
from python_on_whales import DockerClient
from python_on_whales import docker as docker_pow


def _get_free_port(upper_bound=90_000):
    """Returns a random free port, with an upper bound.

    The upper bound is required as Docker does not have
    permissions on high port numbers on some systems.

    """
    port = upper_bound + 1
    attempts = 0
    max_attempts = 10
    while port > upper_bound and attempts < max_attempts:
        sock = socket.socket()
        sock.bind(("", 0))
        port = sock.getsockname()[1]
        attempts += 1

    if attempts == max_attempts:
        raise RuntimeError(
            f"Could not find a free port to use with the provided {upper_bound=}."
        )

    return port


@pytest.fixture(scope="session")
def slurm_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def sge_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def pbs_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def slurm_host(slurm_ssh_port):
    from qtoolkit.host.remote import RemoteConfig, RemoteHost

    def _make_host(username: str):
        conf = RemoteConfig(
            root_dir=f"/home/{username}",
            host="localhost",
            port=slurm_ssh_port,
            user=username,
            connect_kwargs={"password": username},
        )
        return RemoteHost(conf)

    return _make_host


@pytest.fixture(scope="session")
def sge_host(sge_ssh_port):
    from qtoolkit.host.remote import RemoteConfig, RemoteHost

    def _make_host(username: str):
        conf = RemoteConfig(
            root_dir=f"/home/{username}",
            host="localhost",
            port=sge_ssh_port,
            user=username,
            connect_kwargs={"password": username},
        )
        return RemoteHost(conf)

    return _make_host


@pytest.fixture(scope="session")
def pbs_host(pbs_ssh_port):
    from qtoolkit.host.remote import RemoteConfig, RemoteHost

    def _make_host(username: str):
        conf = RemoteConfig(
            root_dir=f"/home/{username}",
            host="localhost",
            port=pbs_ssh_port,
            user=username,
            connect_kwargs={"password": username},
        )
        return RemoteHost(conf)

    return _make_host


@pytest.fixture(scope="session", autouse=True)
def bake_containers():
    hcl_path = Path(__file__).parent.resolve() / "dockerfiles/docker-bake.hcl"
    docker_pow.buildx.bake(
        targets=["slurm", "sge", "pbs"],
        files=hcl_path,
        set={"*.context": str(Path(__file__).parent.parent.parent.resolve())},
    )


@pytest.fixture(scope="session", autouse=True)
def compose_containers(
    slurm_ssh_port, sge_ssh_port, pbs_ssh_port, bake_containers, pytestconfig
):
    compose_yaml = f"""
name: qtoolkit_testing
services:

  qtoolkit_testing_slurm:
    image: ghcr.io/matgenix/qtoolkit-testing-slurm:latest
    container_name: qtoolkit_slurm
    ports:
      - "{slurm_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s

  qtoolkit_testing_sge:
    image: ghcr.io/matgenix/qtoolkit-testing-sge:latest
    container_name: qtoolkit_sge
    ports:
      - "{sge_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s

  qtoolkit_testing_pbs:
    image: ghcr.io/matgenix/qtoolkit-testing-pbs:latest
    container_name: qtoolkit_pbs
    ports:
      - "{pbs_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s
"""
    with tempfile.NamedTemporaryFile("wt", suffix="compose.yaml", delete=False) as f:
        f.write(compose_yaml)
        f.flush()

        docker_client = DockerClient(compose_files=[f.name])
        try:
            print("\n * Launching compose...")

            compose_version = docker_client.compose.up(
                detach=True,
            )
            print(" * Waiting for container to be ready...", end="")
            max_retries = 30
            retries = 0
            while retries < max_retries:
                containers = docker_client.compose.ps()

                if all(
                    c.state.health and c.state.health.status == "healthy"
                    for c in containers
                ):
                    print(f"\n{docker_client.compose.logs()}\n")
                    print(f"\n * {compose_version} launched.")
                    break

                exited = [c.id for c in containers if c.state.status == "exited"]
                if any(exited):
                    logs = f"\n{docker_client.compose.logs()}\n"
                    pytest.fail(
                        f"Containers {', '.join(exited)} exited before being ready.\nFull logs: {logs}"
                    )

                print(".", end="")
                time.sleep(1)
                retries += 1
            else:
                not_started = [
                    c.id
                    for c in containers
                    if not c.state.health or c.state.health.status != "healthy"
                ]
                logs = f"\n{docker_client.compose.logs()}\n"
                pytest.fail(
                    f"Containers {', '.join(not_started)} did not start in time. Full logs: {logs}"
                )

            yield docker_client

        finally:
            if pytestconfig.getoption("keep_containers_alive"):
                print("\n * Keeping containers alive...")
                print(f"\n  - Docker compose yaml file: {f.name}")
                print("\n  - Docker containers:")
                containers = docker_client.compose.ps()
                for c in containers:
                    print(f"\n    - {c.name}")
            else:
                try:
                    print("\n * Stopping containers...")
                    try:
                        docker_client.compose.stop()
                    except Exception:  # noqa: S110
                        pass

                    try:
                        docker_client.compose.kill()
                    except Exception:  # noqa: S110
                        pass

                    try:
                        docker_client.compose.rm(volumes=True)
                    except Exception:  # noqa: S110
                        pass

                    print(" * Done!")
                except Exception as exc:
                    print(f" x Failed to stop container: {exc}")
