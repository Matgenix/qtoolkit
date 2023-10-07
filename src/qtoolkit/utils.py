from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def cd(path: str | Path):
    """
    A Fabric-inspired cd context that temporarily changes directory for
    performing some tasks, and returns to the original working directory
    afterwards. e.g.,

        with cd("/my/path/"):
            do_something()

    Imported from monty to avoid importing it.
    TODO: check if can be directly used from monty.

    Args:
        path: Path to cd to.
    """
    cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)
