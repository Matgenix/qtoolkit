import os
from contextlib import contextmanager


@contextmanager
def cd(path):
    """
    A Fabric-inspired cd context that temporarily changes directory for
    performing some tasks, and returns to the original working directory
    afterwards. E.g.,
        with cd("/my/path/"):
            do_something()

    Imported from monty to avoid importing it.
    TODO: check if can be directly used from monty.

    Args:
        path: Path to cd to.
    """
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)
