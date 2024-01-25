import os
from pathlib import Path

from monty.tempfile import ScratchDir

from qtoolkit.utils import cd


def test_cd():
    with ScratchDir("."):
        dirpath = Path("mydir")
        dirpath.mkdir()
        filepath = dirpath / "empty_file.txt"
        filepath.touch()

        with cd(dirpath):
            assert os.path.exists("empty_file.txt")
        assert not os.path.exists("empty_file.txt")
