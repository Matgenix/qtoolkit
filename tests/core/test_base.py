from dataclasses import dataclass

import pytest

try:
    import monty
except ModuleNotFoundError:
    monty = None


@pytest.mark.skipif(monty is None, reason="monty is not installed")
def test_msonable(test_utils):
    from qtoolkit.core.base import QBase

    @dataclass
    class QClass(QBase):
        name: str = "name"

    qc = QClass()
    assert test_utils.is_msonable(qc)


def test_not_msonable(test_utils, mocker):
    import importlib
    import sys

    orig_import = __import__

    def _import_mock(name, *args):
        if name == "monty.json":
            raise ModuleNotFoundError
        return orig_import(name, *args)

    mocker.patch("builtins.__import__", side_effect=_import_mock)
    if "qtoolkit.core.base" in sys.modules:
        qbase = importlib.reload(sys.modules["qtoolkit.core.base"])
    else:
        qbase = importlib.import_module("qtoolkit.core.base")

    @dataclass
    class QClass(qbase.QBase):
        name: str = "name"

    qc = QClass()
    assert not test_utils.is_msonable(qc)
