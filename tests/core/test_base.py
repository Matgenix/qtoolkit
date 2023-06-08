"""Unit tests for the core.base module of QToolKit."""
import enum
import importlib
import sys
from dataclasses import dataclass

import pytest

try:
    import monty
except ModuleNotFoundError:
    monty = None


@pytest.fixture
def qtk_core_base_mocked_monty_not_found(mocker):
    # Note:
    #     Here we use importlib to dynamically import the qtoolkit.core.base module.
    #     We want to test the QTKObject and QTKEnum super classes with monty present or not.
    #     This is done by mocking the import. We then need to use importlib to reload
    #     the qtoolkit.core.base module when we want to change the behaviour of the
    #     the monty.json import inside the qtoolkit.core.base module (i.e. mocking the
    #     import monty.json or doing the real import). This is due to "module caching"
    #     in python which stores imported modules in sys.modules. Using importlib.reload
    #     forces python to reevaluate the imported module instead of reusing the one
    #     already imported and available in sys.modules.
    #     Note that this is local to this test_base.py file as pytest
    orig_import = __import__

    def _import_mock(name, *args):
        if name == "monty.json":
            raise ModuleNotFoundError
        return orig_import(name, *args)

    mocker.patch("builtins.__import__", side_effect=_import_mock)

    if "qtoolkit.core.base" in sys.modules:
        yield importlib.reload(sys.modules["qtoolkit.core.base"])
    else:
        yield importlib.import_module("qtoolkit.core.base")
    del sys.modules["qtoolkit.core.base"]


class TestQBase:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        import qtoolkit.core.base as qbase

        @dataclass
        class QClass(qbase.QTKObject):
            name: str = "name"

        qc = QClass()
        assert test_utils.is_msonable(qc, obj_cls=QClass)

    def test_not_msonable(self, test_utils, qtk_core_base_mocked_monty_not_found):
        @dataclass
        class QClass(qtk_core_base_mocked_monty_not_found.QTKObject):
            name: str = "name"

        qc = QClass()
        assert not test_utils.is_msonable(qc)


class TestQEnum:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        import qtoolkit.core.base as qbase

        class SomeEnum(qbase.QTKEnum):
            VAL1 = "VAL1"
            VAL2 = "VAL2"

        se = SomeEnum("VAL1")
        assert test_utils.is_msonable(se, obj_cls=SomeEnum)
        assert isinstance(se, enum.Enum)

        se = SomeEnum.VAL2
        assert test_utils.is_msonable(se, obj_cls=SomeEnum)
        assert isinstance(se, enum.Enum)

        class SomeEnum(qbase.QTKEnum):
            VAL1 = 3
            VAL2 = 4

        se = SomeEnum(3)
        assert test_utils.is_msonable(se, obj_cls=SomeEnum)
        assert isinstance(se, enum.Enum)

        se = SomeEnum.VAL2
        assert test_utils.is_msonable(se, obj_cls=SomeEnum)
        assert isinstance(se, enum.Enum)

    def test_not_msonable(self, test_utils, qtk_core_base_mocked_monty_not_found):
        class SomeEnum(qtk_core_base_mocked_monty_not_found.QTKEnum):
            VAL1 = "VAL1"
            VAL2 = "VAL2"

        se = SomeEnum("VAL1")
        assert not test_utils.is_msonable(se)
        assert isinstance(se, enum.Enum)

        se = SomeEnum.VAL2
        assert not test_utils.is_msonable(se)
        assert isinstance(se, enum.Enum)

        class SomeEnum(qtk_core_base_mocked_monty_not_found.QTKEnum):
            VAL1 = 3
            VAL2 = 4

        se = SomeEnum(3)
        assert not test_utils.is_msonable(se)
        assert isinstance(se, enum.Enum)

        se = SomeEnum.VAL2
        assert not test_utils.is_msonable(se)
        assert isinstance(se, enum.Enum)
