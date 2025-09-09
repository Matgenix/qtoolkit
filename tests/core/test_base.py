"""Unit tests for the core.base module of QToolKit."""

import enum
import importlib
import sys
from dataclasses import dataclass

import pytest
from monty.json import jsanitize

try:
    import monty
except ModuleNotFoundError:
    monty = None


@pytest.fixture()
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

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_serialization(self, mocker, test_utils):
        from monty.json import MontyDecoder, MontyEncoder
        from pydantic import BaseModel

        import qtoolkit.core.base as qbase

        class SomeEnum(qbase.QTKEnum):
            VAL1 = "VAL1"
            VAL2 = "VAL2"

        # Manually patch the monty decoder to allow decoding of the classes defined in this test
        class TestDecoder(MontyDecoder):
            def process_decoded(self, d):
                if isinstance(d, dict):
                    if d.get("@class") == "SomeEnum":
                        return SomeEnum.from_dict(d)
                    if d.get("@class") == "SomeModel":
                        mydict = {k: self.process_decoded(v) for k, v in d.items()}
                        return SomeModel(**mydict)
                return super().process_decoded(d)

        class SomeModel(BaseModel):
            value: SomeEnum
            ding: dict

        result = SomeEnum._validate_monty("VAL1")
        assert result == SomeEnum.VAL1
        assert isinstance(result, qbase.QTKEnum)
        assert isinstance(result, SomeEnum)

        with pytest.raises(
            ValueError, match=r"Must provide SomeEnum, the as_dict form, or the proper"
        ):
            SomeEnum._validate_monty("Bad value")

        qtkenum_validate_monty_spy = mocker.spy(qbase.QTKEnum, "_validate_monty")
        se = SomeEnum("VAL1")
        se_encoded = MontyEncoder().default(se)
        se_decoded = TestDecoder().process_decoded(se_encoded)

        assert se == se_decoded
        assert qtkenum_validate_monty_spy.call_count == 0

        some_model = SomeModel(value=se, ding={"hello": "toto"})
        assert qtkenum_validate_monty_spy.call_count == 1
        some_model_encoded = jsanitize(MontyEncoder().default(some_model))
        assert some_model_encoded == {
            "value": {
                "@module": "tests.core.test_base",
                "@class": "SomeEnum",
                "@version": None,
                "value": "VAL1",
            },
            "ding": {"hello": "toto"},
            "@module": "tests.core.test_base",
            "@class": "SomeModel",
            "@version": None,
        }
        some_model_decoded = TestDecoder().process_decoded(some_model_encoded)
        assert some_model == some_model_decoded
        assert qtkenum_validate_monty_spy.call_count == 2

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
