from dataclasses import is_dataclass
from enum import Enum
from pathlib import Path

import pytest

module_dir = Path(__file__).resolve().parent
test_dir = module_dir / "test_data"
TEST_DIR = test_dir.resolve()


@pytest.fixture(scope="session")
def test_dir():
    return TEST_DIR


@pytest.fixture(scope="session")
def log_to_stdout():
    import logging
    import sys

    # Set Logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)


@pytest.fixture(scope="session")
def clean_dir(debug_mode):
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield
    if debug_mode:
        print(f"Tests ran in {newpath}")
    else:
        os.chdir(old_cwd)
        shutil.rmtree(newpath)


@pytest.fixture
def tmp_dir():
    """Same as clean_dir but is fresh for every test"""
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield
    os.chdir(old_cwd)
    shutil.rmtree(newpath)


@pytest.fixture(scope="session")
def debug_mode():
    return False


def is_msonable(obj):
    from monty.json import MSONable

    if not isinstance(obj, MSONable):
        return False
    if not obj.as_dict() == obj.__class__.from_dict(obj.as_dict()).as_dict():
        return False
    return True


class TestUtils:
    import json

    from monty.json import MSONable
    from monty.serialization import MontyDecoder, MontyEncoder

    @classmethod
    def is_msonable(cls, obj, obj_cls=None):
        if not isinstance(obj, cls.MSONable):
            return False
        obj_dict = obj.as_dict()
        if not obj_dict == obj.__class__.from_dict(obj_dict).as_dict():
            return False
        json_string = cls.json.dumps(obj_dict, cls=cls.MontyEncoder)
        obj_from_json = cls.json.loads(json_string, cls=cls.MontyDecoder)
        # When the class is defined as an inner class, the MontyDecoder is unable
        # to find it automatically. This is only used in the core/test_base tests.
        # The next check on the type of the obj_from_json is of course not relevant
        # in that specific case.
        if obj_cls is not None:
            obj_from_json = obj_cls.from_dict(obj_from_json)
        if not isinstance(obj_from_json, obj.__class__):
            return False
        if is_dataclass(obj) or isinstance(obj, Enum):
            return obj_from_json == obj
        return obj_from_json.as_dict() == obj.as_dict()

    @classmethod
    def inkwargs_outref(cls, in_out_ref, inkey, outkey):
        dec = cls.MontyDecoder()
        inkwargs_string = in_out_ref[inkey]
        inkwargs = dec.decode(inkwargs_string)
        outref_string = in_out_ref[outkey]
        outref = dec.decode(outref_string)
        return inkwargs, outref


@pytest.fixture(scope="session")
def test_utils():
    return TestUtils


@pytest.fixture(scope="session")
def maximalist_qresources():
    """A set of QResources options that try to make use of most features"""
    from qtoolkit.core.data_objects import QResources

    return QResources(
        queue_name="test_queue",
        job_name="test_job",
        memory_per_thread=1000,
        nodes=1,
        processes=1,
        processes_per_node=1,
        threads_per_process=1,
        gpus_per_job=1,
        time_limit=100,
        account="test_account",
        qos="test_qos",
        priority=1,
        output_filepath="test_output_filepath",
        error_filepath="test_error_filepath",
        process_placement="no_constraints",
        email_address="test_email_address@email.address",
        rerunnable=True,
        project="test_project",
        njobs=1,
    )
