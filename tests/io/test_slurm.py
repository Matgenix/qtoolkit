from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.core.data_objects import QState
from qtoolkit.io.slurm import SlurmIO, SlurmState

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
submit_ref_file = TEST_DIR / "io" / "slurm" / "parse_submit_output_inout.yaml"
in_out_submit_ref_list = loadfn(submit_ref_file)
cancel_ref_file = TEST_DIR / "io" / "slurm" / "parse_cancel_output_inout.yaml"
in_out_cancel_ref_list = loadfn(cancel_ref_file)


@pytest.fixture(scope="module")
def slurm_io():
    return SlurmIO()


class TestSlurmState:
    @pytest.mark.parametrize("slurm_state", [s for s in SlurmState])
    def test_qstate(self, slurm_state):
        assert isinstance(slurm_state.qstate, QState)
        assert SlurmState("CA") == SlurmState.CANCELLED
        assert SlurmState("CG") == SlurmState.COMPLETING
        assert SlurmState("CD") == SlurmState.COMPLETED
        assert SlurmState("CF") == SlurmState.CONFIGURING
        assert SlurmState("DL") == SlurmState.DEADLINE
        assert SlurmState("F") == SlurmState.FAILED
        assert SlurmState("OOM") == SlurmState.OUT_OF_MEMORY
        assert SlurmState("PD") == SlurmState.PENDING
        assert SlurmState("R") == SlurmState.RUNNING
        assert SlurmState("S") == SlurmState.SUSPENDED
        assert SlurmState("TO") == SlurmState.TIMEOUT


class TestSlurmIO:
    @pytest.mark.parametrize("in_out_ref", in_out_submit_ref_list)
    def test_parse_submit_output(self, slurm_io, in_out_ref, test_utils):
        parse_cmd_output, sr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_submit_kwargs", outkey="submission_result_ref"
        )
        sr = slurm_io.parse_submit_output(**parse_cmd_output)
        assert sr == sr_ref
        sr = slurm_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert sr == sr_ref
        sr = slurm_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert sr == sr_ref

    @pytest.mark.parametrize("in_out_ref", in_out_cancel_ref_list)
    def test_parse_cancel_output(self, slurm_io, in_out_ref, test_utils):
        parse_cmd_output, cr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_cancel_kwargs", outkey="cancel_result_ref"
        )
        cr = slurm_io.parse_cancel_output(**parse_cmd_output)
        assert cr == cr_ref
        cr = slurm_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert cr == cr_ref
        cr = slurm_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert cr == cr_ref
