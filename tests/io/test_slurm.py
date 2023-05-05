from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.io.slurm import SlurmIO

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
ref_file = TEST_DIR / "io" / "slurm" / "parse_submit_cmd_inout.yaml"
in_out_ref_list = loadfn(ref_file)


@pytest.mark.parametrize("in_out_ref", in_out_ref_list)
def test_parse_submit_cmd_output(in_out_ref, test_utils):
    parse_cmd_output, sr_ref = test_utils.inkwargs_outref(
        in_out_ref, inkey="parse_submit_kwargs", outkey="submission_result_ref"
    )
    slurm_io = SlurmIO()
    sr = slurm_io.parse_submit_output(**parse_cmd_output)
    assert sr == sr_ref
