from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.queue.slurm import SlurmQueue

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
ref_file = TEST_DIR / "queue" / "slurm" / "parse_submit_cmd_inout.yaml"
in_out_ref_list = loadfn(ref_file)


@pytest.mark.parametrize("in_out_ref", in_out_ref_list)
def test_parse_submit_cmd_output(in_out_ref, test_utils):
    parse_cmd_output, sr_ref = test_utils.inkwargs_outref(
        in_out_ref, inkey="parse_submit_kwargs", outkey="submission_result_ref"
    )
    slurm_q = SlurmQueue()
    sr = slurm_q._parse_submit_cmd_output(**parse_cmd_output)
    assert sr == sr_ref
