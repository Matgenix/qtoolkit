from datetime import timedelta
from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.core.data_objects import ProcessPlacement, QResources, QState
from qtoolkit.core.exceptions import OutputParsingError, UnsupportedResourcesError
from qtoolkit.io.slurm import SlurmIO, SlurmState

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
submit_ref_file = TEST_DIR / "io" / "slurm" / "parse_submit_output_inout.yaml"
in_out_submit_ref_list = loadfn(submit_ref_file)
cancel_ref_file = TEST_DIR / "io" / "slurm" / "parse_cancel_output_inout.yaml"
in_out_cancel_ref_list = loadfn(cancel_ref_file)
job_ref_file = TEST_DIR / "io" / "slurm" / "parse_job_output_inout.yaml"
in_out_job_ref_list = loadfn(job_ref_file)


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

    @pytest.mark.parametrize("in_out_ref", in_out_job_ref_list)
    def test_parse_job_output(self, slurm_io, in_out_ref, test_utils):
        parse_cmd_output, job_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_job_kwargs", outkey="job_ref"
        )
        job = slurm_io.parse_job_output(**parse_cmd_output)
        assert job == job_ref
        job = slurm_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert job == job_ref
        job = slurm_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert job == job_ref

    def test_get_job_cmd(self, slurm_io):
        cmd = slurm_io._get_job_cmd(3)
        assert cmd == "SLURM_TIME_FORMAT='standard' scontrol show job -o 3"
        cmd = slurm_io._get_job_cmd("56")
        assert cmd == "SLURM_TIME_FORMAT='standard' scontrol show job -o 56"

    def test_get_jobs_list_cmd(self, slurm_io):
        with pytest.raises(
            ValueError, match=r"Cannot query by user and job\(s\) in SLURM"
        ):
            slurm_io._get_jobs_list_cmd(job_ids=["1"], user="johndoe")
        cmd = slurm_io._get_jobs_list_cmd(user="johndoe")
        assert cmd == (
            "SLURM_TIME_FORMAT='standard' "
            "squeue --noheader -o '%i<><> %t<><> %r<><> "
            "%j<><> %u<><> %P<><> %l<><> %D<><> %C<><> "
            "%M<><> %m' -u johndoe"
        )
        cmd = slurm_io._get_jobs_list_cmd(job_ids=["1", "3", "56", "15"])
        assert cmd == (
            "SLURM_TIME_FORMAT='standard' "
            "squeue --noheader -o '%i<><> %t<><> %r<><> "
            "%j<><> %u<><> %P<><> %l<><> %D<><> %C<><> "
            "%M<><> %m' --jobs=1,3,56,15"
        )
        cmd = slurm_io._get_jobs_list_cmd(job_ids=["1"])
        assert cmd == (
            "SLURM_TIME_FORMAT='standard' "
            "squeue --noheader -o '%i<><> %t<><> %r<><> "
            "%j<><> %u<><> %P<><> %l<><> %D<><> %C<><> "
            "%M<><> %m' --jobs=1,1"
        )

    def test_convert_str_to_time(self, slurm_io):
        time_seconds = slurm_io._convert_str_to_time(None)
        assert time_seconds is None
        time_seconds = slurm_io._convert_str_to_time("UNLIMITED")
        assert time_seconds is None
        time_seconds = slurm_io._convert_str_to_time("NOT_SET")
        assert time_seconds is None

        time_seconds = slurm_io._convert_str_to_time("3-10:51:13")
        assert time_seconds == 298273
        time_seconds = slurm_io._convert_str_to_time("2:10:02")
        assert time_seconds == 7802
        time_seconds = slurm_io._convert_str_to_time("10:02")
        assert time_seconds == 602
        time_seconds = slurm_io._convert_str_to_time("45")
        assert time_seconds == 2700

        with pytest.raises(OutputParsingError):
            slurm_io._convert_str_to_time("2:10:02:10")

        with pytest.raises(OutputParsingError):
            slurm_io._convert_str_to_time("2:10:a")

    def test_convert_memory_str(self, slurm_io):
        memory_kb = slurm_io._convert_memory_str(None)
        assert memory_kb is None
        memory_kb = slurm_io._convert_memory_str("")
        assert memory_kb is None

        memory_kb = slurm_io._convert_memory_str("12M")
        assert memory_kb == 12288
        memory_kb = slurm_io._convert_memory_str("13K")
        assert memory_kb == 13
        memory_kb = slurm_io._convert_memory_str("5G")
        assert memory_kb == 5242880
        memory_kb = slurm_io._convert_memory_str("1T")
        assert memory_kb == 1073741824

        with pytest.raises(OutputParsingError):
            slurm_io._convert_memory_str("aT")

    def test_convert_time_to_str(self, slurm_io):
        time_str = slurm_io._convert_time_to_str(10)
        assert time_str == "0-0:0:10"
        time_str = slurm_io._convert_time_to_str(298273)
        assert time_str == "3-10:51:13"
        time_str = slurm_io._convert_time_to_str(7802)
        assert time_str == "0-2:10:2"
        time_str = slurm_io._convert_time_to_str(602)
        assert time_str == "0-0:10:2"

        time_str = slurm_io._convert_time_to_str(timedelta(seconds=298273))
        assert time_str == "3-10:51:13"
        time_str = slurm_io._convert_time_to_str(
            timedelta(days=15, hours=21, minutes=19, seconds=32)
        )
        assert time_str == "15-21:19:32"

    def test_check_convert_qresources(self, slurm_io):
        res = QResources(
            queue_name="myqueue",
            job_name="myjob",
            memory_per_thread=2048,
            account="myaccount",
            qos="myqos",
            output_filepath="someoutputpath",
            error_filepath="someerrorpath",
            njobs=4,
            time_limit=298273,
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
            nodes=4,
            processes_per_node=3,
            threads_per_process=2,
            gpus_per_job=4,
            email_address="john.doe@submit.qtk",
            kwargs={"tata": "toto", "titi": "tutu"},
        )
        header_dict = slurm_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "partition": "myqueue",
            "job_name": "myjob",
            "mem-per-cpu": 2048,
            "account": "myaccount",
            "qos": "myqos",
            "qout_path": "someoutputpath",
            "qerr_path": "someerrorpath",
            "array": "1-4",
            "time": "3-10:51:13",
            "ntasks_per_node": 3,
            "nodes": 4,
            "cpus_per_task": 2,
            "gres": "gpu:4",
            "mail_user": "john.doe@submit.qtk",
            "mail_type": "ALL",
            "tata": "toto",
            "titi": "tutu",
        }

        res = QResources(
            time_limit=298273,
            processes=24,
        )
        header_dict = slurm_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "time": "3-10:51:13",
            "ntasks": 24,
        }

        res = QResources(
            njobs=1,
            processes=24,
            gpus_per_job=4,
        )
        header_dict = slurm_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "ntasks": 24,
            "gres": "gpu:4",
        }

        res = QResources(
            processes=5,
            rerunnable=True,
        )
        with pytest.raises(
            UnsupportedResourcesError, match=r"Keys not supported: rerunnable"
        ):
            slurm_io.check_convert_qresources(res)
