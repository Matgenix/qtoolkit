from datetime import timedelta
from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.core.data_objects import ProcessPlacement, QResources, QState
from qtoolkit.core.exceptions import OutputParsingError, UnsupportedResourcesError
from qtoolkit.io.sge import SGEIO, SGEState

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
submit_ref_file = TEST_DIR / "io" / "sge" / "parse_submit_output_inout.yaml"
in_out_submit_ref_list = loadfn(submit_ref_file)
cancel_ref_file = TEST_DIR / "io" / "sge" / "parse_cancel_output_inout.yaml"
in_out_cancel_ref_list = loadfn(cancel_ref_file)
job_ref_file = TEST_DIR / "io" / "sge" / "parse_job_output_inout.yaml"
in_out_job_ref_list = loadfn(job_ref_file)


@pytest.fixture(scope="module")
def sge_io():
    return SGEIO()


class TestSGEState:
    @pytest.mark.parametrize("sge_state", [s for s in SGEState])
    def test_qstate(self, sge_state):
        assert isinstance(sge_state.qstate, QState)
        assert SGEState("hqw") == SGEState.HOLD
        assert SGEState("r") == SGEState.RUNNING
        assert SGEState("Eqw") == SGEState.ERROR_PENDING
        assert SGEState("dr") == SGEState.DELETION_RUNNING


class TestSGEIO:
    @pytest.mark.parametrize("in_out_ref", in_out_submit_ref_list)
    def test_parse_submit_output(self, sge_io, in_out_ref, test_utils):
        parse_cmd_output, sr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_submit_kwargs", outkey="submission_result_ref"
        )
        sr = sge_io.parse_submit_output(**parse_cmd_output)
        assert sr == sr_ref
        sr = sge_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert sr == sr_ref
        sr = sge_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert sr == sr_ref

    @pytest.mark.parametrize("in_out_ref", in_out_cancel_ref_list)
    def test_parse_cancel_output(self, sge_io, in_out_ref, test_utils):
        parse_cmd_output, cr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_cancel_kwargs", outkey="cancel_result_ref"
        )
        cr = sge_io.parse_cancel_output(**parse_cmd_output)
        assert cr == cr_ref
        cr = sge_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert cr == cr_ref
        cr = sge_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert cr == cr_ref

    @pytest.mark.parametrize("in_out_ref", in_out_job_ref_list)
    def test_parse_job_output(self, sge_io, in_out_ref, test_utils):
        parse_cmd_output, job_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_job_kwargs", outkey="job_ref"
        )
        if "stderr" not in parse_cmd_output:
            parse_cmd_output["stderr"] = ""
        job = sge_io.parse_job_output(**parse_cmd_output)
        assert job == job_ref
        job = sge_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert job == job_ref
        job = sge_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert job == job_ref

    def test_get_job_cmd(self, sge_io):
        cmd = sge_io._get_job_cmd(3)
        assert cmd == "qstat -j 3"
        cmd = sge_io._get_job_cmd("56")
        assert cmd == "qstat -j 56"

    def test_get_jobs_list_cmd(self, sge_io):
        with pytest.raises(
            UnsupportedResourcesError, match=r"Cannot query by job id in SGE"
        ):
            sge_io._get_jobs_list_cmd(job_ids=["1"], user="johndoe")
        cmd = sge_io._get_jobs_list_cmd(user="johndoe")
        assert cmd == ("qstat -ext -urg -xml -u johndoe")
        with pytest.raises(
            UnsupportedResourcesError, match=r"Cannot query by job id in SGE"
        ):
            sge_io._get_jobs_list_cmd(job_ids=["1", "3", "56", "15"])
        with pytest.raises(
            UnsupportedResourcesError, match=r"Cannot query by job id in SGE"
        ):
            sge_io._get_jobs_list_cmd(job_ids=["1"])

    def test_convert_str_to_time(self, sge_io):
        time_seconds = sge_io._convert_str_to_time("10:51:13")
        assert time_seconds == 39073
        time_seconds = sge_io._convert_str_to_time("02:10:02")
        assert time_seconds == 7802
        time_seconds = sge_io._convert_str_to_time("10:02")
        assert time_seconds == 602
        time_seconds = sge_io._convert_str_to_time("45")
        assert time_seconds == 45

        with pytest.raises(OutputParsingError):
            sge_io._convert_str_to_time("2:10:02:10")

        with pytest.raises(OutputParsingError):
            sge_io._convert_str_to_time("2:10:a")

    def test_convert_memory_str(self, sge_io):
        memory_kb = sge_io._convert_memory_str(None)
        assert memory_kb is None
        memory_kb = sge_io._convert_memory_str("")
        assert memory_kb is None

        memory_kb = sge_io._convert_memory_str("12M")
        assert memory_kb == 12288
        memory_kb = sge_io._convert_memory_str("13K")
        assert memory_kb == 13
        memory_kb = sge_io._convert_memory_str("5G")
        assert memory_kb == 5242880
        memory_kb = sge_io._convert_memory_str("1T")
        assert memory_kb == 1073741824

        with pytest.raises(OutputParsingError):
            sge_io._convert_memory_str("aT")

    def test_convert_time_to_str(self, sge_io):
        time_str = sge_io._convert_time_to_str(10)
        assert time_str == "0:0:10"
        time_str = sge_io._convert_time_to_str(39073)
        assert time_str == "10:51:13"
        time_str = sge_io._convert_time_to_str(7802)
        assert time_str == "2:10:2"
        time_str = sge_io._convert_time_to_str(602)
        assert time_str == "0:10:2"

        time_str = sge_io._convert_time_to_str(timedelta(seconds=39073))
        assert time_str == "10:51:13"
        time_str = sge_io._convert_time_to_str(
            timedelta(hours=15, minutes=19, seconds=32)
        )
        assert time_str == "15:19:32"

        # test float
        time_str = sge_io._convert_time_to_str(602.0)
        assert time_str == "0:10:2"

        # test negative
        # negative time makes no sense and should not be passed. this test is just to be alerted
        # if the output for negative numbers changes
        time_str = sge_io._convert_time_to_str(-10)
        assert time_str == "-1:59:50"

    def test_check_convert_qresources(self, sge_io):
        res = QResources(
            queue_name="myqueue",
            job_name="myjob",
            memory_per_thread=2048,
            account="myaccount",
            priority=1,
            output_filepath="someoutputpath",
            error_filepath="someerrorpath",
            njobs=4,
            time_limit=39073,
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
            nodes=4,
            processes_per_node=3,
            threads_per_process=2,
            email_address="john.doe@submit.qtk",
            scheduler_kwargs={"tata": "toto", "titi": "tutu"},
        )
        header_dict = sge_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "queue": "myqueue",
            "job_name": "myjob",
            "place": "scatter",  # a bit unsure about this
            "account": "myaccount",
            "priority": 1,
            "qout_path": "someoutputpath",
            "qerr_path": "someerrorpath",
            "array": "1-4",
            "walltime": "10:51:13",
            "select": "select=4:ncpus=6:mpiprocs=3:ompthreads=2:mem=12288mb",
            "soft_walltime": "9:46:5",
            "mail_user": "john.doe@submit.qtk",
            "mail_type": "abe",
            "tata": "toto",
            "titi": "tutu",
        }

        res = QResources(
            time_limit=39073,
            processes=24,
        )
        header_dict = sge_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "walltime": "10:51:13",
            "soft_walltime": "9:46:5",
            "select": "select=24",  # also not sure about this
        }

        res = QResources(
            njobs=1,
            processes=24,
            gpus_per_job=4,
        )
        header_dict = sge_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "select": "select=24",
        }

        res = QResources(
            processes=5,
            rerunnable=True,
        )
        with pytest.raises(
            UnsupportedResourcesError, match=r"Keys not supported: rerunnable"
        ):
            sge_io.check_convert_qresources(res)
