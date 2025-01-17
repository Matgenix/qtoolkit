from datetime import timedelta
from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.core.data_objects import ProcessPlacement, QResources, QState
from qtoolkit.core.exceptions import OutputParsingError, UnsupportedResourcesError
from qtoolkit.io.pbs import PBSIO, PBSState

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
submit_ref_file = TEST_DIR / "io" / "pbs" / "parse_submit_output_inout.yaml"
in_out_submit_ref_list = loadfn(submit_ref_file)
cancel_ref_file = TEST_DIR / "io" / "pbs" / "parse_cancel_output_inout.yaml"
in_out_cancel_ref_list = loadfn(cancel_ref_file)
job_ref_file = TEST_DIR / "io" / "pbs" / "parse_job_output_inout.yaml"
in_out_job_ref_list = loadfn(job_ref_file)


@pytest.fixture(scope="module")
def pbs_io():
    return PBSIO()


@pytest.fixture()  # scope="session")
def maximalist_qresources_pbs():
    """A set of QResources options that try to make use of most features"""
    from qtoolkit.core.data_objects import QResources

    return QResources(
        queue_name="test_queue",
        job_name="test_job",
        nodes=1,
        processes=1,
        processes_per_node=1,
        threads_per_process=1,
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


class TestPBSState:
    @pytest.mark.parametrize("sge_state", [s for s in PBSState])
    def test_qstate(self, sge_state):
        assert isinstance(sge_state.qstate, QState)

    def test_instance(self):
        assert PBSState("H") == PBSState.HELD
        assert PBSState("R") == PBSState.RUNNING
        assert PBSState("Q") == PBSState.QUEUED
        assert PBSState("E") == PBSState.EXITING


class TestPBSIO:
    @pytest.mark.parametrize("in_out_ref", in_out_submit_ref_list)
    def test_parse_submit_output(self, pbs_io, in_out_ref, test_utils):
        parse_cmd_output, sr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_submit_kwargs", outkey="submission_result_ref"
        )
        sr = pbs_io.parse_submit_output(**parse_cmd_output)
        print(sr, sr_ref)
        assert sr == sr_ref
        sr = pbs_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert sr == sr_ref
        sr = pbs_io.parse_submit_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert sr == sr_ref

    @pytest.mark.parametrize("in_out_ref", in_out_cancel_ref_list)
    def test_parse_cancel_output(self, pbs_io, in_out_ref, test_utils):
        parse_cmd_output, cr_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_cancel_kwargs", outkey="cancel_result_ref"
        )
        cr = pbs_io.parse_cancel_output(**parse_cmd_output)
        assert cr == cr_ref
        cr = pbs_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert cr == cr_ref
        cr = pbs_io.parse_cancel_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert cr == cr_ref

    @pytest.mark.parametrize("in_out_ref", in_out_job_ref_list)
    def test_parse_job_output(self, pbs_io, in_out_ref, test_utils):
        parse_cmd_output, job_ref = test_utils.inkwargs_outref(
            in_out_ref, inkey="parse_job_kwargs", outkey="job_ref"
        )
        if "stderr" not in parse_cmd_output:
            parse_cmd_output["stderr"] = ""
        job = pbs_io.parse_job_output(**parse_cmd_output)
        assert job == job_ref
        job = pbs_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "utf-8"),
            stderr=bytes(parse_cmd_output["stderr"], "utf-8"),
        )
        assert job == job_ref
        job = pbs_io.parse_job_output(
            exit_code=parse_cmd_output["exit_code"],
            stdout=bytes(parse_cmd_output["stdout"], "ascii"),
            stderr=bytes(parse_cmd_output["stderr"], "ascii"),
        )
        assert job == job_ref

    def test_get_job_cmd(self, pbs_io):
        cmd = pbs_io._get_job_cmd(3)
        assert cmd == "qstat -f -w 3"
        cmd = pbs_io._get_job_cmd("56")
        assert cmd == "qstat -f -w 56"

    def test_get_jobs_list_cmd(self, pbs_io):
        with pytest.raises(
            ValueError, match=r"Cannot query by user and job\(s\) in PBS"
        ):
            pbs_io._get_jobs_list_cmd(job_ids=["1"], user="johndoe")
        cmd = pbs_io._get_jobs_list_cmd(user="johndoe")
        assert cmd == "qstat -f -w -u johndoe"
        cmd = pbs_io._get_jobs_list_cmd(job_ids=["1", "3", "56", "15"])
        assert cmd == "qstat -f -w 1 3 56 15"
        cmd = pbs_io._get_jobs_list_cmd(job_ids=["1"])
        assert cmd == "qstat -f -w 1"

    def test_convert_str_to_time(self, pbs_io):
        time_seconds = pbs_io._convert_str_to_time("10:51:13")
        assert time_seconds == 39073
        time_seconds = pbs_io._convert_str_to_time("02:10:02")
        assert time_seconds == 7802
        time_seconds = pbs_io._convert_str_to_time("10:02")
        assert time_seconds == 602
        time_seconds = pbs_io._convert_str_to_time("45")
        assert time_seconds == 45

        with pytest.raises(OutputParsingError):
            pbs_io._convert_str_to_time("2:10:a")

    def test_convert_memory_str(self, pbs_io):
        assert isinstance(pbs_io, PBSIO)
        memory_kb = pbs_io._convert_memory_str(None)
        assert memory_kb is None
        memory_kb = pbs_io._convert_memory_str("")
        assert memory_kb is None

        memory_kb = pbs_io._convert_memory_str("12mb")
        assert memory_kb == 12288
        memory_kb = pbs_io._convert_memory_str("13kb")
        assert memory_kb == 13
        memory_kb = pbs_io._convert_memory_str("5gb")
        assert memory_kb == 5242880
        memory_kb = pbs_io._convert_memory_str("1tb")
        assert memory_kb == 1073741824

        with pytest.raises(OutputParsingError):
            pbs_io._convert_memory_str("aT")

    def test_convert_time_to_str(self, pbs_io):
        time_str = pbs_io._convert_time_to_str(10)
        assert time_str == "0:0:10"
        time_str = pbs_io._convert_time_to_str(39073)
        assert time_str == "10:51:13"
        time_str = pbs_io._convert_time_to_str(7802)
        assert time_str == "2:10:2"
        time_str = pbs_io._convert_time_to_str(602)
        assert time_str == "0:10:2"

        time_str = pbs_io._convert_time_to_str(timedelta(seconds=39073))
        assert time_str == "10:51:13"
        time_str = pbs_io._convert_time_to_str(
            timedelta(hours=15, minutes=19, seconds=32)
        )
        assert time_str == "15:19:32"

        # test float
        time_str = pbs_io._convert_time_to_str(602.0)
        assert time_str == "0:10:2"

        # test negative
        # negative time makes no sense and should not be passed. this test is just to be alerted
        # if the output for negative numbers changes
        time_str = pbs_io._convert_time_to_str(-10)
        assert time_str == "-1:59:50"

    def test_check_convert_qresources(self, pbs_io):
        res = QResources(
            queue_name="myqueue",
            job_name="myjob",
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
        header_dict = pbs_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "queue": "myqueue",
            "job_name": "myjob",
            "place": "scatter",
            "priority": 1,
            "qout_path": "someoutputpath",
            "qerr_path": "someerrorpath",
            "array": "1-4",
            "walltime": "10:51:13",
            "select": "select=4:ncpus=6:mpiprocs=3:ompthreads=2",
            "mail_user": "john.doe@submit.qtk",
            "mail_type": "abe",
            "tata": "toto",
            "titi": "tutu",
        }

        res = QResources(
            time_limit=39073,
            processes=24,
        )
        header_dict = pbs_io.check_convert_qresources(resources=res)
        assert header_dict == {
            "walltime": "10:51:13",
            "select": "select=24",  # also not sure about this
        }

        res = QResources(
            njobs=1,
            processes=24,
        )
        header_dict = pbs_io.check_convert_qresources(resources=res)
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
            pbs_io.check_convert_qresources(res)

    def test_submission_script(self, pbs_io, maximalist_qresources_pbs):
        # remove unsupported SGE options
        maximalist_qresources_pbs.rerunnable = None
        maximalist_qresources_pbs.project = None
        maximalist_qresources_pbs.account = None
        maximalist_qresources_pbs.qos = None
        maximalist_qresources_pbs.process_placement = (
            ProcessPlacement.EVENLY_DISTRIBUTED
        )

        # Set `processes` to None to avoid the conflict
        maximalist_qresources_pbs.processes = None

        # generate the SGE submission script
        script_qresources = pbs_io.get_submission_script(
            commands=["ls -l"], options=maximalist_qresources_pbs
        )

        # assert the correctness of the generated script
        assert (
            script_qresources.split("\n")
            == """#!/bin/bash

#PBS -q test_queue
#PBS -N test_job
#PBS -l select=1:ncpus=1:mpiprocs=1
#PBS -l walltime=0:1:40
#PBS -l place=scatter
#PBS -M test_email_address@email.address
#PBS -m abe
#PBS -o test_output_filepath
#PBS -e test_error_filepath
#PBS -p 1
ls -l""".split(
                "\n"
            )
        )

    def test_sanitize_options(self, pbs_io):
        script = pbs_io.get_submission_script(
            commands=["ls -l"], options={"job_name": "test-_@/*"}
        )
        assert "#PBS -N test-____" in script

        script = pbs_io.get_submission_script(
            commands=["ls -l"], options={"job_name": "test-_test"}
        )
        assert "#PBS -N test-_test" in script

        script = pbs_io.get_submission_script(
            commands=["ls -l"], options={"job_name": "test -_!#$test"}
        )
        assert "#PBS -N test_-____test" in script
