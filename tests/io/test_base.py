from __future__ import annotations

import pytest

from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.io.base import BaseSchedulerIO, QTemplate


def test_qtemplate():
    template_str = """This is a template
    with some $$substitutions
    another $${tata}te"""
    template = QTemplate(template_str)
    identifiers = set(template.get_identifiers())
    assert identifiers == {"substitutions", "tata"}
    substituted_template = template.safe_substitute({"substitutions": "mysubstitution"})
    assert (
        substituted_template
        == """This is a template
    with some mysubstitution
    another $${tata}te"""
    )
    substituted_template = template.safe_substitute({})
    assert (
        substituted_template
        == """This is a template
    with some $$substitutions
    another $${tata}te"""
    )
    substituted_template = template.safe_substitute({"tata": "pata"})
    assert (
        substituted_template
        == """This is a template
    with some $$substitutions
    another patate"""
    )

    template_str = """Multi template $$subst
    $${subst1}$${subst2}
    $${subst3}$${subst3}"""
    template = QTemplate(template_str)
    identifiers = template.get_identifiers()
    assert len(identifiers) == 4
    assert set(identifiers) == {"subst", "subst1", "subst2", "subst3"}
    substituted_template = template.safe_substitute({"subst3": "to", "subst": "bla"})
    assert (
        substituted_template
        == """Multi template bla
    $${subst1}$${subst2}
    toto"""
    )


class TestBaseScheduler:
    @pytest.fixture(scope="module")
    def scheduler(self):
        class MyScheduler(BaseSchedulerIO):
            header_template = """#SPECCMD --option1=$${option1}
#SPECCMD --option2=$${option2}
#SPECCMD --option3=$${option3}
#SPECCMD --processes=$${processes}
#SPECCMD --processes_per_node=$${processes_per_node}
#SPECCMD --nodes=$${nodes}"""

            SUBMIT_CMD = "mysubmit"
            CANCEL_CMD = "mycancel"

            def parse_submit_output(
                self, exit_code, stdout, stderr
            ) -> SubmissionResult:
                pass

            def parse_cancel_output(self, exit_code, stdout, stderr) -> CancelResult:
                pass

            def _get_job_cmd(self, job_id: str) -> str:
                pass

            def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
                pass

            def _convert_qresources(self, resources: QResources) -> dict:
                header_dict = {}

                (
                    nodes,
                    processes,
                    processes_per_node,
                ) = resources.get_processes_distribution()
                if processes:
                    header_dict["processes"] = processes
                if processes_per_node:
                    header_dict["processes_per_node"] = processes_per_node
                if nodes:
                    header_dict["nodes"] = nodes

                if resources.scheduler_kwargs:
                    header_dict.update(resources.scheduler_kwargs)

                return header_dict

            @property
            def supported_qresources_keys(self) -> list:
                return [
                    "scheduler_kwargs",
                    "nodes",
                    "processes_per_node",
                    "process_placement",
                    "processes",
                ]

            def _get_jobs_list_cmd(
                self, job_ids: list[str] | None = None, user: str | None = None
            ) -> str:
                pass

            def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
                pass

        return MyScheduler()

    def test_subclass_base_scheduler(self, scheduler):
        class MyScheduler(BaseSchedulerIO):
            pass

        with pytest.raises(TypeError):
            MyScheduler()

    def test_generate_header(self, scheduler):
        header = scheduler.generate_header({"option2": "value_option2"})
        assert header == """#SPECCMD --option2=value_option2"""

        res = QResources(processes=8)
        header = scheduler.generate_header(res)
        assert header == """#SPECCMD --processes=8"""
        res = QResources(
            nodes=4, processes_per_node=16, scheduler_kwargs={"option2": "myopt2"}
        )
        header = scheduler.generate_header(res)
        assert (
            header
            == """#SPECCMD --option2=myopt2
#SPECCMD --processes_per_node=16
#SPECCMD --nodes=4"""
        )

        # check that the error message contains the expected error, but should not match
        # the possible replacements, as they are too different
        with pytest.raises(
            ValueError,
            match=r"The following keys are not present in the template: tata, titi. Check "
            r"the template in .*MyScheduler.header_template(?!.*instead of 'titi')",
        ):
            res = QResources(
                nodes=4,
                processes_per_node=16,
                scheduler_kwargs={"tata": "tata", "titi": "titi"},
            )
            scheduler.generate_header(res)

        with pytest.raises(
            ValueError,
            match=r"The following keys are not present in the template: option32, processes-per-node. "
            r"Check the template in .*MyScheduler.header_template.*'option3' or 'option2' or 'option1' "
            r"instead of 'option32'. 'processes_per_node' or 'processes' instead of 'processes-per-node'",
        ):
            res = QResources(
                nodes=4,
                processes_per_node=16,
                scheduler_kwargs={"option32": "xxx", "processes-per-node": "yyy"},
            )
            scheduler.generate_header(res)

    def test_generate_ids_list(self, scheduler):
        ids_list = scheduler.generate_ids_list(
            [QJob(job_id=4), QJob(job_id="job_id_abc1"), 215, "job12345"]
        )
        assert ids_list == ["4", "job_id_abc1", "215", "job12345"]

    def test_get_submit_cmd(self, scheduler):
        submit_cmd = scheduler.get_submit_cmd()
        assert submit_cmd == "mysubmit submit.script"
        submit_cmd = scheduler.get_submit_cmd(script_file="sub.sh")
        assert submit_cmd == "mysubmit sub.sh"

    def test_get_cancel_cmd(self, scheduler):
        cancel_cmd = scheduler.get_cancel_cmd(QJob(job_id=5))
        assert cancel_cmd == "mycancel 5"
        cancel_cmd = scheduler.get_cancel_cmd(QJob(job_id="abc1"))
        assert cancel_cmd == "mycancel abc1"
        cancel_cmd = scheduler.get_cancel_cmd("jobid2")
        assert cancel_cmd == "mycancel jobid2"
        cancel_cmd = scheduler.get_cancel_cmd(632)
        assert cancel_cmd == "mycancel 632"

        with pytest.raises(
            ValueError,
            match=r"The id of the job to be cancelled should be defined. "
            r"Received: None",
        ):
            scheduler.get_cancel_cmd(job=None)

        with pytest.raises(
            ValueError,
            match=r"The id of the job to be cancelled should be defined. "
            r"Received: '' \(empty string\)",
        ):
            scheduler.get_cancel_cmd(job="")
