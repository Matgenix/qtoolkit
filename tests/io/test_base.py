from __future__ import annotations

from pathlib import Path

import pytest
from monty.serialization import loadfn

from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.io.base import BaseSchedulerIO, QTemplate

TEST_DIR = Path(__file__).resolve().parents[1] / "test_data"
ref_file = TEST_DIR / "io" / "slurm" / "parse_submit_cmd_inout.yaml"
in_out_ref_list = loadfn(ref_file)


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


def test_base_scheduler():
    class MyScheduler(BaseSchedulerIO):
        pass

    with pytest.raises(TypeError):
        MyScheduler()

    class MyScheduler(BaseSchedulerIO):
        header_template = """#SPECCMD --option1=$${option1}
#SPECCMD --option2=$${option2}
#SPECCMD --option3=$${option3}"""

        SUBMIT_CMD = "mysubmit"
        CANCEL_CMD = "mycancel"

        def parse_submit_output(self, exit_code, stdout, stderr) -> SubmissionResult:
            pass

        def parse_cancel_output(self, exit_code, stdout, stderr) -> CancelResult:
            pass

        def _get_job_cmd(self, job_id: str) -> str:
            pass

        def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
            pass

        def _convert_qresources(self, resources: QResources) -> dict:
            pass

        @property
        def supported_qresources_keys(self) -> list:
            return []

        def _get_jobs_list_cmd(
            self, job_ids: list[str] | None = None, user: str | None = None
        ) -> str:
            pass

        def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
            pass

    scheduler = MyScheduler()

    header = scheduler.generate_header({"option2": "value_option2"})
    assert header == """#SPECCMD --option2=value_option2"""
    scheduler.parse_submit_output(0, "", "")