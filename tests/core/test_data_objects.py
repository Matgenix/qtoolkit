"""Unit tests for the core.data_objects module of QToolKit."""
import pytest

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    ProcessPlacement,
    QResources,
    QState,
    QSubState,
    SubmissionResult,
    SubmissionStatus,
)
from qtoolkit.core.exceptions import UnsupportedResourcesError

try:
    import monty
except ModuleNotFoundError:
    monty = None


class TestSubmissionStatus:
    def test_status_list(self):
        all_status = [status.value for status in SubmissionStatus]
        assert set(all_status) == {"SUCCESSFUL", "FAILED", "JOB_ID_UNKNOWN"}

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        status = SubmissionStatus("SUCCESSFUL")
        assert test_utils.is_msonable(status)

    def test_equal(self):
        assert SubmissionStatus("SUCCESSFUL") == SubmissionStatus.SUCCESSFUL


class TestSubmissionResult:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        sr = SubmissionResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=SubmissionStatus.FAILED,
        )
        assert test_utils.is_msonable(sr)

    def test_equality(self):
        sr1 = SubmissionResult(
            job_id=None,
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=SubmissionStatus.JOB_ID_UNKNOWN,
        )
        sr2 = SubmissionResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=SubmissionStatus.FAILED,
        )
        sr3 = SubmissionResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=SubmissionStatus.FAILED,
        )
        assert sr2 == sr3
        assert sr1 != sr2
        assert sr1 != sr3


class TestCancelStatus:
    def test_status_list(self):
        all_status = [status.value for status in CancelStatus]
        assert set(all_status) == {"SUCCESSFUL", "FAILED", "JOB_ID_UNKNOWN"}

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        status = CancelStatus("FAILED")
        assert test_utils.is_msonable(status)

    def test_equal(self):
        assert CancelStatus("JOB_ID_UNKNOWN") == CancelStatus.JOB_ID_UNKNOWN
        assert CancelStatus("JOB_ID_UNKNOWN") != SubmissionStatus.JOB_ID_UNKNOWN


class TestCancelResult:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        cr = CancelResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=CancelStatus.FAILED,
        )
        assert test_utils.is_msonable(cr)

    def test_equality(self):
        cr1 = CancelResult(
            job_id="abc123",
            step_id=1,
            exit_code=0,
            stdout="mystdout",
            stderr="mystderr",
            status=CancelStatus.SUCCESSFUL,
        )
        cr2 = CancelResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=CancelStatus.FAILED,
        )
        cr3 = CancelResult(
            job_id="abc123",
            step_id=1,
            exit_code=1,
            stdout="mystdout",
            stderr="mystderr",
            status=CancelStatus.FAILED,
        )
        assert cr2 == cr3
        assert cr1 != cr2
        assert cr1 != cr3


class TestQState:
    def test_states_list(self):
        all_states = [state.value for state in QState]
        assert set(all_states) == {
            "UNDETERMINED",
            "QUEUED",
            "QUEUED_HELD",
            "RUNNING",
            "SUSPENDED",
            "REQUEUED",
            "REQUEUED_HELD",
            "DONE",
            "FAILED",
        }

    def test_equality(self):
        state1 = QState("QUEUED")
        state2 = QState("RUNNING")
        state3 = QState.RUNNING
        assert state1 != state2
        assert state1 != state3
        assert state2 == state3

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        state = QState.DONE
        assert test_utils.is_msonable(state)


class SomeSubState(QSubState):
    STATE_A = "stateA", "STA"
    STATE_B = "stateB", "STB", "sb"
    STATE_C = "sc"

    def qstate(self) -> QState:
        return QState.DONE


class TestQSubState:
    def test_equality(self):
        sst1 = SomeSubState.STATE_B
        sst2 = SomeSubState("stateB")
        sst3 = SomeSubState("STB")
        sst4 = SomeSubState("sb")
        assert sst1 == sst2
        assert sst1 == sst3
        assert sst1 == sst4

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        sst1 = SomeSubState.STATE_A
        assert test_utils.is_msonable(sst1)
        sst2 = SomeSubState("sb")
        assert test_utils.is_msonable(sst2)

    def test_repr(self):
        sst1 = SomeSubState.STATE_B
        sst2 = SomeSubState("stateB")
        sst3 = SomeSubState("STB")
        sst4 = SomeSubState("sb")
        assert repr(sst1) == "<SomeSubState.STATE_B: 'stateB', 'STB', 'sb'>"
        assert repr(sst1) == repr(sst2)
        assert repr(sst1) == repr(sst3)
        assert repr(sst1) == repr(sst4)


class TestProcessPlacement:
    def test_process_placement_list(self):
        all_process_placements = [
            process_placement.value for process_placement in ProcessPlacement
        ]
        assert set(all_process_placements) == {
            "NO_CONSTRAINTS",
            "SCATTERED",
            "SAME_NODE",
            "EVENLY_DISTRIBUTED",
        }

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        pp1 = ProcessPlacement.SCATTERED
        assert test_utils.is_msonable(pp1)
        pp2 = ProcessPlacement("SAME_NODE")
        assert test_utils.is_msonable(pp2)


class TestQResources:
    def test_no_process_placement(self):
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"When process_placement is None either define only nodes "
            r"plus processes_per_node or only processes",
        ):
            QResources(processes=8, nodes=2)

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"When process_placement is None either define only nodes "
            r"plus processes_per_node or only processes",
        ):
            QResources(processes=8, processes_per_node=2)

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        qr1 = QResources(
            queue_name="main",
            job_name="myjob",
            memory_per_thread=1024,
            processes=16,
            time_limit=86400,
        )
        assert test_utils.is_msonable(qr1)
        qr2 = QResources.evenly_distributed(nodes=4, processes_per_node=8)
        assert test_utils.is_msonable(qr2)

    def test_no_constraints(self):
        qr = QResources.no_constraints(processes=16, queue_name="main")
        assert qr.queue_name == "main"
        assert qr.process_placement == ProcessPlacement.NO_CONSTRAINTS

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with no constraints jobs",
        ):
            QResources.no_constraints(processes=16, nodes=4)

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with no constraints jobs",
        ):
            QResources.no_constraints(processes=16, processes_per_node=2)

    def test_evenly_distributed(self):
        qr = QResources.evenly_distributed(
            nodes=4, processes_per_node=2, queue_name="main"
        )
        assert qr.queue_name == "main"
        assert qr.process_placement == ProcessPlacement.EVENLY_DISTRIBUTED

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"processes is incompatible with evenly distributed jobs",
        ):
            QResources.evenly_distributed(nodes=4, processes_per_node=2, processes=12)

    def test_scattered(self):
        qr = QResources.scattered(processes=16, queue_name="main")
        assert qr.queue_name == "main"
        assert qr.process_placement == ProcessPlacement.SCATTERED

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with scattered jobs",
        ):
            QResources.scattered(processes=16, nodes=4)

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with scattered jobs",
        ):
            QResources.scattered(processes=16, processes_per_node=4)

    def test_same_node(self):
        qr = QResources.same_node(processes=16, queue_name="main")
        assert qr.queue_name == "main"
        assert qr.process_placement == ProcessPlacement.SAME_NODE

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with same node jobs",
        ):
            QResources.same_node(processes=16, nodes=4)

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"nodes and processes_per_node are incompatible with same node jobs",
        ):
            QResources.same_node(processes=16, processes_per_node=4)

    def test_equality(self):
        qr1 = QResources.evenly_distributed(
            nodes=4, processes_per_node=4, job_name="myjob"
        )
        qr2 = QResources(
            nodes=4,
            processes_per_node=4,
            job_name="myjob",
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
        )
        qr3 = QResources(nodes=4, processes_per_node=4, job_name="myjob")
        qr4 = QResources(
            nodes=4,
            processes_per_node=4,
            job_name="myjob",
            process_placement=ProcessPlacement.SAME_NODE,
        )
        assert qr1 == qr2
        assert qr1 == qr3
        assert qr1 != qr4
