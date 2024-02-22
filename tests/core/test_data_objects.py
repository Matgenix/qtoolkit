"""Unit tests for the core.data_objects module of QToolKit."""
import pytest

from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    ProcessPlacement,
    QJob,
    QJobInfo,
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

        with pytest.raises(
            UnsupportedResourcesError,
            match=r"When process_placement is None either define only nodes "
            r"plus processes_per_node or only processes to get a default value. "
            r"Otherwise all the fields must be empty.",
        ):
            QResources(project="xxx")

        # This is acceptable for empty process placement and no details passed
        assert QResources()

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        qr1 = QResources(
            queue_name="main",
            job_name="myjob",
            memory_per_thread=1024,
            processes=16,
            time_limit=86400,
            scheduler_kwargs={"a": "b"},
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

    def test_get_processes_distribution(self):
        qr = QResources(nodes=4, processes_per_node=2)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [4, None, 2]
        qr = QResources(processes=18)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [None, 18, None]
        qr = QResources.scattered(processes=12)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [12, 12, 1]
        qr = QResources.same_node(processes=12)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 12, 12]
        qr = QResources.evenly_distributed(nodes=4, processes_per_node=8)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [4, None, 8]
        qr = QResources.no_constraints(processes=14)
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [None, 14, None]
        qr = QResources(
            process_placement=ProcessPlacement.SCATTERED, nodes=None, processes=4
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [4, 4, 1]
        qr = QResources(
            process_placement=ProcessPlacement.SCATTERED, nodes=3, processes=None
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [3, 3, 1]
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.SCATTERED is incompatible "
            r"with different values of nodes and processes",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.SCATTERED, nodes=3, processes=4
            )
            qr.get_processes_distribution()
        qr = QResources(
            process_placement=ProcessPlacement.SCATTERED, nodes=None, processes=None
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 1, 1]
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.SCATTERED is incompatible "
            r"with 2 processes_per_node",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.SCATTERED,
                nodes=4,
                processes=4,
                processes_per_node=2,
            )
            qr.get_processes_distribution()
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.SAME_NODE is incompatible " r"with 4 nodes",
        ):
            qr = QResources(process_placement=ProcessPlacement.SAME_NODE, nodes=4)
            qr.get_processes_distribution()
        qr = QResources(
            process_placement=ProcessPlacement.SAME_NODE,
            nodes=None,
            processes=None,
            processes_per_node=4,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 4, 4]
        qr = QResources(
            process_placement=ProcessPlacement.SAME_NODE,
            nodes=1,
            processes=6,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 6, 6]
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.SAME_NODE is incompatible with "
            r"different values of nodes and processes",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.SAME_NODE,
                nodes=1,
                processes=2,
                processes_per_node=6,
            )
            qr.get_processes_distribution()
        qr = QResources(
            process_placement=ProcessPlacement.SAME_NODE,
            nodes=1,
            processes=None,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 1, 1]
        qr = QResources(
            process_placement=ProcessPlacement.SAME_NODE,
            nodes=None,
            processes=None,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 1, 1]
        qr = QResources(
            process_placement=ProcessPlacement.SAME_NODE,
            nodes=None,
            processes=3,
            processes_per_node=3,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, 3, 3]
        qr = QResources(
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
            nodes=None,
            processes=None,
            processes_per_node=3,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [1, None, 3]
        qr = QResources(
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
            nodes=4,
            processes=None,
            processes_per_node=3,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [4, None, 3]
        qr = QResources(
            process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
            nodes=4,
            processes=None,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [4, None, 1]
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.EVENLY_DISTRIBUTED "
            r"is incompatible with processes attribute",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.EVENLY_DISTRIBUTED,
                nodes=1,
                processes=2,
                processes_per_node=6,
            )
            qr.get_processes_distribution()
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.NO_CONSTRAINTS is incompatible "
            r"with processes_per_node and nodes attribute",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.NO_CONSTRAINTS,
                nodes=1,
                processes=2,
                processes_per_node=None,
            )
            qr.get_processes_distribution()
        with pytest.raises(
            UnsupportedResourcesError,
            match=r"ProcessPlacement.NO_CONSTRAINTS is incompatible "
            r"with processes_per_node and nodes attribute",
        ):
            qr = QResources(
                process_placement=ProcessPlacement.NO_CONSTRAINTS,
                nodes=None,
                processes=2,
                processes_per_node=2,
            )
            qr.get_processes_distribution()
        qr = QResources(
            process_placement=ProcessPlacement.NO_CONSTRAINTS,
            nodes=None,
            processes=None,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [None, 1, None]
        qr = QResources(
            process_placement=ProcessPlacement.NO_CONSTRAINTS,
            nodes=None,
            processes=8,
            processes_per_node=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [None, 8, None]
        qr = QResources(
            process_placement="No placement",
            nodes="a",
            processes="b",
            processes_per_node="c",
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == ["a", "b", "c"]
        qr = QResources(
            process_placement=None,
        )
        proc_distr = qr.get_processes_distribution()
        assert proc_distr == [None, None, None]

    def test_is_empty(self):
        qr = QResources()
        assert qr.check_empty()

        qr = QResources(process_placement=ProcessPlacement.NO_CONSTRAINTS, processes=10)
        assert not qr.check_empty()

        qr = QResources(process_placement=None)
        qr.processes = 10
        with pytest.raises(
            ValueError, match="process_placement is None, but some values are set"
        ):
            qr.check_empty()


class TestQJobInfo:
    def test_equality(self):
        qji1 = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qji2 = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qji3 = QJobInfo(
            memory=4000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        assert qji1 == qji2
        assert qji1 != qji3

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        qji = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        assert test_utils.is_msonable(qji)


class TestQJob:
    def test_equality(self):
        qji1 = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qji2 = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qji3 = QJobInfo(
            memory=4000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qjob1 = QJob(
            name="job1",
            job_id="id1",
            exit_status=0,
            state=QState.DONE,
            sub_state=SomeSubState.STATE_A,
            info=qji1,
            account="myacc",
            runtime=2541,
            queue_name="mymain",
        )
        qjob2 = QJob(
            name="job1",
            job_id="id1",
            exit_status=0,
            state=QState.DONE,
            sub_state=SomeSubState.STATE_A,
            info=qji2,
            account="myacc",
            runtime=2541,
            queue_name="mymain",
        )
        qjob3 = QJob(
            name="job1",
            job_id="id1",
            exit_status=0,
            state=QState.DONE,
            sub_state=SomeSubState.STATE_A,
            info=qji3,
            account="myacc",
            runtime=2541,
            queue_name="mymain",
        )
        assert qjob1 == qjob2
        assert qjob1 != qjob3

    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        qji = QJobInfo(
            memory=2000,
            memory_per_cpu=500,
            nodes=2,
            cpus=4,
            threads_per_process=2,
            time_limit=3600,
        )
        qjob = QJob(
            name="job1",
            job_id="id1",
            exit_status=0,
            state=QState.DONE,
            sub_state=SomeSubState.STATE_A,
            info=qji,
            account="myacc",
            runtime=2541,
            queue_name="mymain",
        )
        assert test_utils.is_msonable(qjob)
