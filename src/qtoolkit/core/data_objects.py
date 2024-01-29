from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path

from qtoolkit.core.base import QTKEnum, QTKObject
from qtoolkit.core.exceptions import UnsupportedResourcesError


class SubmissionStatus(QTKEnum):
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    JOB_ID_UNKNOWN = "JOB_ID_UNKNOWN"


@dataclass
class OperationResult(QTKObject):
    job_id: int | str | None = None
    """Job ID of the submitted job."""

    step_id: int | None = None
    """Step ID of the submitted job."""

    exit_code: int | None = None
    """Exit code of the submitted job."""

    stdout: str | None = None
    """Standard output of the submitted job."""

    stderr: str | None = None
    """Standard error of the submitted job."""


@dataclass
class SubmissionResult(OperationResult):
    status: SubmissionStatus | None = None
    """Status of the submission."""


@dataclass
class CancelResult(OperationResult):
    status: CancelStatus | None = None
    """Status of the cancellation."""


class CancelStatus(QTKEnum):
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    JOB_ID_UNKNOWN = "JOB_ID_UNKNOWN"


class QState(QTKEnum):
    """Enumeration of possible ("standardized") job states.

    These "standardized" states are based on the drmaa specification.
    A mapping between the actual job states in a given
    queue manager (e.g. PBS, SLURM, ...) needs to be
    defined.

    Note that not all these standardized states are available in the
    actual queue manager implementations.
    """

    UNDETERMINED = "UNDETERMINED"
    QUEUED = "QUEUED"
    QUEUED_HELD = "QUEUED_HELD"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    REQUEUED = "REQUEUED"
    REQUEUED_HELD = "REQUEUED_HELD"
    DONE = "DONE"
    FAILED = "FAILED"


class QSubState(QTKEnum):
    """QSubState class defined without any enum values so it can be subclassed.

    These sub-states should be the actual job states in a given queuing system
    (e.g. PBS, SLURM, ...). This class is also extended to support multiple
    values for the same key.
    """

    def __new__(cls, *values):
        obj = object.__new__(cls)
        obj._value_ = values[0]
        for other_value in values[1:]:
            cls._value2member_map_[other_value] = obj
        obj._all_values = values
        return obj

    def __repr__(self):
        return "<{}.{}: {}>".format(
            self.__class__.__name__,
            self._name_,
            ", ".join([repr(v) for v in self._all_values]),
        )

    @property
    @abc.abstractmethod
    def qstate(self) -> QState:
        raise NotImplementedError  # pragma: no cover


class ProcessPlacement(QTKEnum):
    NO_CONSTRAINTS = "NO_CONSTRAINTS"
    SCATTERED = "SCATTERED"
    SAME_NODE = "SAME_NODE"
    EVENLY_DISTRIBUTED = "EVENLY_DISTRIBUTED"


@dataclass
class QResources(QTKObject):
    """Data defining resources for a given job (submitted or to be submitted)."""

    queue_name: str | None = None
    """Name of the queue to submit the job to."""

    job_name: str | None = None
    """Name of the job."""

    memory_per_thread: int | None = None
    """Memory per thread in MB."""

    nodes: int | None = None
    """Number of nodes."""

    processes: int | None = None
    """Number of processes."""

    processes_per_node: int | None = None
    """Number of processes per node."""

    threads_per_process: int | None = None
    """Number of threads per process."""

    gpus_per_job: int | None = None
    """Number of GPUs per job."""

    time_limit: int | None = None
    """Time limit for the job. In seconds"""

    account: str | None = None
    """Account to charge the job to."""

    qos: str | None = None
    """Quality of service."""

    priority: int | str | None = None
    """Priority of the job."""

    output_filepath: str | Path | None = None
    """Filepath for the standard output."""

    error_filepath: str | Path | None = None
    """Filepath for the standard error."""

    process_placement: ProcessPlacement | None = None
    """Process placement."""

    email_address: str | None = None
    """Email address to send notifications to."""

    rerunnable: bool | None = None
    """Whether the job is rerunnable."""

    project: str | None = None
    """Project to charge the job to."""

    njobs: int | None = None
    """Number of jobs in a job array."""

    scheduler_kwargs: dict | None = None
    """Additional keyword arguments to be passed to the scheduler IO."""

    def __post_init__(self):
        if self.process_placement is None:
            if self.processes and not self.processes_per_node and not self.nodes:
                self.process_placement = ProcessPlacement.NO_CONSTRAINTS  # type: ignore # due to QTKEnum
            elif self.nodes and self.processes_per_node and not self.processes:
                self.process_placement = ProcessPlacement.EVENLY_DISTRIBUTED
            else:
                msg = (
                    "When process_placement is None either define only nodes "
                    "plus processes_per_node or only processes"
                )
                raise UnsupportedResourcesError(msg)
        self.scheduler_kwargs = self.scheduler_kwargs or {}

    @classmethod
    def no_constraints(cls, processes, **kwargs):
        if "nodes" in kwargs or "processes_per_node" in kwargs:
            msg = (
                "nodes and processes_per_node are incompatible with no constraints jobs"
            )
            raise UnsupportedResourcesError(msg)
        kwargs["process_placement"] = ProcessPlacement.NO_CONSTRAINTS
        return cls(processes=processes, **kwargs)

    @classmethod
    def evenly_distributed(cls, nodes, processes_per_node, **kwargs):
        if "processes" in kwargs:
            msg = "processes is incompatible with evenly distributed jobs"
            raise UnsupportedResourcesError(msg)
        kwargs["process_placement"] = ProcessPlacement.EVENLY_DISTRIBUTED
        return cls(nodes=nodes, processes_per_node=processes_per_node, **kwargs)

    @classmethod
    def scattered(cls, processes, **kwargs):
        if "nodes" in kwargs or "processes_per_node" in kwargs:
            msg = "nodes and processes_per_node are incompatible with scattered jobs"
            raise UnsupportedResourcesError(msg)
        kwargs["process_placement"] = ProcessPlacement.SCATTERED
        return cls(processes=processes, **kwargs)

    @classmethod
    def same_node(cls, processes, **kwargs):
        if "nodes" in kwargs or "processes_per_node" in kwargs:
            msg = "nodes and processes_per_node are incompatible with same node jobs"
            raise UnsupportedResourcesError(msg)
        kwargs["process_placement"] = ProcessPlacement.SAME_NODE
        return cls(processes=processes, **kwargs)

    def get_processes_distribution(self) -> list:
        # TODO consider moving this to the __post_init__
        nodes = self.nodes
        processes = self.processes
        processes_per_node = self.processes_per_node
        if self.process_placement == ProcessPlacement.SCATTERED:
            if nodes is None:
                nodes = processes
            elif processes is None:
                processes = nodes
            elif nodes != processes:
                msg = "ProcessPlacement.SCATTERED is incompatible with different values of nodes and processes"
                raise UnsupportedResourcesError(msg)
            if not nodes and not processes:
                nodes = processes = 1

            if processes_per_node not in (None, 1):
                msg = f"ProcessPlacement.SCATTERED is incompatible with {self.processes_per_node} processes_per_node"
                raise UnsupportedResourcesError(msg)
            processes_per_node = 1
        elif self.process_placement == ProcessPlacement.SAME_NODE:
            if nodes not in (None, 1):
                msg = f"ProcessPlacement.SAME_NODE is incompatible with {self.nodes} nodes"
                raise UnsupportedResourcesError(msg)
            nodes = 1
            if processes is None:
                processes = processes_per_node
            elif processes_per_node is None:
                processes_per_node = processes
            elif processes_per_node != processes:
                msg = "ProcessPlacement.SAME_NODE is incompatible with different values of nodes and processes"
                raise UnsupportedResourcesError(msg)
            if not processes_per_node and not processes:
                processes_per_node = processes = 1
        elif self.process_placement == ProcessPlacement.EVENLY_DISTRIBUTED:
            if nodes is None:
                nodes = 1
            if processes:
                msg = "ProcessPlacement.EVENLY_DISTRIBUTED is incompatible with processes attribute"
                raise UnsupportedResourcesError(msg)
            processes_per_node = processes_per_node or 1
        elif self.process_placement == ProcessPlacement.NO_CONSTRAINTS:
            if processes_per_node or nodes:
                msg = "ProcessPlacement.NO_CONSTRAINTS is incompatible with processes_per_node and nodes attribute"
                raise UnsupportedResourcesError(msg)
            if not processes:
                processes = 1

        return [nodes, processes, processes_per_node]


@dataclass
class QJobInfo(QTKObject):
    memory: int | None = None
    """Job memory in Kb."""

    memory_per_cpu: int | None = None
    """Job memory per CPU in Kb."""

    nodes: int | None = None
    """Number of nodes."""

    cpus: int | None = None
    """Number of CPUs."""

    threads_per_process: int | None = None
    """Number of threads per process."""

    time_limit: int | None = None
    """Time limit in seconds."""


@dataclass
class QJob(QTKObject):
    name: str | None = None
    """Job name."""

    job_id: str | None = None
    """Job ID."""

    exit_status: int | None = None
    """Shell exit status."""

    state: QState | None = None
    """Standardized job state."""

    sub_state: QSubState | None = None
    """Standardized job substate."""

    info: QJobInfo | None = None
    """Job info."""

    account: str | None = None
    """Job execution account name."""

    runtime: int | None = None
    """Job runtime in seconds."""

    queue_name: str | None = None
    """Job execution queue name."""
