from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from qtoolkit.core.base import QBase, QEnum
from qtoolkit.core.exceptions import UnsupportedResourcesError


class SubmissionStatus(QEnum):
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    JOB_ID_UNKNOWN = "JOB_ID_UNKNOWN"


@dataclass
class SubmissionResult(QBase):
    job_id: int | str | None = None
    step_id: int | None = None
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    status: SubmissionStatus | None = None


class CancelStatus(QEnum):
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    JOB_ID_UNKNOWN = "JOB_ID_UNKNOWN"


@dataclass
class CancelResult(QBase):
    job_id: int | str | None = None
    step_id: int | None = None
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    status: CancelStatus | None = None


class QState(QEnum):
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


class QSubState(QEnum):
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
        raise NotImplementedError


class ProcessPlacement(QEnum):
    NO_CONSTRAINTS = "NO_CONSTRAINTS"
    SCATTERED = "SCATTERED"
    SAME_NODE = "SAME_NODE"
    EVENLY_DISTRIBUTED = "EVENLY_DISTRIBUTED"


@dataclass
class QResources(QBase):
    """Data defining resources for a given job (submitted or to be submitted).

    Attributes
    ----------
    queue_name : str
        Name of the queue (or partition) used to submit a job or to which a job has
        been submitted.
    memory : int
        Maximum amount of memory requested for a job.
    nodes : int
        Number of nodes requested for a job.

    """

    queue_name: str | None = None
    job_name: str | None = None
    memory_per_thread: int | None = None
    nodes: int | None = None
    processes: int | None = None
    processes_per_node: int | None = None
    threads_per_process: int | None = None
    gpus_per_job: int | None = None
    time_limit: int | timedelta | None = None
    account: str | None = None
    qos: str | None = None
    priority: int | str | None = None
    output_filepath: str | Path | None = None
    error_filepath: str | Path | None = None
    process_placement: ProcessPlacement | None = None
    email_address: str | None = None
    rerunnable: bool | None = None

    project: str | None = None
    njobs: int | None = None  # for job arrays

    kwargs: dict | None = None

    def __post_init__(self):
        if self.process_placement is None:
            if self.processes and not self.processes_per_node and not self.nodes:
                self.process_placement = ProcessPlacement.NO_CONSTRAINTS  # type: ignore # due to QEnum
            elif self.nodes and self.processes_per_node and not self.processes:
                self.process_placement = ProcessPlacement.EVENLY_DISTRIBUTED
            else:
                msg = (
                    "When process_placement is None either define only nodes "
                    "plus processes_per_node or only processes"
                )
                raise UnsupportedResourcesError(msg)

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
class QJobInfo(QBase):
    memory: int | None = None  # in Kb
    memory_per_cpu: int | None = None  # in Kb
    nodes: int | None = None
    cpus: int | None = None
    threads_per_process: int | None = None
    time_limit: int | None = None


@dataclass
class QJob(QBase):
    name: str | None = None
    job_id: str | None = None
    exit_status: int | None = None
    state: QState | None = None  # Standard
    sub_state: QSubState | None = None
    info: QJobInfo | None = None
    account: str | None = None
    runtime: int | None = None
    queue_name: str | None = None
