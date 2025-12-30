
********
Overview
********

qtoolkit is a Python package designed to provide a programmatic interface for interacting with standard queue managers commonly found on high-performance computing (HPC) clusters, such as Slurm, PBS, and SGE.

The primary goal of qtoolkit is to offer a slim, flexible, and decoupled tool that allows third-party software to manage computational jobs without being tied to the specific details of a particular scheduler or the environment where commands are executed.

Design Philosophy
=================

qtoolkit is built around several key design choices:

*   **Slim dependencies**: The package aims to have minimal dependencies, making it easy to integrate into various environments and workflows.
*   **Decoupled command generation and parsing**: Instead of directly executing commands, qtoolkit's scheduler interfaces (subclasses of ``BaseSchedulerIO``) expose methods to:

    *  Build the shell commands required for job submission, cancellation, and monitoring.
    *  Parse the output of these commands into standardized Python objects.
*   **Platform agnostic execution**: By separating command generation from execution, qtoolkit allows users to choose how and where the commands are run (e.g., locally, via SSH, or through other remote execution frameworks).
*   **Standardized data objects**: qtoolkit uses a set of common data objects (like ``QJob``, ``QResources``, and ``QState``) to represent jobs and their requirements across different schedulers, providing a consistent API for users.

Supported Schedulers
====================

Currently, qtoolkit provides implementations for the following queue managers:

*   **Slurm**: Simple Linux Utility for Resource Management.
*   **PBS**: Portable Batch System (including variants like OpenPBS and PBS Professional).
*   **SGE**: Sun Grid Engine (and its descendants).

Aside from HPC schedulers, qtoolkit also supports executing commands and monitoring them in a **unix shell**.

How it Works
============

The core of ``qtoolkit`` is built around the ``SchedulerIO`` classes, which focus on the decoupled generation of shell commands and the parsing of their outputs. This allows users to integrate scheduler interactions into any environment without being forced into a specific execution model.

The standard operations provided by each ``SchedulerIO`` implementation include:

*   **Script generation**: build a full shell script with appropriate scheduler headers.
*   **Submission**: generate the command to submit a script (e.g., ``sbatch`` or ``qsub``).
*   **Job status**: query job information.
*   **Cancellation**: terminate a job.

When defining job requirements (like nodes, memory, or time), users can choose between:

1.  **QResources object**: A standardized, scheduler-agnostic data object. This helps maintaining portability across different cluster environments.
2.  **Options dictionary**: A simple dictionary with keys that match the identifiers in the scheduler's header template. This provides a direct, low-level way to pass parameters specific to a particular scheduler.

While ``qtoolkit`` provides a ``QueueManager`` and ``Host`` abstraction to simplify the common case of executing these commands locally or via SSH, the primary strength of the package lies in its ability to generate and parse these commands independently of how they are run.
