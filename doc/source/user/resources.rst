*******************************
Defining Resources (QResources)
*******************************

The ``QResources`` class provides a standardized way to describe the computational requirements for a job in ``qtoolkit``. 
It ensures that job requirements are specified in a consistent manner, regardless of the target scheduler.

.. note::

    Not all the attributes of ``QResources`` and process placement strategies are supported by all schedulers. 
    If you try to use an attribute that is not supported by the scheduler, an ``UnsupportedResourcesError`` will be raised.

.. note::

    The different types of process placement may have limitations on the keywords that can be passed to the 
    ``QResources`` object. For example, if the ``same_node`` process placement strategy is used, the ``nodes`` and 
    ``processes_per_node`` keywords should be left undefined.
    Inconsistent values for the process placement strategy will result in an ``UnsupportedResourcesError`` exception.

Basic Resource Specification
============================

You can initialize ``QResources`` with keyword arguments for common requirements:

.. code-block:: python

    from qtoolkit.core.data_objects import QResources

    resources = QResources(
        job_name="my_calculation",
        queue_name="standard",
        time_limit=3600,  # 1 hour in seconds
        memory_per_thread=2048,  # 2 GB per thread
        email_address="user@example.com",
        processes=8,
    )

The **process placement is always required internally**. If the value is not specified explicitly
the code will attempt to find a default value based on the other values specified.
In this case the process placement will be set automatically to ``NO_CONSTRAINTS``.
In most cases it is preferable to set the process placement explicitly.


Process Placement Strategies
============================

``qtoolkit`` provides several strategies for placing processes across nodes. It is recommended to use the factory methods provided by ``QResources`` to ensure consistency:

No Constraints
--------------

Use ``no_constraints`` when you only care about the total number of processes and don't mind how they are distributed.

.. code-block:: python

    resources = QResources.no_constraints(processes=16)

Evenly Distributed
------------------

Use ``evenly_distributed`` to request a specific number of nodes and processes per node.

.. code-block:: python

    resources = QResources.evenly_distributed(nodes=2, processes_per_node=8)

Scattered
---------

Use ``scattered`` when you want each process to run on its own node.

.. code-block:: python

    resources = QResources.scattered(processes=4) # Request 4 nodes, 1 process each

Same Node
---------

Use ``same_node`` when all processes must run on the same node.

.. code-block:: python

    resources = QResources.same_node(processes=8) # Request 1 node with 8 processes

Advanced Options
================

threads_per_process
-------------------

If your application is multi-threaded (e.g., uses OpenMP), you can specify the number of threads per process:

.. code-block:: python

    resources = QResources.evenly_distributed(
        nodes=1,
        processes_per_node=4,
        threads_per_process=2
    )

gpus_per_job
------------

For GPU-accelerated workloads:

.. code-block:: python

    resources = QResources(processes=1, gpus_per_job=1)

scheduler_kwargs
----------------

If you need to pass scheduler-specific options that are not covered by the standard attributes, use ``scheduler_kwargs``. These will be passed directly to the ``SchedulerIO`` generator.

.. code-block:: python

    resources = QResources(
        processes=1,
        scheduler_kwargs={"gres": "gpu:rtx3080:1"} # Slurm specific syntax
    )

Alternatives to QResources
==========================

While ``QResources`` provides a convenient way to specify requirements in a scheduler-agnostic manner, you can also pass a standard Python dictionary directly to the ``options`` argument of ``submit`` or the ``SchedulerIO`` methods.

This dictionary should contain keys that match the identifiers defined in the target scheduler's header template (e.g., ``partition``, ``nodes``, ``time`` for Slurm).

.. code-block:: python

    from qtoolkit.io.slurm import SlurmIO

    slurm_io = SlurmIO()
    options_dict = {
        "partition": "standard",
        "nodes": 2,
        "ntasks_per_node": 8,
        "time": "01:00:00"
    }
    script = slurm_io.get_submission_script(commands="echo 'Hello'", options=options_dict)

This approach provides maximum flexibility but requires the user to know the specific keywords for the scheduler being used.

If a keyword missing from the template is passed a ``ValueError`` is raised.

Given the large number of options usually available for each scheduler, the header template for each scheduler 
only contains a subset of the available options. In each of the templates it is possible to pass a ``qverbatim`` option that 
will be added to the header as is, allowing to pass any scheduler-specific option. For example, in the case of a Slurm scheduler:

.. code-block:: python

    options = {
        "partition": "standard",
        "qverbatim": "#SBATCH --tmp=10G\n#SBATCH --nice=100"
    }
    script = slurm_io.get_submission_script(commands="echo 'Hello'", options=options)

Scheduler Templates
===================

Below are the default header templates for each supported scheduler. You can use any of the placeholders shown (e.g., ``partition``, ``job_name``) as keys in your ``options`` dictionary.

.. dropdown:: Slurm Template
    :color: primary
    :icon: eye

    .. literalinclude:: ../../../src/qtoolkit/io/templates/slurm
       :language: bash

.. dropdown:: PBS Template
    :color: primary
    :icon: eye

    .. literalinclude:: ../../../src/qtoolkit/io/templates/pbs
       :language: bash

.. dropdown:: SGE Template
    :color: primary
    :icon: eye

    .. literalinclude:: ../../../src/qtoolkit/io/templates/sge
       :language: bash

.. dropdown:: Shell Template
    :color: primary
    :icon: eye

    .. literalinclude:: ../../../src/qtoolkit/io/templates/shell
       :language: bash
