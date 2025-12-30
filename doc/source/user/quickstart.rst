**********
Quickstart
**********

This guide will show you how to use the ``qtoolkit`` QueueManager to submit 
jobs directly to a queue manager using the ``QueueManager`` class. Note that 
the ``QueueManager`` is a simple implementation that can use the different 
interfaces provided by ``qtoolkit`` to interact with the queue manager.
If you are interested in handling directly the interactions with the queue
manager check the :doc:`core_concepts` section.

Basic Usage
===========

The most straightforward way to use ``qtoolkit`` is through the ``QueueManager``. You need to specify a scheduler interface and a host where the commands will be executed.

In this example, we use ``SlurmIO`` for a Slurm cluster and ``LocalHost`` to execute commands on the current machine.

.. code-block:: python

    from qtoolkit.manager import QueueManager
    from qtoolkit.io.slurm import SlurmIO
    from qtoolkit.host.local import LocalHost
    from qtoolkit.core.data_objects import QResources

    # 1. Initialize the scheduler and host
    slurm = SlurmIO()
    host = LocalHost()
    manager = QueueManager(scheduler_io=slurm, host=host)

    # 2. Define the job commands
    commands = "echo 'Hello from qtoolkit!'"

    # 3. Define the resources
    resources = QResources(
        job_name="my_first_job",
        nodes=1,
        processes_per_node=1,
        time_limit=300
    )

    # 4. Submit the job
    result = manager.submit(commands=commands, options=resources)

    if result.status.value == "SUCCESSFUL":
        print(f"Job submitted successfully! Job ID: {result.job_id}")
    else:
        print(f"Job submission failed: {result.stderr}")

Monitoring Jobs
===============

Once a job is submitted, you can check its status using the job ID.

.. code-block:: python

    # Get job information
    job = manager.get_job(result.job_id)

    if job:
        print(f"Job Name: {job.name}")
        print(f"Job State: {job.state}") # e.g., QUEUED, RUNNING, DONE
        print(f"Actual State: {job.sub_state}") # Slurm-specific state
    else:
        print("Job not found.")

You can also list all jobs for the current user or specific jobs:

.. code-block:: python

    # List all jobs
    jobs = manager.get_jobs_list()
    for j in jobs:
        print(f"ID: {j.job_id} | Name: {j.name} | State: {j.state}")

Cancelling a Job
================

If you need to cancel a job:

.. code-block:: python

    # Cancel the job
    cancel_result = manager.cancel(result.job_id)

    if cancel_result.status.value == "SUCCESSFUL":
        print("Job cancelled.")
    else:
        print(f"Failed to cancel job: {cancel_result.stderr}")

Next Steps
==========

*   Check the :doc:`core_concepts` for a deeper understanding of how ``qtoolkit`` is structured.
*   Explore the :doc:`resources` guide for detailed information on configuring job requirements.
