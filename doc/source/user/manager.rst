**********************
Using the QueueManager
**********************

The ``QueueManager`` is a full implementation of a queue manager that provides a high-level API to submit, monitor, and cancel jobs.

Initialization
==============

A ``QueueManager`` requires a ``SchedulerIO`` instance. By default, it uses a ``LocalHost`` to execute commands on the machine where it is running.

.. code-block:: python

    from qtoolkit.manager import QueueManager
    from qtoolkit.io.slurm import SlurmIO
    from qtoolkit.host.local import LocalHost

    slurm = SlurmIO()
    host = LocalHost()
    manager = QueueManager(scheduler_io=slurm, host=host)

Submitting Jobs
===============

The ``submit`` method handles the complete job submission process:

1.  Generating a submission script.
2.  Writing the script to a specified directory.
3.  Executing the submission command.
4.  Parsing the output to retrieve the job ID.

.. code-block:: python

    commands = ["module load python", "python script.py"]
    result = manager.submit(
        commands=commands,
        work_dir="path/to/workdir",
        script_fname="submit.sh",
        create_submit_dir=True
    )

Environment Configuration
-------------------------

You can pass an ``environment`` dictionary to ``submit`` to configure the execution environment:

.. code-block:: python

    env = {
        "modules": ["intel/2021", "python/3.9"],
        "source_files": ["/path/to/env_vars.sh"],
        "conda_environment": "my_env",
        "environ": {"OMP_NUM_THREADS": "4"}
    }
    result = manager.submit(commands=commands, environment=env)

This will add the following lines to the submission script:

.. code-block:: bash

    module purge
    module load intel/2021
    module load python/3.9
    source /path/to/env_vars.sh
    conda activate my_env
    export OMP_NUM_THREADS=4    

Managing Jobs
=============

Retrieving Job Information
--------------------------

Use ``get_job`` to get a ``QJob`` object for a specific job:

.. code-block:: python

    job = manager.get_job("12345")
    if job:
        print(f"State: {job.state}")

Listing Jobs
------------

Use ``get_jobs_list`` to retrieve a list of jobs:

.. code-block:: python

    # List all jobs
    jobs = manager.get_jobs_list()

    # List specific jobs
    jobs = manager.get_jobs_list(jobs=["12345", "12346"])

    # List jobs for a specific user
    jobs = manager.get_jobs_list(user="username")

Cancelling Jobs
---------------

Use ``cancel`` to terminate a job:

.. code-block:: python

    result = manager.cancel("12345")
    if result.status.value == "SUCCESSFUL":
        print("Job cancelled.")
