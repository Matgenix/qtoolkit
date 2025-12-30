*****************
Configuring Hosts
*****************

In ``qtoolkit``, a ``Host`` represents the environment where shell commands are executed. This abstraction allows you to use the same ``QueueManager`` and ``SchedulerIO`` logic whether you are running commands locally or on a remote cluster.


Available Hosts
===============

LocalHost
---------

The ``LocalHost`` executes commands on the same machine where the Python process is running. This is the default host used by the ``QueueManager``.

.. code-block:: python

    from qtoolkit.host.local import LocalHost
    host = LocalHost()

RemoteHost
----------

The ``RemoteHost`` allows you to execute commands on a remote machine, typically via SSH. This is useful when your Python script is running on your workstation, but you need to submit jobs to a cluster.

.. code-block:: python

    from qtoolkit.host.remote import RemoteHost, RemoteConfig

    config = RemoteConfig(
        host="cluster.example.edu",
        user="myuser",
        root_dir="/home/myuser",
    )
    host = RemoteHost(config=config)

Host Interface
==============

All host classes implement a common interface, primarily the ``execute`` method:

.. code-block:: python

    # This is usually called internally by QueueManager
    stdout, stderr, exit_code = host.execute("ls -l", workdir="/tmp")

Hosts also provide methods for file manipulation, such as ``write_text_file``, which are used by the ``QueueManager`` to upload submission scripts.

Using a Host with QueueManager
==============================

When initializing a ``QueueManager``, pass the desired host instance:

.. code-block:: python

    from qtoolkit.manager import QueueManager
    from qtoolkit.io.slurm import SlurmIO

    manager = QueueManager(scheduler_io=SlurmIO(), host=RemoteHost(...))
