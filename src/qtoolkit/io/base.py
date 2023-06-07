from __future__ import annotations

import abc
import shlex
from dataclasses import fields
from pathlib import Path
from string import Template

from qtoolkit.core.base import QBase
from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.core.exceptions import UnsupportedResourcesError


class QTemplate(Template):
    delimiter = "$$"

    def get_identifiers(self) -> list:
        """
        Returns a list of the valid identifiers in the template,
        in the order they first appear, ignoring any invalid identifiers.
        Imported from implementation in python 3.11 for backward compatibility.
        """
        ids = []
        for mo in self.pattern.finditer(self.template):
            named = mo.group("named") or mo.group("braced")
            if named is not None and named not in ids:
                # add a named group only the first time it appears
                ids.append(named)
            elif (
                named is None
                and mo.group("invalid") is None
                and mo.group("escaped") is None
            ):
                # If all the groups are None, there must be
                # another group we're not expecting
                raise ValueError("Unrecognized named group in pattern", self.pattern)
        return ids


class BaseSchedulerIO(QBase, abc.ABC):
    """Base class for job queues.

    Attributes
    ----------

    """

    header_template: str

    SUBMIT_CMD: str | None
    CANCEL_CMD: str | None

    shebang: str = "#!/bin/bash"

    # config: QueueConfig = None

    # scheduler = None,
    # name = None,
    # cores = None,
    # memory = None,
    # processes = None,
    # nanny = True,
    # protocol = None,
    # security = None,
    # interface = None,
    # death_timeout = None,
    # local_directory = None,
    # extra = None,
    # worker_extra_args = None,
    # job_extra = None,
    # job_extra_directives = None,
    # env_extra = None,
    # job_script_prologue = None,
    # header_skip = None,
    # job_directives_skip = None,
    # log_directory = None,
    # shebang = None,
    # python = sys.executable,
    # job_name = None,
    # config_name = None,

    """ABIPY
    Args:
        qname: Name of the queue.
        qparams: Dictionary with the parameters used in the template.
        setup: String or list of commands to execute during the initial setup.
        modules: String or list of modules to load before running the application.
        shell_env: Dictionary with the environment variables to export before
            running the application.
        omp_env: Dictionary with the OpenMP variables.
        pre_run: String or list of commands to execute before launching the
            calculation.
        post_run: String or list of commands to execute once the calculation is
            completed.
        mpi_runner: Path to the MPI runner or :class:`MpiRunner` instance.
            None if not used
        mpi_runner_options: Optional string with options passed to the mpi_runner.
        max_num_launches: Maximum number of submissions that can be done for a
            specific task. Defaults to 5
        qverbatim:
        min_cores, max_cores, hint_cores: Minimum, maximum, and hint limits of
            number of cores that can be used
        min_mem_per_proc=Minimum memory per process in megabytes.
        max_mem_per_proc=Maximum memory per process in megabytes.
        timelimit: initial time limit in seconds
        timelimit_hard: hard limelimit for this queue
        priority: Priority level, integer number > 0
        condition: Condition object (dictionary)
    """

    def get_submission_script(
        self,
        commands: str | list[str],
        options: dict | QResources | None = None,
    ) -> str:
        """
        This is roughly what/how it is done in the existing solutions.

        abipy: done with a str template (using $$ as a delimiter).
            Remaining "$$" delimiters are then removed at the end.
            It uses a ScriptEditor object to add/modify things to the templated script.
        The different steps of "get_script_str(...)" in abipy are summarized here:
            - _header, based on the str template (includes the shebang line and all
                #SBATCH, #PBS, ... directives)
            - change directory (added by the script editor)
            - setup section, list of commands executed before running (added
                by the script editor)
            - load modules section, list of modules to be loaded before running
                (added by the script editor)
            - setting of openmp environment variables (added by the script editor)
            - setting of shell environment variables (added by the script editor)
            - prerun, i.e. commands to run before execution, again? (added by
                the script editor)
            - run line (added by the script editor)
            - postrun (added by the script editor)

        aiida: done with a class template (JobTemplate) that should contain
            the required info to generate the job header. Other class templates
            are also used inside the generation, e.g. JobTemplateCodesInfo, which
            defines the command(s) to be run. The JobTemplate is only used as a
            container of the information and the script is generated not using
            templating but rather directly using python methods based on that
            "JobTemplate" container. Actually this JobTemplate is based on the
            DRMAA v2 specifications and many other objects are based on that too
            (e.g. machine, slots, etc ...).
        The different steps of "get_submit_script(...)" in aiida are summarized here:
            - shebang line
            - _header
                - all #SBATCH, #PBS etc ... lines defining the resources and other
                    info for the queuing system
                - some custom lines if it is not dealt with by the template
                - environment variables
            - prepend_text (something to be written before the run lines)
            - _run_line (defines the code execution(s) based on a CodeInfo object).
                There can be several codes run.
            - append_text (something to be written after the run lines)
            - _footer (some post commands done after the run) [note this is only
                done/needed for LSF in aiida]

        fireworks: done with a str template. similar to abipy (actually abipy took
            its initial concept from fireworks)

        dask_jobqueue: quite obscure ... the job header is done in the init of a
            given JobCluster (e.g. SLURMCluster) based on something in the actual
            Job object itself. Dask is not really meant to be our use case anyway.

        dpdispatcher: uses python's format() with 5 templates, combined into
            another python's format "script" template.
        Here are the steps:
            - header (includes shebang and #SBATCH, #PBS, ... directives)
            - custom directives
            - script environment (modules, environment variables, source
                somefiles, ...)
            - run command
            - append script lines
        In the templates of the different steps, there are some dpdispatcher's
            specific things (e.g. tag a job as finished by touching a file, ...)

        jobqueues: Some queues are using pure python (PBS, LSF, ...), some are
            using jinja2 templates (SLURM and SGE). Directly written to file.

        myqueue: the job queue directives are directly passed to the submit
            command (no #SBATCH, #PBS, ...).

        troika: uses a generic generator with a list of directives as well
            as a directive prefix. These directives are defined in specific
            files for each type of job queue.
        """
        script_blocks = [self.shebang]
        if header := self.generate_header(options):
            script_blocks.append(header)

        run_commands = self.generate_run_commands(commands)
        script_blocks.append(run_commands)

        if footer := self.generate_footer():
            script_blocks.append(footer)

        return "\n".join(script_blocks)

    def generate_header(self, options: dict | QResources | None) -> str:
        # needs info from self.meta_info (email, job name [also execution])
        # queuing_options (priority, account, qos and submit as hold)
        # execution (rerunnable)
        # resources (nodes, cores, memory, time, [gpus])
        # default values for (almost) everything in the object ?

        options = options or {}

        if isinstance(options, QResources):
            options = self.check_convert_qresources(options)

        template = QTemplate(self.header_template)

        # check that all the options are present in the template
        keys = set(options.keys())
        extra = keys.difference(template.get_identifiers())
        if extra:
            msg = f"The following keys are not present in the template: {', '.join(extra)}"
            raise ValueError(msg)

        unclean_header = template.safe_substitute(options)
        # Remove lines with leftover $$.
        clean_header = []
        for line in unclean_header.split("\n"):
            if "$$" not in line:
                clean_header.append(line)

        return "\n".join(clean_header)

    def generate_run_commands(self, commands: list[str] | str) -> str:
        if isinstance(commands, list):
            commands = "\n".join(commands)

        return commands

    def generate_footer(self) -> str:
        return ""

    def generate_ids_list(self, jobs: list[QJob | int | str] | None) -> list[str]:
        if jobs is None:
            return None
        ids_list = []
        for j in jobs:
            if isinstance(j, QJob):
                ids_list.append(str(j.job_id))
            else:
                ids_list.append(str(j))

        return ids_list

    def get_submit_cmd(self, script_file: str | Path | None = "submit.script") -> str:
        """
        Get the command used to submit a given script to the queue.

        Parameters
        ----------
        script_file: (str) path of the script file to use.
        """
        script_file = script_file or ""
        return f"{self.SUBMIT_CMD} {script_file}"

    @abc.abstractmethod
    def parse_submit_output(self, exit_code, stdout, stderr) -> SubmissionResult:
        pass

    def get_cancel_cmd(self, job: QJob | int | str) -> str:
        """
        Get the command used to cancel a given job.

        Parameters
        ----------
        job: (str) job to be cancelled.
        """
        job_id = job.job_id if isinstance(job, QJob) else job
        if job_id is None or job_id == "":
            raise ValueError(
                f"The id of the job to be cancelled should be defined. Received: {job_id}"
            )
        return f"{self.CANCEL_CMD} {job_id}"

    @abc.abstractmethod
    def parse_cancel_output(self, exit_code, stdout, stderr) -> CancelResult:
        pass

    def get_job_cmd(self, job: QJob | int | str) -> str:
        job_id = self.generate_ids_list([job])[0]
        shlex.quote(job_id)
        return self._get_job_cmd(job_id)

    @abc.abstractmethod
    def _get_job_cmd(self, job_id: str) -> str:
        pass

    @abc.abstractmethod
    def parse_job_output(self, exit_code, stdout, stderr) -> QJob | None:
        pass

    def check_convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a Qresources instance to a dict that will be used to fill in the
        header of the submission script.
        Also checks that passed values are declared to be handled by the corresponding
        subclass.
        """
        not_none = set()
        for field in fields(resources):
            if getattr(resources, field.name) is not None:
                not_none.add(field.name)

        unsupported_options = not_none.difference(self.supported_qresources_keys)

        if unsupported_options:
            msg = f"Keys not supported: {', '.join(unsupported_options)}"
            raise UnsupportedResourcesError(msg)

        return self._convert_qresources(resources)

    @abc.abstractmethod
    def _convert_qresources(self, resources: QResources) -> dict:
        """
        Converts a QResources instance to a dict that will be used to fill in the
        header of the submission script.
        A subclass does not strictly need to support all the options available in
        QResources. For this reason a list of supported attributes should be
        maintained and the supported attributes in the implementation of this
        method should match the list of values defined in  supported_qresources_keys.
        """

    @property
    def supported_qresources_keys(self) -> list:
        """
        List of attributes of QResources that are correctly handled by the
        _convert_qresources method. It is used to validate that the user
        does not pass an unsupported value, expecting to have an effect.
        """
        return []

    def get_jobs_list_cmd(
        self, jobs: list[QJob | int | str] | None, user: str | None
    ) -> str:
        job_ids = self.generate_ids_list(jobs)
        if user:
            user = shlex.quote(user)
        return self._get_jobs_list_cmd(job_ids, user)

    @abc.abstractmethod
    def _get_jobs_list_cmd(
        self, job_ids: list[str] | None = None, user: str | None = None
    ) -> str:
        pass

    @abc.abstractmethod
    def parse_jobs_list_output(self, exit_code, stdout, stderr) -> list[QJob]:
        pass
