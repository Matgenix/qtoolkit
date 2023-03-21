from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

from qtoolkit.core.base import QBase
from qtoolkit.core.data_objects import QJob
from qtoolkit.host.base import BaseHost
from qtoolkit.host.local import LocalHost


class QTemplate(Template):
    delimiter = "$$"


@dataclass
class BaseQueue(QBase):
    """Base class for job queues.

    Attributes
    ----------
    name : str
        Name of the queue
    host : BaseHost
        Host where the command should be executed.
    """

    header_template: str
    name: str = "name of queue"
    host: BaseHost = field(default_factory=LocalHost)
    default_shebang: str = "#!/bin/bash"

    SCRIPT_FNAME = "submit.script"
    SUBMIT_CMD: str | None = None

    # host : QToolKit.Host or paramiko Client or Fabric client or None
    #         The host where the command should be executed.

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

    def execute_cmd(self, cmd):
        """Execute a command.

        Parameters
        ----------
        cmd : str
            Command to be executed

        Returns
        -------
        stdout : str
        stderr : str
        exit_code : int
        """
        return self.host.execute(cmd)

    def get_submission_script(
        self,
        commands: str | list[str] | None,
        resources=None,
        submit_dir=None,
        environment=None,
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
        if header := self.get_header(resources):
            script_blocks.append(header)
        if environment_setup := self.get_environment_setup(environment):
            script_blocks.append(environment_setup)
        if change_dir := self.get_change_dir():
            script_blocks.append(change_dir)
        if prerun := self.get_prerun():
            script_blocks.append(prerun)
        if run_commands := self.get_run_commands(commands):
            script_blocks.append(run_commands)
        if postrun := self.get_postrun():
            script_blocks.append(postrun)
        if footer := self.get_footer():
            script_blocks.append(footer)
        return "\n".join(script_blocks)

    @property
    def shebang(self):
        return "#!/bin/bash"

    # @abc.abstractmethod
    # def get_header(self, job):
    #     pass

    def get_header(self, resources):
        # needs info from self.meta_info (email, job name [also execution])
        # queuing_options (priority, account, qos and submit as hold)
        # execution (rerunnable)
        # resources (nodes, cores, memory, time, [gpus])
        # default values for (almost) everything in the object ?
        mapping = {}
        if resources:
            mapping.update(resources)
        unclean_header = QTemplate(self.header_template).safe_substitute(mapping)
        # Remove lines with leftover $$.
        clean_header = []
        for line in unclean_header.split("\n"):
            if "$$" not in line:
                clean_header.append(line)

        return "\n".join(clean_header)

    def get_environment_setup(self, env_config):
        if env_config:
            env_setup = []
            if "modules" in env_config:
                env_setup.append("module purge")
                for mod in env_config["modules"]:
                    env_setup.append(f"module load {mod}")
            if "source_files" in env_config:
                for source_file in env_config["source_files"]:
                    env_setup.append(f"source {source_file}")
            if "conda_environment" in env_config:
                env_setup.append(f'conda activate {env_config["conda_environment"]}')
            if "environ" in env_config:
                for var, value in env_config["environ"].items():
                    env_setup.append(f"export {var}={value}")
            return "\n".join(env_setup)
        # This is from aiida, maybe we need to think about this escape_for_bash ?
        # lines = ['# ENVIRONMENT VARIABLES BEGIN ###']
        # for key, value in template.job_environment.items():
        #     lines.append(f'export {key.strip()}={
        #         escape_for_bash(value,
        #                         template.environment_variables_double_quotes)
        #         }')
        # lines.append('# ENVIRONMENT VARIABLES END ###')
        return None

    def get_change_dir(self):
        pass

    def get_prerun(self):
        pass

    def get_run_commands(self, commands):
        if isinstance(commands, str):
            return commands
        elif isinstance(commands, list):
            return "\n".join(commands)
        else:
            raise ValueError("commands should be a str or a list of str.")

    def get_postrun(self):
        pass

    def get_footer(self):
        pass

    def get_submit_cmd(self, script_file: str | Path = SCRIPT_FNAME) -> str:
        """
        Get the command used to submit a given script to the queue.

        Parameters
        ----------
        script_file: (str) name of the script file to use.
        """

        return f"{self.SUBMIT_CMD} {script_file}"

    def get_cancel_cmd(self, job: QJob | int | str) -> str:
        """
        Get the command used to cancel a given job.

        Parameters
        ----------
        job: (str) job to be cancelled.
        """
        job_id = QJob.job_id if isinstance(job, QJob) else job
        return f"{self.CANCEL_CMD} {job_id}"

    def write_script(self, script_fpath: str | Path, script_content: str) -> None:
        self.host.write_text_file(script_fpath, script_content)

    @abc.abstractmethod
    def _parse_submit_cmd_output(self, exit_code, stdout, stderr):
        pass

    @abc.abstractmethod
    def _parse_cancel_cmd_output(self, exit_code, stdout, stderr):
        pass

    def submit(
        self,
        commands: str | list[str] | None,
        resources=None,
        submit_dir=None,
        environment=None,
        script_fname=SCRIPT_FNAME,
        create_submit_dir=False,
    ):
        script_str = self.get_submission_script(
            commands=commands,
            resources=resources,
            # TODO: Do we need the submit_dir here ?
            #  Should we distinguish submit_dir and work_dir ?
            submit_dir=submit_dir,
            environment=environment,
        )
        # TODO: deal with remote directory directly on the host here.
        #  Will currently only work on the localhost.
        submit_dir = Path(submit_dir) if submit_dir is not None else Path.cwd()
        if create_submit_dir:
            self.host.mkdir(submit_dir, recursive=True, exist_ok=True)
        script_fpath = Path(submit_dir, script_fname)
        self.write_script(script_fpath, script_str)
        submit_cmd = self.get_submit_cmd(script_fpath)
        print(submit_cmd)
        stdout, stderr, returncode = self.execute_cmd(submit_cmd)
        return self._parse_submit_cmd_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_job_info(self, job: QJob | int | str):
        pass

    def get_jobs(self, jobs: list[QJob | int | str]):
        pass

    def cancel(self, job: QJob | int | str):
        cancel_cmd = self.get_cancel_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(cancel_cmd)
        return self._parse_cancel_cmd_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    # @abc.abstractmethod
    # def _get_jobs_cmd(self, jobs=None, user=None) -> str:
    #     """Get multiple jobs at once."""
    #     pass

    @abc.abstractmethod
    def get_job(self, job: QJob | int | str):
        pass
