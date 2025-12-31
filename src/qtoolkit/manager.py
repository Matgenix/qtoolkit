from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from qtoolkit.core.base import QTKObject
from qtoolkit.core.data_objects import QJob
from qtoolkit.core.exceptions import CommandFailedError
from qtoolkit.host.local import LocalHost

if TYPE_CHECKING:
    from qtoolkit.core.data_objects import CancelResult, QResources, SubmissionResult
    from qtoolkit.host.base import BaseHost
    from qtoolkit.io.base import BaseSchedulerIO


class QueueManager(QTKObject):
    """
    Main interface to interact with a job queue on a given host.

    Attributes
    ----------
    scheduler_io : BaseSchedulerIO
        The scheduler IO implementation (e.g., SlurmIO).
    host : BaseHost
        The host where commands are executed (e.g., LocalHost, RemoteHost).
    """

    def __init__(self, scheduler_io: BaseSchedulerIO, host: BaseHost = None):
        self.scheduler_io = scheduler_io
        self.host = host or LocalHost()

    def execute_cmd(
        self, cmd: str, workdir: str | Path | None = None
    ) -> tuple[str, str, int]:
        """Execute a command.

        Parameters
        ----------
        cmd
            Command to be executed.
        workdir
            Path where the command will be executed.

        Returns
        -------
        stdout : str
            Standard output of the command.
        stderr : str
            Standard error of the command.
        exit_code : int
            Exit code of the command.
        """
        return self.host.execute(cmd, workdir)

    def get_submission_script(
        self,
        commands: str | list[str] | None,
        options: dict | QResources | None = None,
        work_dir: str | Path | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        environment: dict | None = None,
    ) -> str:
        """
        Generate the full submission script.

        Parameters
        ----------
        commands
            The main commands to execute in the job.
        options
            Scheduler options.
        work_dir
            Working directory for the job.
        pre_run
            Commands to run before the main commands.
        post_run
            Commands to run after the main commands.
        environment
            Configuration for the execution environment (modules, conda, env vars).

        Returns
        -------
        str
            The generated submission script content.
        """
        commands_list = []
        if environment_setup := self.get_environment_setup(environment):
            commands_list.append(environment_setup)
        if change_dir := self.get_change_dir(work_dir):
            commands_list.append(change_dir)
        if pre_run := self.get_pre_run(pre_run):
            commands_list.append(pre_run)
        if run_commands := self.get_run_commands(commands):
            commands_list.append(run_commands)
        if post_run := self.get_post_run(post_run):
            commands_list.append(post_run)
        return self.scheduler_io.get_submission_script(commands_list, options)

    def get_environment_setup(self, env_config: dict | None) -> str | None:
        """
        Generate bash commands to set up the execution environment.

        Parameters
        ----------
        env_config
            Environment configuration dictionary.

        Returns
        -------
        str or None
            The environment setup commands, or None if no config provided.
        """
        if env_config:
            env_setup = []
            if "modules" in env_config:
                env_setup.append("module purge")
                env_setup += [f"module load {mod}" for mod in env_config["modules"]]
            if "source_files" in env_config:
                env_setup += [
                    f"source {source_file}"
                    for source_file in env_config["source_files"]
                ]
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

    def get_change_dir(self, dir_path: str | Path | None) -> str:
        """
        Generate the command to change to the working directory.

        Parameters
        ----------
        dir_path
            The directory path.

        Returns
        -------
        str
            The 'cd' command string.
        """
        if dir_path:
            return f"cd {dir_path}"
        return ""

    def get_pre_run(self, pre_run: str | list[str] | None) -> str | None:
        """
        Process the pre-run commands.

        Parameters
        ----------
        pre_run
            The pre-run commands.

        Returns
        -------
        str or None
            The processed pre-run commands string.
        """
        if isinstance(pre_run, list):
            return "\n".join(pre_run)
        return pre_run

    def get_run_commands(self, commands: str | list[str] | None) -> str | None:
        """
        Process the main run commands.

        Parameters
        ----------
        commands
            The main commands.

        Returns
        -------
        str or None
            The processed run commands string.
        """
        if isinstance(commands, str):
            return commands
        if isinstance(commands, list):
            return "\n".join(commands)
        raise ValueError("commands should be a str or a list of str.")

    def get_post_run(self, post_run: str | list[str] | None) -> str | None:
        """
        Process the post-run commands.

        Parameters
        ----------
        post_run
            The post-run commands.

        Returns
        -------
        str or None
            The processed post-run commands string.
        """
        if isinstance(post_run, list):
            return "\n".join(post_run)
        return post_run

    def submit(
        self,
        commands: str | list[str] | None,
        options: dict | QResources | None = None,
        work_dir: str | Path | None = None,
        environment: dict | None = None,
        script_fname: str = "submit.script",
        create_submit_dir: bool = False,
    ) -> SubmissionResult:
        """
        Submit a job to the queue.

        Parameters
        ----------
        commands
            The commands to run in the job.
        options
            Scheduler options.
        work_dir
            Working directory for the job.
        environment
            Environment setup configuration.
        script_fname
            Filename for the submission script.
        create_submit_dir
            Whether to create the working directory if it doesn't exist.

        Returns
        -------
        SubmissionResult
            The result of the submission.
        """
        script_str = self.get_submission_script(
            commands=commands,
            options=options,
            # TODO: Do we need the submit_dir here ?
            #  Should we distinguish submit_dir and work_dir ?
            work_dir=work_dir,
            environment=environment,
        )
        # TODO: deal with remote directory directly on the host here.
        #  Will currently only work on the localhost.
        work_dir = Path(work_dir) if work_dir is not None else Path.cwd()
        if create_submit_dir:
            created = self.host.mkdir(work_dir, recursive=True, exist_ok=True)
            if not created:
                raise RuntimeError("failed to create directory")
        script_fpath = Path(work_dir, script_fname)
        self.host.write_text_file(script_fpath, script_str)
        submit_cmd = self.scheduler_io.get_submit_cmd(script_fpath)
        stdout, stderr, returncode = self.execute_cmd(submit_cmd, work_dir)
        return self.scheduler_io.parse_submit_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def cancel(self, job: QJob | int | str) -> CancelResult:
        """
        Cancel a job from the queue.

        Parameters
        ----------
        job
            The job to cancel.

        Returns
        -------
        CancelResult
            The result of the cancellation.
        """
        cancel_cmd = self.scheduler_io.get_cancel_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(cancel_cmd)
        return self.scheduler_io.parse_cancel_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_job(self, job: QJob | int | str) -> QJob | None:
        """Get job from job id or QJob object.

        Parameters
        ----------
        job
            Identifier of the job to get.

        Returns
        -------
        :py:class:`qtoolkit.QJob` object or None
            Qjob object corresponding to the job id provided or None if no job
            was found with that id.

        """
        job_cmd = self.scheduler_io.get_job_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(job_cmd)
        job_str = job.job_id if isinstance(job, QJob) else str(job)
        try:
            return self.scheduler_io.parse_job_output(
                exit_code=returncode, stdout=stdout, stderr=stderr, job_id=job_str
            )
        # TODO: deal more specifically with why the command failed here maybe ?
        except CommandFailedError:
            return None

    def get_jobs_list(
        self, jobs: list[QJob | int | str] | None = None, user: str | None = None
    ) -> list[QJob]:
        """
        Get a list of jobs from the queue.

        Parameters
        ----------
        jobs
            List of job identifiers to retrieve.
        user
            Filter jobs by username.

        Returns
        -------
        list of QJob
            The list of retrieved jobs.
        """
        job_cmd = self.scheduler_io.get_jobs_list_cmd(jobs, user)
        job_ids_str = self.scheduler_io.generate_ids_list(jobs)
        stdout, stderr, returncode = self.execute_cmd(job_cmd)
        return self.scheduler_io.parse_jobs_list_output(
            exit_code=returncode, stdout=stdout, stderr=stderr, job_ids=job_ids_str
        )
