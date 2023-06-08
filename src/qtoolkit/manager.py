from __future__ import annotations

from pathlib import Path

from qtoolkit.core.base import QTKObject
from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.host.base import BaseHost
from qtoolkit.host.local import LocalHost
from qtoolkit.io.base import BaseSchedulerIO


class QueueManager(QTKObject):
    """Base class for job queues.

    Attributes
    ----------
    scheduler_io : str
        Name of the queue
    host : BaseHost
        Host where the command should be executed.
    """

    def __init__(self, scheduler_io: BaseSchedulerIO, host: BaseHost = None):
        self.scheduler_io = scheduler_io
        self.host = host or LocalHost()

    def execute_cmd(self, cmd: str, workdir: str | Path | None = None):
        """Execute a command.

        Parameters
        ----------
        cmd : str
            Command to be executed
        workdir: str or None
            path where the command will be executed.

        Returns
        -------
        stdout : str
        stderr : str
        exit_code : int
        """
        return self.host.execute(cmd, workdir)

    def get_submission_script(
        self,
        commands: str | list[str] | None,
        options: dict | QResources | None = None,
        work_dir: str | Path | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        environment=None,
    ) -> str:
        """ """
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

    def get_environment_setup(self, env_config) -> str:
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

    def get_change_dir(self, dir_path: str | Path | None) -> str:
        if dir_path:
            return f"cd {dir_path}"
        return ""

    def get_pre_run(self, pre_run) -> str:
        pass

    def get_run_commands(self, commands) -> str:
        if isinstance(commands, str):
            return commands
        elif isinstance(commands, list):
            return "\n".join(commands)
        else:
            raise ValueError("commands should be a str or a list of str.")

    def get_post_run(self, post_run) -> str:
        pass

    def submit(
        self,
        commands: str | list[str] | None,
        options=None,
        work_dir=None,
        environment=None,
        script_fname="submit.script",
        create_submit_dir=False,
    ) -> SubmissionResult:
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
        cancel_cmd = self.scheduler_io.get_cancel_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(cancel_cmd)
        return self.scheduler_io.parse_cancel_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_job(self, job: QJob | int | str) -> QJob | None:
        job_cmd = self.scheduler_io.get_job_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(job_cmd)
        return self.scheduler_io.parse_job_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_jobs_list(
        self, jobs: list[QJob | int | str] | None = None, user: str | None = None
    ) -> list[QJob]:
        job_cmd = self.scheduler_io.get_jobs_list_cmd(jobs, user)
        stdout, stderr, returncode = self.execute_cmd(job_cmd)
        return self.scheduler_io.parse_jobs_list_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )
