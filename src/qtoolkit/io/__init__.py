from qtoolkit.io.base import BaseSchedulerIO
from qtoolkit.io.pbs import PBSIO, PBSState
from qtoolkit.io.shell import ShellIO, ShellState
from qtoolkit.io.slurm import SlurmIO, SlurmState

scheduler_mapping = {"slurm": SlurmIO, "pbs": PBSIO, "shell": ShellIO}
