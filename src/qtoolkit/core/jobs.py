from dataclasses import dataclass

from qtoolkit.core.base import QBase


@dataclass
class QJob(QBase):
    name: str
