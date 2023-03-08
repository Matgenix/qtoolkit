# from __future__ import annotations
#
# from dataclasses import dataclass
#
# from qtoolkit.core.base import QBase
# from qtoolkit.core.data_objects import QState, QSubState, QResources, QJobInfo
#
#
# TODO: this has been moved to data_objects for now. See if it should be here
#  for some reason ?
# @dataclass
# class QJob(QBase):
#     name: str
#     qid: str | None
#     exit_status: int | None
#     state: QState | None  # Standard
#     sub_state: QSubState | None
#     queue: str | None
#     resources: QResources | None
#     job_info: QJobInfo | None
