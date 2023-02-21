# from dataclasses import dataclass
#
# from qtoolkit.core.base import QBase
# from qtoolkit.core.data_objects import QState, QSubState, QResources, QJobInfo
#
# from typing import Optional
#
# TODO: this has been moved to data_objects for now. See if it should be here
#  for some reason ?
# @dataclass
# class QJob(QBase):
#     name: str
#     qid: Optional[str]
#     exit_status: Optional[int]
#     state: Optional[QState]  # Standard
#     sub_state: Optional[QSubState]
#     queue: Optional[str]
#     resources: Optional[QResources]
#     job_info: Optional[QJobInfo]
