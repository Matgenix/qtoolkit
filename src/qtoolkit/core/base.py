from enum import Enum

try:
    from monty.json import MSONable

    supercls = MSONable
    enum_superclses = (MSONable, Enum)

except ModuleNotFoundError:
    supercls = object
    enum_superclses = (Enum,)  # type: ignore


class QBase(supercls):  # type: ignore
    pass


class QEnum(*enum_superclses):  # type: ignore
    pass
