from enum import Enum

try:
    from monty.json import MSONable

    supercls = MSONable
    enum_superclses = (MSONable, Enum)

except ModuleNotFoundError:
    supercls = object
    enum_superclses = (Enum,)  # type: ignore


class QTKObject(supercls):  # type: ignore
    pass


class QTKEnum(*enum_superclses):  # type: ignore
    pass
