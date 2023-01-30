try:
    from monty.json import MSONable

    supercls = MSONable
except ModuleNotFoundError:
    supercls = object


class QBase(supercls):  # type: ignore
    pass
