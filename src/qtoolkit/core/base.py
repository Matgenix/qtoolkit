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
    @classmethod
    def _validate_monty(cls, __input_value):
        """
        Override the original pydantic Validator for MSONable pattern.
        If not would not allow to deserialize as a standard Enum in pydantic,
        that just needs the value.
        """
        try:
            super()._validate_monty(__input_value)
        except ValueError as e:
            try:
                return cls(__input_value)
            except Exception:
                raise e
