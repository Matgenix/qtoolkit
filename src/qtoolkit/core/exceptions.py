class QTKError(Exception):
    """Base class for all the exceptions generated by qtoolkit."""


class CommandFailedError(QTKError):
    """
    Exception raised when the execution of a command has failed,
    typically by a non-zero return code.
    """


class OutputParsingError(QTKError):
    """
    Exception raised when errors are recognized during the parsing
    of the outputs of command.
    """


class UnsupportedResourcesError(QTKError):
    """
    Exception raised when the resources requested are not supported
    in qtoolkit for the chosen scheduler.
    """
