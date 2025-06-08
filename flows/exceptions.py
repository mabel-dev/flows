class MissingDependencyError(Exception):
    """Exception raised when a required module is not found."""

    def __init__(self, dependency):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, "
        "please install or include in requirements.txt"
        super().__init__(message)


class FlowError(Exception):
    pass


class TimeExceeded(FlowError):
    pass


__all__ = (
    "FlowError",
    "MissingDependencyError",
    "TimeExceeded",
)
