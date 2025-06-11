import importlib
import pkgutil
import re
from importlib import import_module
from types import ModuleType

STEP_REGISTRY = (
    "extract",
    "filter",
    "normalize",
    "read",
    "save",
    "sql",
)


def find_versions(module: ModuleType, expected_class: str) -> dict[str, type]:
    """
    Discover and load versioned step classes within a module.

    Parameters:
        module: ModuleType
            The parent step module (e.g., flows.internal.read).
        expected_class: str
            The expected class name to load from versioned modules.

    Returns:
        Dict mapping version strings to classes (e.g., "1.2.3": ReadStep)
    """
    versions = {}
    version_pattern = re.compile(r"version_(\d+)_(\d+)_(\d+)")
    for _, modname, _ in pkgutil.iter_modules(module.__path__):
        match = version_pattern.fullmatch(modname)
        if match:
            version_str = ".".join(match.groups())
            version_module = importlib.import_module(f".{modname}", module.__name__)
            if hasattr(version_module, expected_class):
                versions[version_str] = getattr(version_module, expected_class)
    return versions


def get_step(step_name: str, version: str = "latest"):
    """
    Get a step class by name and version.

    Parameters:
        step_name: str
            Logical name of the step, e.g., "read", "filter".
        version: str
            Requested version, or "latest" for the highest available.

    Returns:
        The selected step class.

    Raises:
        ValueError if the step name is unknown or the version is not available.
    """
    if step_name not in STEP_REGISTRY:
        raise ValueError(f"Unknown step: {step_name!r}")

    module = import_module(f"flows.internal.{step_name}")
    available_versions = find_versions(module, step_name.capitalize() + "Step")

    if not available_versions:
        raise ValueError(f"No versions found for step {step_name!r}")

    if version == "latest":
        version = sorted(available_versions, key=lambda x: tuple(map(int, x.split("."))))[-1]

    if version not in available_versions:
        raise ValueError(
            f"Version {version!r} not found for step {step_name!r}. "
            f"Available: {list(available_versions.keys())}"
        )

    return available_versions[version]
