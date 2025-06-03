import importlib
import pkgutil
import re


def _find_versions():
    versions = {}
    version_pattern = re.compile(r"version_(\d+)_(\d+)_(\d+)")
    for _, modname, _ in pkgutil.iter_modules(__path__):
        match = version_pattern.fullmatch(modname)
        if match:
            version_str = ".".join(match.groups())
            module = importlib.import_module(f".{modname}", __name__)
            if hasattr(module, "ReadStep"):
                versions[version_str] = getattr(module, "ReadStep")
    return versions


_versions = _find_versions()
_latest = (
    sorted(_versions.keys(), key=lambda v: list(map(int, v.split("."))))[-1] if _versions else None
)


def get_step(version: str = "latest"):
    if not _versions:
        raise RuntimeError("No internal/read versions found.")
    if version is None or version == "latest":
        version = _latest
    if version not in _versions:
        raise ValueError(f"Unsupported interal/read version: {version}")
    return _versions[version]
