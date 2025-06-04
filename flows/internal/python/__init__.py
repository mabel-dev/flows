import importlib
import pkgutil
import re


def find_versions():
    versions = {}
    version_pattern = re.compile(r"version_(\d+)_(\d+)_(\d+)")
    for _, modname, _ in pkgutil.iter_modules(__path__):
        match = version_pattern.fullmatch(modname)
        if match:
            version_str = ".".join(match.groups())
            module = importlib.import_module(f".{modname}", __name__)
            if hasattr(module, "PythonStep"):
                versions[version_str] = getattr(module, "PythonStep")
    return versions
