import re


def _get_step(*, step_name: str, version: str, available_versions: dict):
    if not available_versions:
        raise RuntimeError(f"No versions of {step_name} found.")
    if version is None or version == "latest":
        version = (
            sorted(available_versions.keys(), key=lambda v: list(map(int, v.split("."))))[-1]
            if available_versions
            else None
        )

    if version in available_versions:
        return available_versions[version]

    # Support wildcards like 0.*, 0.0.*, etc.
    if "*" in version:
        if version.endswith(".*") and version.count(".") == 1:
            # Special case for single wildcard like 0.*
            version += ".*"
        pattern = version.replace(".", r"\.").replace("*", r"\d+")
        regex = re.compile(f"^{pattern}$")
        matches = [v for v in available_versions if regex.match(v)]
        if matches:
            # Return the highest matching version
            selected = sorted(matches, key=lambda v: list(map(int, v.split("."))))[-1]
            return available_versions[selected]
        raise ValueError(f"No versions of {step_name} match the pattern: {version}")

    raise ValueError(f"Unsupported {step_name} version: {version}")


def get_step(step_name: str = None, version: str = "latest"):
    if step_name == "read":
        from flows.internal.read import find_versions as find_read_versions

        return _get_step(
            step_name="internal/read",
            version=version,
            available_versions=find_read_versions(),
        )
    elif step_name == "filter":
        from flows.internal.filter import find_versions as find_filter_versions

        return _get_step(
            step_name="internal/filter",
            version=version,
            available_versions=find_filter_versions(),
        )
    elif step_name == "save":
        from flows.internal.save import find_versions as find_save_versions

        return _get_step(
            step_name="internal/save",
            version=version,
            available_versions=find_save_versions(),
        )
    elif step_name == "sql":
        from flows.internal.sql import find_versions as find_sql_versions

        return _get_step(
            step_name="internal/sql",
            version=version,
            available_versions=find_sql_versions(),
        )
    else:
        from flows.internal.read import find_versions as find_read_versions

        return _get_step(
            step_name="internal/read",
            version=version,
            available_versions=find_read_versions(),
        )
