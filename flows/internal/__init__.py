def get_step(step_name: str = None, version: str = "latest"):
    if step_name == "read":
        from flows.internal.read import get_step as read_step

        return read_step(version)

    else:
        from flows.internal.read import get_step as read_step

        return read_step(version)
