"""
Test cases for the PythonStep implementation in the internal Python module.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from flows.internal.python import get_step


def test_get_python_step():
    # Test getting the latest version
    step = get_step()
    assert step is not None, "Failed to get the latest PythonStep implementation"

    # Test getting a specific version
    step_v1 = get_step("1.0.0")
    assert step_v1 is not None, "Failed to get PythonStep version 1.0.0"

    step_latest = get_step("latest")
    assert step_latest is not None, "Failed to get the latest PythonStep implementation by name"

    # Ensure the latest version matches the expected version
    assert step_v1.__name__ == "PythonStep", "Expected PythonStep class name mismatch"

    # Test with an unsupported version
    try:
        get_step("999.999.999")
        assert False, "Expected ValueError for unsupported version"
    except ValueError as e:
        assert str(e) == "Unsupported interal/python version: 999.999.999", (
            "Unexpected error message"
        )


def test_python_step_execution():
    # Test execution of the PythonStep with a simple code snippet
    step = get_step()
    assert step is not None, "Failed to get the latest PythonStep implementation"

    config = {
        "code": "def process(data, context): return {'result': 2}, {'context': context}",
    }
    flow_config = {}

    python_step = step(config, flow_config)
    result = python_step.execute(data=[{"input": 5}], context={})

    assert result == {"result": 10}, "PythonStep execution did not return expected result"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
