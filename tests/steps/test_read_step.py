"""
Test cases for the ReadStep implementation in the internal Python module.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from flows.internal.read import get_step


def test_get_read_step():
    # Test getting the latest version
    step = get_step()
    assert step is not None, "Failed to get the latest ReadStep implementation"

    # Test getting a specific version
    step_v1 = get_step("1.0.0")
    assert step_v1 is not None, "Failed to get ReadStep version 1.0.0"

    step_latest = get_step("latest")
    assert step_latest is not None, "Failed to get the latest ReadStep implementation by name"

    # Ensure the latest version matches the expected version
    assert step_v1.__name__ == "ReadStep", "Expected ReadStep class name mismatch"

    # Test with an unsupported version
    try:
        get_step("999.999.999")
        assert False, "Expected ValueError for unsupported version"
    except ValueError as e:
        assert str(e) == "Unsupported interal/read version: 999.999.999", "Unexpected error message"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
