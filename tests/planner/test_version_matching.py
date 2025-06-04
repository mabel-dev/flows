import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from flows.internal import _get_step


def test_version_matching():
    versions = {
        "0.0.1": "A",
        "0.0.2": "B",
        "0.1.0": "C",
        "1.0.0": "D",
    }

    assert _get_step(step_name="", version="0.0.1", available_versions=versions) == "A"
    assert _get_step(step_name="", version="0.*", available_versions=versions) == "C"
    assert _get_step(step_name="", version="0.0.*", available_versions=versions) == "B"
    assert _get_step(step_name="", version="latest", available_versions=versions) == "D"
    assert _get_step(step_name="", version="1.0.0", available_versions=versions) == "D"
    assert _get_step(step_name="", version="0.1.*", available_versions=versions) == "C"
    assert _get_step(step_name="", version="1.*", available_versions=versions) == "D"
    try:
        assert _get_step(step_name="", version="2.*", available_versions=versions)
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError for unmatched wildcard"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
