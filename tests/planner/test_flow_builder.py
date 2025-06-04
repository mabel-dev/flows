import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from flows.models import FlowModel


def test_flow_builder_simple():
    model = {
        "steps": [
            {
                "name": "load_data",
                "uses": "internal/read@latest",
                "config": {"path": "gs://data.csv"},
            }
        ]
    }
    flow = FlowModel.from_dict(model)
    assert flow is not None, "Failed to create FlowModel"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
