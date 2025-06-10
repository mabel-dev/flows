# isort: skip_file

import sys

sys.path.append(".")

from orso.logging import set_log_name

from flows.models import FlowModel
from flows.models import TenantModel


set_log_name("FLOWS")
FLOW_NAME = "example"


if __name__ == "__main__":
    # Load the pipeline definition from YAML
    pipeline = FlowModel.from_name(FLOW_NAME)

    tenant_name = pipeline.flow_config.get("tenant")
    tenant = TenantModel.from_name(tenant_name)

    pipeline.resolve_variables(tenant.variables)

    # Execute the pipeline (not actually running the steps, just a placeholder)
    with pipeline.runner() as runner:
        runner(pipeline)

    # Print the final result
    print("Pipeline execution completed.")
