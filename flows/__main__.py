# isort: skip_file

import sys

sys.path.append(".")

from flows.models import FlowModel
from flows.models import TenantModel


FLOW_NAME = "example"


if __name__ == "__main__":
    # Load the pipeline definition from YAML
    pipeline = FlowModel.from_name(FLOW_NAME)

    tenant_name = pipeline.flow_config.get("tenant")
    tenant = TenantModel.from_name(tenant_name)

    pipeline.resolve_variables(tenant.variables)

    # Execute the pipeline (not actually running the steps, just a placeholder)
    data = None
    for step in pipeline.steps:
        print(f"Executing step: {step.name}")
        data = step.operator.execute(data, step.config)

    # Print the final result
    print("Pipeline execution completed. Final result:")
    print(data)
