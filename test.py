import re
import sys
from typing import Any
from typing import Dict

from pipeline_definition import PipelineDefinition  # your code above

sys.path.append(".")  # make sure internal.* is discoverable


variables = {
    "secrets": {"API_USER": "admin", "API_PASSWORD": "hunter2"},
    "environment": {"HOST": "prod.service.com"},
}

_PATTERN = re.compile(r"\{\{\s*(\w+)\.(\w+)\s*\}\}")


def resolve_variables(config: Any, variables: Dict[str, Dict[str, Any]]) -> Any:
    """
    Recursively resolve template variables in a config structure.

    Parameters:
        config: Any
            Input config (dict, list, str, etc.)
        variables: Dict[str, Dict[str, Any]]
            Mapping of namespaces (e.g., 'secrets', 'environment') to key-value pairs.

    Returns:
        The config with placeholders replaced by values from variables.
    """

    if isinstance(config, dict):
        return {k: resolve_variables(v, variables) for k, v in config.items()}

    elif isinstance(config, list):
        return [resolve_variables(i, variables) for i in config]

    elif isinstance(config, str):

        def replacer(match):
            namespace, key = match.groups()
            if namespace not in variables or key not in variables[namespace]:
                raise KeyError(f"Missing variable: {namespace}.{key}")
            return str(variables[namespace][key])

        return _PATTERN.sub(replacer, config)

    else:
        return config  # e.g., int, float, None, etc.


with open("flow.yaml", "r") as f:
    pipeline_yaml = f.read()


pipeline = PipelineDefinition.from_yaml(pipeline_yaml)

for step in pipeline.steps:
    print(f"Resolving step: {step.name}")
    step.config = resolve_variables(step.config, variables)

data = None
for step in pipeline.steps:
    print(f"Executing step: {step.name}")
    data = step.func(data, step.config, pipeline.flow_config)
