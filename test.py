import re
import sys
from typing import Any
from typing import Dict

import yaml
from pipeline_definition import PipelineDefinition  # your code above

sys.path.append(".")  # make sure internal.* is discoverable


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


with open("flows/example.yaml", "r") as f:
    pipeline_yaml = f.read()


pipeline = PipelineDefinition.from_yaml(pipeline_yaml)

tenant = pipeline.flow_config.get("tenant", "default")

with open(f"tenants/{tenant}/variables.yaml", "r") as f:
    variables_text = f.read()

variables = yaml.safe_load(variables_text)


for step in pipeline.steps:
    print(f"Resolving step: {step.name}")
    step.config = resolve_variables(step.config, variables)
    step.config.update(pipeline.flow_config)  # merge flow config into step config

data = None
for step in pipeline.steps:
    print(f"Executing step: {step.name}")
    data = step.func(data, step.config)
