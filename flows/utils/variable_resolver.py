import re
from typing import Any
from typing import Dict

from flows.providers.secrets import get_secrets_provider

_PATTERN = re.compile(r"\{\{\s*(\w+)\.(\w+)\s*\}\}")

secrets_provider = get_secrets_provider()


def variable_resolver(config: Any, variables: Dict[str, Dict[str, Any]]) -> Any:
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
        return {k: variable_resolver(v, variables) for k, v in config.items()}

    elif isinstance(config, list):
        return [variable_resolver(i, variables) for i in config]

    elif isinstance(config, str):

        def replacer(match):
            namespace, key = match.groups()
            if namespace not in variables or key not in variables[namespace]:
                raise KeyError(f"Missing variable: {namespace}.{key}")
            if namespace == "secrets":
                # Get secrets from the secrets provider
                secret_name = variables[namespace][key]
                return secrets_provider.get(secret_name)
            return str(variables[namespace][key])

        return _PATTERN.sub(replacer, config)

    else:
        return config  # e.g., int, float, None, etc.
