import os
from typing import Dict
from typing import Type

from flows.providers.flow_definitions import FlowDefinitionProvider
from flows.providers.flow_definitions.file_provider import FileProvider

# Registry of known providers
_REGISTERED_PROVIDERS: Dict[str, Type[FlowDefinitionProvider]] = {
    "file": FileProvider,
    # "aws": AwsSecretsProvider,
    # "gcp": GcpSecretsProvider,
}


def get_flow_definitions_provider(**kwargs) -> FlowDefinitionProvider:
    """
    Factory method for obtaining the appropriate SecretsProvider implementation.

    Environment detection rules:
    - If SECRETS_BACKEND is set, it takes priority.
    - Otherwise, if ENVIRONMENT is set to 'dev' or 'local', use EnvSecretsProvider.
    - Fallback default is 'hashi'.

    Parameters:
        kwargs: Additional arguments passed to provider.open()

    Returns:
        An initialized and opened SecretsProvider instance.
    """
    backend = (
        kwargs.get("backend") if "backend" in kwargs else os.getenv("FLOW_DEFINITIONS_BACKEND")
    )
    if backend is None:
        env = os.getenv("ENVIRONMENT", "local").lower()
        if env in ("dev", "local"):
            backend = "file"

    if backend not in _REGISTERED_PROVIDERS:
        raise ValueError(f"Unsupported secrets backend: '{backend}'")

    provider_class = _REGISTERED_PROVIDERS[backend]
    provider = provider_class(**kwargs)
    return provider
