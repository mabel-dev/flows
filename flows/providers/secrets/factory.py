import os
from typing import Dict
from typing import Type

from flows.providers.secrets import SecretsProvider

from .env_secrets import EnvSecretsProvider

# from hashi_secrets import HashiSecretsProvider  # example placeholder

# Registry of known providers
_REGISTERED_PROVIDERS: Dict[str, Type[SecretsProvider]] = {
    "env": EnvSecretsProvider,
    # "hashi": HashiSecretsProvider,
    # "aws": AwsSecretsProvider,
    # "gcp": GcpSecretsProvider,
}


def get_secrets_provider(**kwargs) -> SecretsProvider:
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
    backend = kwargs.get("backend") if "backend" in kwargs else os.getenv("SECRETS_BACKEND")
    if backend is None:
        env = os.getenv("ENVIRONMENT", "local").lower()
        if env in ("dev", "local"):
            backend = "env"

    if backend not in _REGISTERED_PROVIDERS:
        raise ValueError(f"Unsupported secrets backend: '{backend}'")

    provider_class = _REGISTERED_PROVIDERS[backend]
    provider = provider_class()
    provider.open(**kwargs)
    return provider
