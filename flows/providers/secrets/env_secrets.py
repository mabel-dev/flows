import os

from .base import SecretsProvider


class EnvSecretsProvider(SecretsProvider):
    """
    Secrets provider that pulls secrets from environment variables.
    """

    def open(self, **kwargs) -> None:
        # No setup required for environment variables, but flag as open
        self._is_open = True

    def close(self) -> None:
        self._is_open = False

    def get(self, key: str) -> str:
        if not self._is_open:
            raise RuntimeError("SecretsProvider must be opened before use.")
        try:
            return os.environ[key]
        except KeyError:
            raise KeyError(f"Secret '{key}' not found in environment.")
