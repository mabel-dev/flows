class SecretsProvider:
    """
    Base interface for secrets providers.

    Subclasses must implement open(), close(), and get().
    """

    def __init__(self) -> None:
        self._is_open = False

    def open(self, **kwargs) -> None:
        """
        Establishes a connection to the secrets backend.

        Parameters:
            kwargs: Backend-specific connection parameters.
        """
        raise NotImplementedError("open() must be implemented by subclass")

    def close(self) -> None:
        """
        Closes any resources or connections associated with the secrets backend.
        """
        raise NotImplementedError("close() must be implemented by subclass")

    def get(self, key: str) -> str:
        """
        Retrieve the value of a secret by key.

        Parameters:
            key: str
                The name or path of the secret to retrieve.

        Returns:
            The value of the secret as a string.
        """
        raise NotImplementedError("get() must be implemented by subclass")
