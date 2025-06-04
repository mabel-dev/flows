from flows.providers.tenants import TenantsProvider


class FileProvider(TenantsProvider):
    def __init__(self, file_path: str) -> None:
        super().__init__()
        self.file_path = file_path
        self.definitions = {}

    def open(self, **kwargs) -> None:
        try:
            with open(self.file_path, "r") as file:
                for line in file:
                    key, value = line.strip().split("=", 1)
                    self.definitions[key] = value
        except FileNotFoundError:
            raise ValueError(f"File not found: {self.file_path}")

    def get(self, key: str) -> str:
        return self.definitions.get(key, "")
