import os

import yaml

from flows.providers.flow_definitions import FlowDefinitionProvider


class FileProvider(FlowDefinitionProvider):
    def __init__(self, file_path: str = "test_files/definitions") -> None:
        super().__init__()
        self.file_path = file_path
        self.definitions = {}

        if file_path and not file_path.endswith(os.sep):
            self.file_path += os.sep

    def get(self, key: str) -> dict:
        with open(f"{self.file_path}{key}.yaml", "r") as file:
            return yaml.safe_load(file.read())
