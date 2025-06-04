from typing import Generator
from typing import Optional


class BaseOperator:
    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        raise NotImplementedError("execute must be overwritten")
