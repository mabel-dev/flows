from typing import Optional


class BaseOperator:
    def execute(self, data: Optional[dict] = None, context: dict = None) -> tuple:
        raise NotImplementedError("execute must be overwritten")
