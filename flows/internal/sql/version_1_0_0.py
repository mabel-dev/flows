from typing import Generator
from typing import Optional

from flows.engine import BaseOperator


class SqlStep(BaseOperator):
    def __init__(self, config: dict, flow_config: dict):
        self.config = config
        self.flow_config = flow_config
        if "statement" not in self.config:
            raise ValueError("SQL step requires a 'statement' in its configuration.")
        self.statement = self.config["statement"]
        if not isinstance(self.statement, str):
            raise ValueError("SQL step 'statement' must be a string.")

        super().__init__()

    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        import opteryx

        data = opteryx.query(self.statement)
        for row in data:
            yield row.to_dict(), context
