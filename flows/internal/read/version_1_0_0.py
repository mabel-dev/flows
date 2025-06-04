from typing import Generator
from typing import Optional

import opteryx

from flows.internal.base import BaseOperator


class ReadStep(BaseOperator):
    def __init__(self, config: dict, flow_config: dict):
        self.config = config
        self.flow_config = flow_config
        super().__init__()

    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        data = opteryx.query("SELECT * FROM $planets")
        for row in data:
            yield row.to_dict(), context
