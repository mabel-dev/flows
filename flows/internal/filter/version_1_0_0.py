from typing import Generator
from typing import Optional

from flows.engine import BaseOperator


class FilterStep(BaseOperator):
    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        yield data, context
