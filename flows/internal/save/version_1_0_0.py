from typing import Generator
from typing import Optional

from flows.engine import BaseOperator


class SaveStep(BaseOperator):
    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        print(data)
        yield data, context
