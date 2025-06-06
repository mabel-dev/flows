class FlowDefinitionProvider:
    def __init__(self, **kwargs) -> None:
        pass

    def get(self, key: str) -> dict:
        raise NotImplementedError("get() must be implemented by subclass")
