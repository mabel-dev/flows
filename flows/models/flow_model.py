import importlib
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import yaml

from flows.internal import get_step
from flows.utils.variable_resolver import variable_resolver


class PipelineStep:
    """
    A single step in a pipeline.

    Parameters:
        name: str
            Unique name of the step within the pipeline.
        uses: str
            The import path of the function to execute.
        config: Dict
            Step-specific configuration.
        func: Callable
            Resolved Python callable for execution.
    """

    def __init__(self, name: str, uses: str, config: Dict, func: Callable):
        self.name = name
        self.uses = uses
        self.config = config
        self.func = func


class FlowModel:
    """
    Represents a parsed pipeline definition.

    Parameters:
        steps: List[PipelineStep]
            Ordered list of steps to execute.
        flow_config: Dict
            Static metadata and schema for the pipeline.
        imports: List[str]
            List of module paths required for resolution.
    """

    def __init__(
        self, steps: List[PipelineStep], flow_config: Dict, imports: Optional[List[str]] = None
    ):
        self.steps = steps
        self.flow_config = flow_config
        self.imports = imports or []

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlowModel":
        imports = data.get("imports", [])

        flow_config = {
            "name": data.get("name"),
            "description": data.get("description"),
            "tenant": data.get("tenant"),
            "classification": data.get("classification"),
            "access_model": data.get("access_model"),
            "trigger": data.get("trigger"),
            "schema": data.get("schema", []),
        }

        # preload imported modules
        available: Dict[str, Callable] = {}
        for path in imports:
            module_name, attr_name = path.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            available[path] = getattr(mod, attr_name)

        steps = []
        for step in data.get("steps", []):
            name = step["name"]
            uses = step["uses"]
            config = step.get("config", {})

            if uses in available:
                func = available[uses]
            else:
                version = "latest"
                module_name, attr_name = uses.rsplit("/", 1)
                if "@" in attr_name:
                    attr_name, version = attr_name.split("@", 1)

                step = get_step(attr_name, version)

            steps.append(PipelineStep(name=name, uses=uses, config=config, func=func))

        return cls(steps=steps, flow_config=flow_config, imports=imports)

    @classmethod
    def from_yaml(cls, yaml_text: str) -> "FlowModel":
        data = yaml.safe_load(yaml_text)
        return cls.from_dict(data)

    @classmethod
    def from_name(cls, flow_name: str) -> "FlowModel":
        if "." in flow_name:
            raise ValueError("Flow name should not contain dots. Use underscores instead.")
        with open(f"definitions/{flow_name}.yaml", "r") as f:
            pipeline_yaml = f.read()
        return cls.from_yaml(pipeline_yaml)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "imports": self.imports,
            "steps": [
                {"name": step.name, "uses": step.uses, "config": step.config} for step in self.steps
            ],
            **self.flow_config,
        }

    def resolve_variables(self, variables: Dict[str, Dict[str, Any]]) -> "FlowModel":
        """
        Resolve template variables in the pipeline steps' configurations.

        Parameters:
            variables: Dict[str, Dict[str, Any]]
                Mapping of namespaces (e.g., 'secrets', 'environment') to key-value pairs.

        Returns:
            A new FlowModel with resolved configurations.
        """
        for step in self.steps:
            print(f"Resolving step: {step.name}")
            step.config = variable_resolver(step.config, variables)
            step.config.update(self.flow_config)
