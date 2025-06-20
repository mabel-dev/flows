from typing import Any
from typing import Dict
from typing import List

from flows.engine import Flow
from flows.engine.base_operator import BaseOperator
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
        step: BaseOperator
            Resolved Python class for execution.
    """

    def __init__(self, name: str, uses: str, config: Dict, operator: BaseOperator):
        self.name = name
        self.uses = uses
        self.config = config
        self.operator = operator


class FlowModel:
    """
    Represents a parsed pipeline definition.

    Parameters:
        steps: List[PipelineStep]
            Ordered list of steps to execute.
        flow_config: Dict
            Static metadata and schema for the pipeline.
    """

    def __init__(self, steps: List[PipelineStep], flow_config: Dict[str, Any]):
        self.steps = steps
        self.flow_config = flow_config

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlowModel":
        flow_config = {
            "name": data.get("name"),
            "description": data.get("description"),
            "tenant": data.get("tenant"),
            "classification": data.get("classification"),
            "access_model": data.get("access_model"),
            "trigger": data.get("trigger"),
            "schema": data.get("schema", []),
        }

        steps = []
        for step in data.get("steps", []):
            name = step["name"]
            uses = step["uses"]
            config = step.get("config", {})

            version = "latest"
            module_name, attr_name = uses.rsplit("/", 1)

            if module_name != "internal":
                raise ValueError(f"Invalid module name: {module_name}. Only 'internal' is allowed.")

            if "@" not in attr_name:
                raise ValueError(
                    f"Step '{name}' must specify a version using '@'. Found: {attr_name}"
                )

            attr_name, version = attr_name.split("@", 1)

            operator = get_step(attr_name, version)

            steps.append(PipelineStep(name=name, uses=uses, config=config, operator=operator))

        return cls(steps=steps, flow_config=flow_config)

    @classmethod
    def from_name(cls, flow_name: str) -> "FlowModel":
        if "." in flow_name:
            raise ValueError("Flow name should not contain dots. Use underscores instead.")

        from flows.providers.flow_definitions import get_flow_definitions_provider

        flow_definitions_provider = get_flow_definitions_provider()
        flow_definition = flow_definitions_provider.get(flow_name)

        return cls.from_dict(flow_definition)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "steps": [
                {"name": step.name, "uses": step.uses, "config": step.config} for step in self.steps
            ],
            **self.flow_config,
        }

    def resolve_variables(self, variables: Dict[str, Dict[str, Any]]):
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

    def runner(self) -> Flow:
        """
        Create a Flow object from the pipeline steps which can then be used to execute the pipeline.
        """
        from flows.engine import EndOperator

        flow = Flow()
        previous_step = None
        for step in self.steps:
            flow.add_step(name=step.name, operator=step.operator(**step.config))
            if previous_step:
                flow.link_steps(previous_step, step.name)
            previous_step = step.name
        flow.add_step(name="end", operator=EndOperator())  # Add an end step
        if previous_step:
            flow.link_steps(previous_step, "end")

        return flow
