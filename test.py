from pipeline_definition import PipelineDefinition  # your code above
import sys
sys.path.append(".")  # make sure internal.* is discoverable

with open("flow.yaml", "r") as f:
    pipeline_yaml = f.read()

pipeline = PipelineDefinition.from_yaml(pipeline_yaml)

data = None
for step in pipeline.steps:
    print(f"Executing step: {step.name}")
    data = step.func(data, step.config, pipeline.flow_config)