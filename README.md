# flows

Github Actions style data pipline execution engine.



Terminology:

- **Flow**: a single executable instance of a _pipeline_
- **Operator**: defines a reusable processing task
- **Pipeline**: defines the sequence of _operators_ (tasks) to be performed.
- **Step**: a specific instance of an _operator_, configured and executed as part of a _flow_.
- **Tenant**: a permission boundary for access to resources
