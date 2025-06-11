# flows

Github Actions-style data pipline execution engine.

## Terminology

- **Flow**: A single executable instance of a _pipeline_.
- **Operator**: A reusable processing task.
- **Pipeline**: The sequence of _operators_ (tasks) to be performed.
- **Step**: A specific instance of an _operator_, configured and executed as part of a _flow_.
- **Tenant**: A permission boundary for access to resources.

## Example Pipeline Definition

Below is an example of a pipeline definition in YAML format:

~~~yaml
name: user_data_pipeline
tenant: acme_corp

schema:
  - name: name
    description: Full name of the user
    type: varchar
    expectations:
      not_null: true
      min_length: 2

  - name: age
    description: Age of the user
    type: integer
    expectations:
      min: 0

steps:
  - name: load_data
    uses: internal/read@1.0.0
    config:
      path: "gs://data.csv"

  - name: filter_data
    uses: internal/filter@latest
    config:
      conditions:
        - [["length", ">=", 4], ["status", "==", "approved"]]
        - [["is_published", "==", true]]

  - name: save_results
    uses: internal/save@1.0.0
    config:
      endpoint: "https://{{ environment.HOST }}/upload"
      username: "{{ secrets.API_USER }}"
      password: "{{ secrets.API_PASSWORD }}"
~~~