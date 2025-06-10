# flows

Github Actions style data pipline execution engine.



Terminology:

- **Flow**: a single executable instance of a _pipeline_
- **Operator**: defines a reusable processing task
- **Pipeline**: defines the sequence of _operators_ (tasks) to be performed.
- **Step**: a specific instance of an _operator_, configured and executed as part of a _flow_.
- **Tenant**: a permission boundary for access to resources

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