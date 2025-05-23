# internal/fake_steps.py

def load_csv(data, config, flow_config):
    print(f"[load_csv] loading from {config['path']}")
    return [{"name": "Alice"}, {"name": "Bob"}]

def filter_names(data, config, flow_config):
    print(f"[filter_names] keeping names with min length {config['min_length']}")
    return [row for row in data if len(row["name"]) >= config["min_length"]]

def save_json(data, config, flow_config):
    print(f"[save_json] saving to {config['path']}")
    return True

# internal/sql_steps.py

def load_sql(data, config, flow_config):
    """
    Execute a SQL statement against the input data.

    Parameters:
        data: any
            Input table or dataset to query
        config: Dict
            Must contain a `sql` key with the SQL query
        flow_config: Dict
            Immutable flow-level metadata and schema
    Returns:
        Transformed dataset
    """
    sql = config["sql"]
    print("[load_sql] Executing SQL:\n" + sql.strip())

    # Stubbed for example - replace with actual SQL engine call
    # e.g. run_sql(sql, data, schema=flow_config["schema"])
    return [
        {"region": "EMEA", "total_sales": 123456.78},
        {"region": "APAC", "total_sales": 987654.32},
    ]