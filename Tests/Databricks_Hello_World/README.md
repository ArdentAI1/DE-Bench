# Databricks Hello World Test

This test demonstrates a simple Spark job that validates the agent can create and execute Databricks notebooks.

## Test Overview

The test creates a Databricks notebook that:
1. Creates a simple Spark DataFrame with a unique test message
2. Adds a timestamp column with the current datetime  
3. Writes the result to a unique Delta table location
4. Registers the table in the hive_metastore catalog
5. Displays the DataFrame contents to confirm execution

## Unique Test Isolation

Each test run generates unique identifiers to avoid conflicts:
- **Unique Message**: `HELLO_WORLD_SUCCESS_{timestamp}_{uuid}`
- **Storage Path**: `dbfs:/tmp/hello_world_test_{test_id}`
- **Table Name**: `default.hello_world_test_{test_id}`

This ensures multiple test runs don't interfere with each other.

## Validation

The test performs simple but effective validation:

### File-Level Validation
- ✅ Delta table files exist in DBFS
- ✅ Delta log directory exists (proper Delta table structure)

### SQL-Level Validation (if warehouse available)
- ✅ **Table Registration**: Confirms table exists in the hive_metastore catalog
- ✅ **Unique Message**: Verifies the test's unique string appears in the table data

### Validation Logic
- **Pass**: Delta table exists AND (unique message found OR no SQL warehouse available)
- **Fail**: Missing Delta table OR (SQL available but unique message not found)

## Running the Test

```bash
pytest -sv -k "databricks_hello_world"
```

## Environment Requirements

Set the following environment variables:
- `DATABRICKS_HOST`: Your Databricks workspace hostname
- `DATABRICKS_TOKEN`: Your Databricks personal access token
- `DATABRICKS_CLUSTER_ID` (optional): Existing cluster ID to use
- `DATABRICKS_HTTP_PATH` (optional): SQL warehouse HTTP path for validation queries

## What This Test Validates

1. **Agent Capability**: Can the AI agent create and execute Spark jobs?
2. **Output Generation**: Did the specific test run produce verifiable output?
3. **No Conflicts**: Each test run is completely isolated
4. **Easy Debugging**: Unique identifiers make issues easy to trace

## Test Structure

- `Test_Configs.py`: Configuration with unique ID generation
- `test_databricks_hello_world.py`: Main test implementation with simple validation
- `README.md`: This documentation file

The test focuses on validating the agent's ability to create and execute Spark jobs rather than complex mathematical correctness.