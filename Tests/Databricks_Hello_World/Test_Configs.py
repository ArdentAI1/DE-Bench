import os
import uuid
import time
from dotenv import load_dotenv

load_dotenv()

# Generate unique identifiers for this test run to avoid conflicts
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]
test_id = f"{test_timestamp}_{test_uuid}"
unique_message = f"HELLO_WORLD_SUCCESS_{test_id}"
table_suffix = test_id.replace('-', '_')  # SQL-safe table name

# Required environment variables for Databricks testing
REQUIRED_ENV_VARS = {
    "DATABRICKS_HOST": "Databricks workspace hostname (e.g., dbc-abc123-def.cloud.databricks.com)",
    "DATABRICKS_TOKEN": "Databricks personal access token"
}

OPTIONAL_ENV_VARS = {
    "DATABRICKS_CLUSTER_ID": "Existing cluster ID (if not provided, will create one)",
    "DATABRICKS_HTTP_PATH": "SQL warehouse HTTP path (defaults to /sql/1.0/warehouses/default)"
}

# Validate required environment variables
missing_vars = []
for var, description in REQUIRED_ENV_VARS.items():
    if not os.getenv(var):
        missing_vars.append(f"  {var}: {description}")

if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables for Databricks testing:\n" + 
        "\n".join(missing_vars) +
        f"\n\nOptional variables:\n" + 
        "\n".join([f"  {var}: {desc}" for var, desc in OPTIONAL_ENV_VARS.items()])
    )

# Test configuration for simple Spark job validation
User_Input = f"""
Create a Databricks notebook that:
1. Creates a simple Spark DataFrame with a single row containing the message: "{unique_message}"
2. Adds a timestamp column with the current datetime
3. Writes the result to a Delta table at dbfs:/tmp/hello_world_test_{test_id}
4. Registers the table as default.hello_world_test_{table_suffix} in the hive_metastore catalog
5. Displays the DataFrame contents to confirm the job ran successfully
6. Returns a success message containing: "{unique_message}"
"""

# Test configuration parameters
Configs = {
    "services": {
        "databricks": {
            # Workspace configuration
            "host": os.getenv("DATABRICKS_HOST"),
            "token": os.getenv("DATABRICKS_TOKEN"),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
            "http_path": os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/default"),
            
            # Unity Catalog configuration
            "catalog": "hive_metastore",
            "schema": "default",
            "table": f"hello_world_test_{table_suffix}",
            
            # Test parameters
            "unique_message": unique_message,
            "test_id": test_id,
            
            # Paths
            "notebook_path": f"/Shared/de_bench/hello_world_test_{test_id}",
            "delta_table_path": f"dbfs:/tmp/hello_world_test_{test_id}"
        }
    }
}