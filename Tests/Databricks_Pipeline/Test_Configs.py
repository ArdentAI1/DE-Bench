import os
from dotenv import load_dotenv

load_dotenv()

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

# Test configuration for Phase 1: Batch Ingestion
User_Input = """
Create a Databricks notebook that:
1. Uses spark.read.json() to read sample transaction data from dbfs:/tmp/sample_transactions.json
2. Writes to Delta table at dbfs:/pipelines/raw/transactions
3. Registers the table as default.transactions in the hive_metastore catalog
4. Validates row count matches expected sample size (10 records)
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
            "table": "transactions",
            
            # Test parameters
            "expected_row_count": 10,
            
            # Paths
            "notebook_path": "/Shared/de_bench/batch_ingest",
            "delta_table_path": "dbfs:/pipelines/raw/transactions",
            "sample_data_path": "dbfs:/tmp/sample_transactions.json"
        }
    }
} 