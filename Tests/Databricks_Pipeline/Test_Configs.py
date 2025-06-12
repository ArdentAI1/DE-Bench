import os
from dotenv import load_dotenv

load_dotenv()

# Test configuration for Phase 1: Batch Ingestion
User_Input = """
Create a Databricks notebook that:
1. Uses spark.read.json() to read sample transaction data from dbfs:/tmp/sample_transactions.json
2. Writes to Delta table at dbfs:/pipelines/raw/transactions
3. Registers the table as bronze.transactions in Unity Catalog
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
            
            # Unity Catalog configuration
            "catalog": "sales_demo",
            "schema": "bronze",
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