import os
import time
import json
import pytest
import importlib
import requests
from databricks_api import DatabricksAPI

from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs

# Import test configurations
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

@pytest.fixture(scope="module")
def databricks_client():
    """Initialize Databricks API client for validation"""
    config = Test_Configs.Configs["services"]["databricks"]
    return DatabricksAPI(
        host=config["host"],
        token=config["token"]
    )

def generate_sample_data(count=10):
    """Generate sample transaction data"""
    import random
    from datetime import datetime, timedelta
    
    transactions = []
    merchants = ["Walmart", "Amazon", "Target", "Costco", "Best Buy"]
    categories = ["Retail", "Electronics", "Groceries", "Home Goods"]
    
    for i in range(count):
        transactions.append({
            "transaction_id": f"txn_{i:06d}",
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "customer_id": f"cust_{random.randint(1000, 9999)}",
            "merchant": random.choice(merchants),
            "category": random.choice(categories)
        })
    
    return transactions

def setup_databricks_environment(client, config):
    """Set up the Databricks environment with sample data and required directories"""
    # 1. Create sample data in JSON format
    sample_data = generate_sample_data(config["expected_row_count"])
    sample_data_json = json.dumps(sample_data)
    
    # 2. Upload sample data to DBFS
    # First check if the directory exists
    try:
        client.dbfs.mkdirs(os.path.dirname(config["sample_data_path"].replace("dbfs:", "")))
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise e
    
    # Upload the data
    client.dbfs.put(
        path=config["sample_data_path"].replace("dbfs:", ""),
        contents=sample_data_json,
        overwrite=True
    )
    
    # 3. Ensure output directory is clean
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
    except Exception as e:
        if "not found" not in str(e).lower():
            raise e
            
    # 4. Set up Unity Catalog if needed
    try:
        # Create catalog if it doesn't exist
        client.sql.execute(f"CREATE CATALOG IF NOT EXISTS {config['catalog']}")
        client.sql.execute(f"USE CATALOG {config['catalog']}")
        
        # Create schema if it doesn't exist
        client.sql.execute(f"CREATE SCHEMA IF NOT EXISTS {config['schema']}")
    except Exception as e:
        # If we don't have permissions, we'll assume it's already set up
        if "permission" not in str(e).lower():
            raise e

def cleanup_databricks_environment(client, config):
    """Clean up the Databricks environment after testing"""
    # 1. Drop the table if it exists
    try:
        client.sql.execute(f"DROP TABLE IF EXISTS {config['catalog']}.{config['schema']}.{config['table']}")
    except Exception as e:
        print(f"Warning: Could not drop table: {e}")
    
    # 2. Remove the sample data
    try:
        client.dbfs.delete(
            path=config["sample_data_path"].replace("dbfs:", ""),
            recursive=False
        )
    except Exception as e:
        print(f"Warning: Could not delete sample data: {e}")
    
    # 3. Remove the output directory
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
    except Exception as e:
        print(f"Warning: Could not delete output directory: {e}")

@pytest.mark.databricks
@pytest.mark.pipeline
def test_databricks_pipeline_phase1(request, databricks_client):
    """
    Test Phase 1 of the Databricks pipeline using Ardent's run_model approach:
    - Set up Databricks environment with sample data
    - Run model to create and execute notebook
    - Validate results via direct checks against Databricks
    - Clean up the environment
    """
    config = Test_Configs.Configs["services"]["databricks"]
    
    # Store test metadata
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    test_steps = [
        {
            "name": "Environment Setup",
            "description": "Set up Databricks environment with sample data",
            "status": "did not reach",
            "Result_Message": ""
        },
        {
            "name": "Model Configuration",
            "description": "Set up Databricks model configuration",
            "status": "did not reach",
            "Result_Message": ""
        },
        {
            "name": "Model Execution",
            "description": "Run model to create and execute notebook",
            "status": "did not reach",
            "Result_Message": ""
        },
        {
            "name": "Data Validation",
            "description": "Verify data quality and table registration",
            "status": "did not reach",
            "Result_Message": ""
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))
    
    try:
        # Step 1: Set up Databricks environment
        setup_databricks_environment(databricks_client, config)
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "Databricks environment setup completed successfully"
        
        # Step 2: Set up model configuration
        config_results = set_up_model_configs(Configs=Test_Configs.Configs)
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "Model configuration completed successfully"
        
        # Step 3: Run model
        start_time = time.time()
        result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "Model execution completed successfully"
        
        # Step 4: Validate Results - Critical for evaluation
        # Check if table exists
        table_exists = databricks_client.sql.execute_and_fetch(
            f"SHOW TABLES IN {config['catalog']}.{config['schema']}"
        )
        assert any(row["tableName"] == config["table"] for row in table_exists), \
            f"Table {config['catalog']}.{config['schema']}.{config['table']} not found"
        
        # Verify row count
        row_count = databricks_client.sql.execute_and_fetch(
            f"SELECT COUNT(*) as count FROM {config['catalog']}.{config['schema']}.{config['table']}"
        )[0]["count"]
        assert row_count == config["expected_row_count"], \
            f"Expected {config['expected_row_count']} rows, got {row_count}"
        
        # Verify schema structure
        schema_check = databricks_client.sql.execute_and_fetch(
            f"DESCRIBE TABLE {config['catalog']}.{config['schema']}.{config['table']}"
        )
        column_names = [row["col_name"] for row in schema_check]
        expected_columns = ["transaction_id", "timestamp", "amount", "customer_id", "merchant", "category"]
        for col in expected_columns:
            assert col in column_names, f"Expected column '{col}' not found in table"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "All data validations passed"
        
    except Exception as e:
        # Find first non-passed step and mark as failed
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = str(e)
                break
        raise e
    
    finally:
        # Clean up Databricks environment
        cleanup_databricks_environment(databricks_client, config)
        
        # Clean up model configs
        remove_model_configs()
        
        # Store execution time
        request.node.user_properties.append(
            ("execution_time", time.time() - start_time)
        ) 