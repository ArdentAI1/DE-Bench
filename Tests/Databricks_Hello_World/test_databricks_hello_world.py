import os
import time
import json
import pytest
import importlib
import requests
from databricks_api import DatabricksAPI

from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
from Environment.Databricks import (
    get_or_create_cluster,
    setup_databricks_environment,
    cleanup_databricks_environment,
    extract_warehouse_id_from_http_path,
    execute_sql_query
)

# Import test configurations
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

@pytest.fixture(scope="module")
def databricks_client():
    """Initialize Databricks API client for validation"""
    config = Test_Configs.Configs["services"]["databricks"]
    
    # Check if required environment variables are set
    if not config["host"] or not config["token"]:
        pytest.skip("Databricks credentials not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
    
    # Ensure host has proper format
    host = config["host"]
    if not host.startswith("https://"):
        host = f"https://{host}"
    
    return DatabricksAPI(
        host=host,
        token=config["token"]
    )



def validate_hello_world_results(client, config, timeout=60):
    """Validate that the simple Spark job ran successfully and produced our unique string"""
    validation_results = {}
    start_time = time.time()
    
    unique_message = config["unique_message"]
    test_id = config["test_id"]
    
    print(f"🔍 Validating Hello World test with unique message: {unique_message}")
    
    # 1. Check if Delta table files exist in DBFS
    try:
        if time.time() - start_time > timeout:
            raise TimeoutError("Validation timeout reached")
            
        dbfs_path = config["delta_table_path"].replace("dbfs:", "")
        dbfs_info = client.dbfs.get_status(dbfs_path)
        validation_results["delta_table_exists"] = True
        validation_results["delta_table_path"] = dbfs_path
        print(f"✓ Delta table exists at {dbfs_path}")
    except Exception as e:
        validation_results["delta_table_exists"] = False
        validation_results["delta_table_error"] = str(e)
        print(f"✗ Delta table not found: {e}")
        # If no Delta table, can't proceed with other validations
        validation_results["overall_valid"] = False
        return validation_results
    
    # 2. Check Delta table structure (basic validation)
    try:
        if time.time() - start_time > timeout:
            raise TimeoutError("Validation timeout reached")
            
        # Check for Delta log directory
        delta_log_path = config["delta_table_path"].replace("dbfs:", "") + "/_delta_log"
        try:
            delta_log_info = client.dbfs.get_status(delta_log_path)
            validation_results["delta_log_exists"] = True
            print(f"✓ Delta log directory exists")
        except:
            validation_results["delta_log_exists"] = False
            print(f"⚠ Delta log directory not found")
            
    except Exception as e:
        validation_results["delta_structure_error"] = str(e)
        print(f"⚠ Delta table structure validation failed: {e}")
    
    # 3. Try SQL validation if warehouse is available
    warehouse_id = extract_warehouse_id_from_http_path(config.get("http_path", ""))
    if warehouse_id:
        try:
            if time.time() - start_time > timeout:
                raise TimeoutError("Validation timeout reached")
            
            host = config["host"]
            token = config["token"]
            catalog = config["catalog"]
            schema = config["schema"]
            table = config["table"]
            
            print(f"🔍 Attempting SQL validation using warehouse {warehouse_id}")
            
            # Check if table exists and contains our unique message
            content_query = f"""
            SELECT * FROM {catalog}.{schema}.{table}
            WHERE CAST(message AS STRING) LIKE '%{unique_message}%'
            OR CAST(message AS STRING) = '{unique_message}'
            """
            
            result = execute_sql_query(host, token, warehouse_id, content_query, catalog, schema, timeout=30)
            
            if result["success"]:
                validation_results["sql_validation"] = True
                validation_results["unique_message_found"] = len(result["data"]) > 0
                validation_results["sql_row_count"] = result["row_count"]
                
                if validation_results["unique_message_found"]:
                    print(f"✓ Found unique message '{unique_message}' in table data")
                    validation_results["table_data"] = result["data"][:3]  # Store first few rows for debugging
                else:
                    print(f"⚠ Unique message '{unique_message}' not found in table data")
                    # Try a broader search
                    all_data_query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 5"
                    all_result = execute_sql_query(host, token, warehouse_id, all_data_query, catalog, schema, timeout=30)
                    if all_result["success"]:
                        validation_results["sample_table_data"] = all_result["data"]
                        print(f"Sample table data: {all_result['data']}")
            else:
                validation_results["sql_validation"] = False
                validation_results["sql_error"] = result["error"]
                print(f"⚠ SQL validation failed: {result['error']}")
                
        except Exception as e:
            validation_results["sql_validation"] = False
            validation_results["sql_error"] = str(e)
            print(f"⚠ SQL validation failed with error: {e}")
    else:
        validation_results["sql_validation"] = False
        validation_results["sql_error"] = "No warehouse ID available for SQL validation"
        print(f"⚠ No warehouse ID available, skipping SQL validation")
    
    # 4. Overall validation result
    validation_results["overall_valid"] = (
        validation_results.get("delta_table_exists", False) and
        (validation_results.get("unique_message_found", False) or not validation_results.get("sql_validation", False))
    )
    
    # If SQL validation worked, require unique message to be found
    if validation_results.get("sql_validation", False):
        validation_results["overall_valid"] = (
            validation_results["overall_valid"] and 
            validation_results.get("unique_message_found", False)
        )
    
    if validation_results["overall_valid"]:
        print(f"✓ Hello World validation passed! Unique message: {unique_message}")
    else:
        print(f"✗ Hello World validation failed")
    
    return validation_results


def update_test_step(test_steps, step_name, status, message):
    """Helper function to update both status and result message for a test step"""
    test_steps[step_name]["status"] = status
    test_steps[step_name]["Result_Message"] = message


@pytest.mark.databricks
@pytest.mark.hello_world
def test_databricks_hello_world(request, databricks_client):
    """
    Test Databricks Hello World with unique string validation:
    - Set up or create Databricks cluster
    - Set up Databricks environment
    - Run model to create and execute simple Spark job with unique string
    - Validate results by checking for unique string output
    - Clean up the environment
    """
    config = Test_Configs.Configs["services"]["databricks"]
    cluster_created_by_us = False
    start_time = time.time()
    
    # Store test metadata including unique identifiers
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    request.node.user_properties.append(("unique_message", config["unique_message"]))
    request.node.user_properties.append(("test_id", config["test_id"]))
    
    test_steps = {
        "Cluster Setup": {
            "order": 1,
            "description": "Get or create Databricks cluster",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Environment Setup": {
            "order": 2,
            "description": "Set up Databricks environment for Hello World test",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Model Configuration": {
            "order": 3,
            "description": "Set up Databricks model configuration",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Model Execution": {
            "order": 4,
            "description": "Run model to create and execute simple Spark job",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Data Validation": {
            "order": 5,
            "description": f"Verify Spark job ran and produced unique string: {config['unique_message'][:20]}...",
            "status": "did not reach",
            "Result_Message": ""
        }
    }
    request.node.user_properties.append(("test_steps", test_steps))
    
    try:
        # Step 1: Get or create cluster
        cluster_id, cluster_created_by_us = get_or_create_cluster(databricks_client, config)
        update_test_step(test_steps, "Cluster Setup", "passed", f"Using cluster: {cluster_id}")
        
        # Step 2: Set up Databricks environment
        setup_databricks_environment(databricks_client, config, cluster_id)
        update_test_step(test_steps, "Environment Setup", "passed", f"Environment setup completed for test ID: {config['test_id']}")
        
        # Step 3: Set up model configuration
        config_results = set_up_model_configs(Configs=Test_Configs.Configs)
        update_test_step(test_steps, "Model Configuration", "passed", "Model configuration completed successfully")
        
        # Step 4: Run model
        model_start_time = time.time()
        result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=Test_Configs.Configs
        )
        model_end_time = time.time()
        request.node.user_properties.append(("model_runtime", model_end_time - model_start_time))
        update_test_step(test_steps, "Model Execution", "passed", "Model execution completed successfully")
        
        # Step 5: Validate Results
        print(f"Starting validation for unique message: {config['unique_message']}")
        try:
            validation_results = validate_hello_world_results(databricks_client, config)
            print(f"Validation Results: {validation_results}")
            
            # Check if validation passed
            validation_passed = validation_results.get("overall_valid", False)
            
            # Store detailed validation information for reporting
            request.node.user_properties.append(("validation_details", validation_results))
            
            if validation_passed:
                update_test_step(test_steps, "Data Validation", "passed", 
                    f"✓ Hello World validation passed! "
                    f"Delta table: {validation_results.get('delta_table_exists', False)}, "
                    f"Unique message found: {validation_results.get('unique_message_found', 'N/A')}"
                )
            else:
                # Create detailed failure message
                failure_details = []
                if not validation_results.get("delta_table_exists", False):
                    failure_details.append("Delta table not found")
                if validation_results.get("sql_validation", False) and not validation_results.get("unique_message_found", False):
                    failure_details.append("Unique message not found in table")
                if validation_results.get("sql_error"):
                    failure_details.append(f"SQL error: {validation_results['sql_error']}")
                    
                update_test_step(test_steps, "Data Validation", "failed", f"✗ Validation failed: {', '.join(failure_details) or 'Unknown error'}")
                print(f"Hello World validation failed: {failure_details}")
                
        except Exception as validation_error:
            update_test_step(test_steps, "Data Validation", "failed", f"Validation error: {str(validation_error)}")
            print(f"Validation failed with error: {validation_error}")

        # Check all steps for failure
        failed_steps = []
        for step_name, step_data in test_steps.items():
            if step_data["status"] == "failed":
                failed_steps.append(step_name)
        
        if failed_steps:
            raise Exception(f"Test failed at one or more steps: {', '.join(failed_steps)}")
        
    except Exception as e:
        # Find first non-passed step and mark as failed
        for step_name, step_data in test_steps.items():
            if step_data["status"] == "did not reach":
                update_test_step(test_steps, step_name, "failed", str(e))
                break
        raise e
    
    finally:
        
        # Clean up Databricks environment
        cleanup_databricks_environment(databricks_client, config, cluster_created_by_us)
        
        # Clean up model configs
        remove_model_configs(Configs=Test_Configs.Configs)
        
        # Store execution time
        request.node.user_properties.append(
            ("execution_time", time.time() - start_time)
        )