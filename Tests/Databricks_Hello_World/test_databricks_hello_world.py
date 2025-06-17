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

def extract_warehouse_id_from_http_path(http_path):
    """Extract warehouse ID from HTTP path like /sql/1.0/warehouses/abc123"""
    if "/warehouses/" in http_path:
        return http_path.split("/warehouses/")[-1]
    return None

def execute_sql_query(host, token, warehouse_id, sql_query, catalog="hive_metastore", schema="default", timeout=60):
    """Execute SQL query using Databricks SQL Statement Execution API"""
    
    # Ensure host has proper format
    if not host.startswith("https://"):
        host = f"https://{host}"
    
    # Prepare request payload for SQL execution
    payload = {
        "warehouse_id": warehouse_id,
        "catalog": catalog,
        "schema": schema,
        "statement": sql_query,
        "wait_timeout": f"{timeout}s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE"
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Execute SQL statement
        response = requests.post(
            f"{host}/api/2.0/sql/statements/",
            headers=headers,
            json=payload,
            timeout=timeout + 10  # Add buffer for HTTP timeout
        )
        
        if response.status_code != 200:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text}",
                "status_code": response.status_code
            }
        
        result = response.json()
        
        # Check if statement executed successfully
        if result.get("status", {}).get("state") == "SUCCEEDED":
            return {
                "success": True,
                "data": result.get("result", {}).get("data_array", []),
                "schema": result.get("manifest", {}).get("schema", {}),
                "row_count": result.get("manifest", {}).get("total_row_count", 0)
            }
        elif result.get("status", {}).get("state") == "PENDING":
            return {
                "success": False,
                "error": f"Query timed out after {timeout} seconds",
                "state": "PENDING"
            }
        else:
            return {
                "success": False,
                "error": f"Query failed with state: {result.get('status', {}).get('state')}",
                "details": result
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": f"Request failed: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}"
        }

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

def create_test_cluster(client, cluster_name="de-bench-hello-world-cluster"):
    """Create a test cluster for the Hello World test"""
    cluster_config = {
        "cluster_name": cluster_name,
        "spark_version": "13.3.x-scala2.12",  # Latest LTS version
        "node_type_id": "m5.large",  # Small, supported instance type
        "num_workers": 0,  # Single node cluster to minimize cost
        "autotermination_minutes": 30,  # Auto-terminate after 30 minutes
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]"
        },
        "aws_attributes": {
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100  # Minimum size in GB
        },
        "custom_tags": {
            "purpose": "de-bench-hello-world-testing",
            "auto-created": "true"
        }
    }
    
    print(f"Creating test cluster: {cluster_name}")
    response = client.cluster.create_cluster(**cluster_config)
    cluster_id = response["cluster_id"]
    
    # Wait for cluster to start
    print(f"Waiting for cluster {cluster_id} to start...")
    max_wait = 600  # 10 minutes timeout
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        cluster_info = client.cluster.get_cluster(cluster_id)
        state = cluster_info["state"]

        print(f"Cluster {cluster_id} is in state: {state}")
        
        if state == "RUNNING":
            print(f"Cluster {cluster_id} is now running")
            return cluster_id
        elif state in ["ERROR", "TERMINATED"]:
            raise Exception(f"Cluster failed to start. State: {state}")
        
        time.sleep(5)
    
    raise Exception(f"Cluster {cluster_id} failed to start within {max_wait} seconds")

def get_or_create_cluster(client, config, timeout=600):
    """Get existing cluster or create a new one if needed"""
    cluster_id = config.get("cluster_id")
    
    # If cluster_id is provided, check if it exists and is running
    if cluster_id:
        try:
            cluster_info = client.cluster.get_cluster(cluster_id)
            state = cluster_info["state"]
            
            if state == "RUNNING":
                print(f"Using existing running cluster: {cluster_id}")
                return cluster_id, False  # False = not created by us
            elif state == "TERMINATED":
                print(f"Starting existing cluster: {cluster_id}")
                client.cluster.start_cluster(cluster_id)
                
                # Wait for it to start
                max_wait = min(timeout, 300)  # Use provided timeout or 5 minutes max
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    cluster_info = client.cluster.get_cluster(cluster_id)
                    if cluster_info["state"] == "RUNNING":
                        return cluster_id, False
                    time.sleep(10)
                
                raise Exception(f"Cluster {cluster_id} failed to start")
            else:
                print(f"Cluster {cluster_id} is in state {state}, creating new cluster")
                return create_test_cluster(client), True  # True = created by us
                
        except Exception as e:
            print(f"Error with cluster {cluster_id}: {e}. Creating new cluster.")
            return create_test_cluster(client), True
    
    # No cluster_id provided, create a new one
    print("No cluster ID provided, creating new test cluster")
    return create_test_cluster(client), True

def setup_databricks_environment(client, config, cluster_id):
    """Set up the Databricks environment for Hello World test"""
    # Update config with the cluster_id we're using
    config["cluster_id"] = cluster_id
    
    # Ensure output directory is clean
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
        print(f"Cleaned up existing output directory: {config['delta_table_path']}")
    except Exception as e:
        if "not found" not in str(e).lower():
            print(f"Warning during cleanup: {e}")

def cleanup_databricks_environment(client, config, cluster_created_by_us=False):
    """Clean up the Databricks environment after testing"""
    print("Starting cleanup...")
    
    # 1. Try to drop the table if we have SQL capabilities
    try:
        warehouse_id = extract_warehouse_id_from_http_path(config.get("http_path", ""))
        if warehouse_id:
            host = config["host"]
            token = config["token"]
            catalog = config["catalog"]
            schema = config["schema"]
            table = config["table"]
            
            drop_table_query = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
            result = execute_sql_query(host, token, warehouse_id, drop_table_query, catalog, schema)
            
            if result["success"]:
                print(f"✓ Dropped table: {catalog}.{schema}.{table}")
            else:
                print(f"Warning: Could not drop table: {result['error']}")
        else:
            print("Note: Table cleanup skipped (no warehouse ID available)")
    except Exception as e:
        print(f"Warning: Could not drop table: {e}")
    
    # 2. Remove the output directory
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
        print(f"✓ Removed Delta table directory: {config['delta_table_path']}")
    except Exception as e:
        print(f"Warning: Could not delete output directory: {e}")
    
    # 3. Terminate cluster ONLY if we created it AND it wasn't from env vars
    cluster_from_env = os.getenv("DATABRICKS_CLUSTER_ID") is not None
    if cluster_created_by_us and config.get("cluster_id") and not cluster_from_env:
        try:
            print(f"Terminating test cluster: {config['cluster_id']}")
            client.cluster.delete_cluster(config["cluster_id"])
            print(f"✓ Terminated cluster: {config['cluster_id']}")
        except Exception as e:
            print(f"Warning: Could not terminate cluster: {e}")
    else:
        if cluster_from_env:
            print(f"Cluster cleanup skipped (cluster from env var: {config.get('cluster_id')})")
        else:
            print(f"Cluster cleanup skipped (using existing cluster: {config.get('cluster_id')})")
    
    print("Cleanup completed.")

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
    
    test_steps = [
        {
            "name": "Cluster Setup",
            "description": "Get or create Databricks cluster",
            "status": "did not reach",
            "Result_Message": ""
        },
        {
            "name": "Environment Setup",
            "description": "Set up Databricks environment for Hello World test",
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
            "description": "Run model to create and execute simple Spark job",
            "status": "did not reach",
            "Result_Message": ""
        },
        {
            "name": "Data Validation",
            "description": f"Verify Spark job ran and produced unique string: {config['unique_message'][:20]}...",
            "status": "did not reach",
            "Result_Message": ""
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))
    
    try:
        # Step 1: Get or create cluster
        cluster_id, cluster_created_by_us = get_or_create_cluster(databricks_client, config)
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = f"Using cluster: {cluster_id}"
        
        # Step 2: Set up Databricks environment
        setup_databricks_environment(databricks_client, config, cluster_id)
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = f"Environment setup completed for test ID: {config['test_id']}"
        
        # Step 3: Set up model configuration
        config_results = set_up_model_configs(Configs=Test_Configs.Configs)
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "Model configuration completed successfully"
        
        # Step 4: Run model
        model_start_time = time.time()
        result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=Test_Configs.Configs
        )
        model_end_time = time.time()
        request.node.user_properties.append(("model_runtime", model_end_time - model_start_time))
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "Model execution completed successfully"
        
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
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = (
                    f"✓ Hello World validation passed! "
                    f"Delta table: {validation_results.get('delta_table_exists', False)}, "
                    f"Unique message found: {validation_results.get('unique_message_found', 'N/A')}"
                )
            else:
                test_steps[4]["status"] = "failed" 
                
                # Create detailed failure message
                failure_details = []
                if not validation_results.get("delta_table_exists", False):
                    failure_details.append("Delta table not found")
                if validation_results.get("sql_validation", False) and not validation_results.get("unique_message_found", False):
                    failure_details.append("Unique message not found in table")
                if validation_results.get("sql_error"):
                    failure_details.append(f"SQL error: {validation_results['sql_error']}")
                    
                test_steps[4]["Result_Message"] = f"✗ Validation failed: {', '.join(failure_details) or 'Unknown error'}"
                print(f"Hello World validation failed: {failure_details}")
                
        except Exception as validation_error:
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = f"Validation error: {str(validation_error)}"
            print(f"Validation failed with error: {validation_error}")

        # Check all steps for failure
        failed_steps = []
        for step in test_steps:
            if step["status"] == "failed":
                failed_steps.append(step)
        
        if failed_steps:
            raise Exception(f"Test failed at one or more steps: {', '.join([str(step) for step in failed_steps])}")
        
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
        cleanup_databricks_environment(databricks_client, config, cluster_created_by_us)
        
        # Clean up model configs
        remove_model_configs(Configs=Test_Configs.Configs)
        
        # Store execution time
        request.node.user_properties.append(
            ("execution_time", time.time() - start_time)
        )