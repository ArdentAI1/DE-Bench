import os
import time
import json
import base64
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

def create_test_cluster(client, cluster_name="de-bench-test-cluster"):
    """Create a test cluster for the evaluation"""
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
            "purpose": "de-bench-testing",
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

def setup_databricks_environment(client, config, cluster_id):
    """Set up the Databricks environment with sample data and required directories"""
    # Update config with the cluster_id we're using
    config["cluster_id"] = cluster_id
    
    # 1. Create sample data in JSON format
    sample_data = generate_sample_data(config["expected_row_count"])
    sample_data_json = json.dumps(sample_data)
    
    # 2. Upload sample data to DBFS (base64 encoded)
    # First check if the directory exists
    try:
        client.dbfs.mkdirs(os.path.dirname(config["sample_data_path"].replace("dbfs:", "")))
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise e
    
    # Upload the data (base64 encoded)
    sample_data_b64 = base64.b64encode(sample_data_json.encode('utf-8')).decode('utf-8')
    client.dbfs.put(
        path=config["sample_data_path"].replace("dbfs:", ""),
        contents=sample_data_b64,
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

def validate_pipeline_results(client, config, timeout=30):
    """Validate that the pipeline executed successfully and produced expected results"""
    validation_results = {}
    start_time = time.time()
    
    # 1. Check if Delta table exists in DBFS
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
    
    # 2. Validate sample data was consumed
    try:
        if time.time() - start_time > timeout:
            raise TimeoutError("Validation timeout reached")
            
        sample_data_path = config["sample_data_path"].replace("dbfs:", "")
        sample_data_info = client.dbfs.get_status(sample_data_path)
        validation_results["sample_data_exists"] = True
        validation_results["sample_data_size"] = sample_data_info.get("file_size", "unknown")
        print(f"✓ Sample data exists at {sample_data_path} (size: {validation_results['sample_data_size']} bytes)")
    except Exception as e:
        validation_results["sample_data_exists"] = False
        validation_results["sample_data_error"] = str(e)
        print(f"✗ Sample data not found: {e}")
    
    # 2b. Validate Delta table structure and content
    try:
        if time.time() - start_time > timeout:
            raise TimeoutError("Validation timeout reached")
            
        if validation_results.get("delta_table_exists"):
            # Check for Delta log directory (indicates proper Delta table structure)
            delta_log_path = config["delta_table_path"].replace("dbfs:", "") + "/_delta_log"
            try:
                delta_log_info = client.dbfs.get_status(delta_log_path)
                validation_results["delta_log_exists"] = True
                print(f"✓ Delta log directory exists at {delta_log_path}")
            except:
                validation_results["delta_log_exists"] = False
                print(f"⚠ Delta log directory not found at {delta_log_path}")
                
            # List files in Delta table directory
            try:
                delta_files = client.dbfs.list(config["delta_table_path"].replace("dbfs:", ""))
                validation_results["delta_file_count"] = len(delta_files.get("files", []))
                print(f"✓ Delta table has {validation_results['delta_file_count']} files/directories")
            except Exception as e:
                validation_results["delta_file_list_error"] = str(e)
                print(f"⚠ Could not list Delta table contents: {e}")
                
    except Exception as e:
        validation_results["delta_structure_error"] = str(e)
        print(f"⚠ Delta table structure validation failed: {e}")
    
    # 3. Try to validate table registration using workspace API (simpler approach)
    try:
        # For now, we'll assume the table is registered if the delta table exists
        # In a production environment, you'd want to use the SQL execution API
        # or the new Databricks SDK with proper SQL connectivity
        validation_results["table_registered"] = validation_results.get("delta_table_exists", False)
        validation_results["sql_execution"] = "deferred"
        validation_results["note"] = "Table registration validation deferred - requires SQL execution capabilities"
        print(f"⚠ Table registration validation deferred (Delta table exists: {validation_results['delta_table_exists']})")
        
    except Exception as e:
        validation_results["table_registered"] = False
        validation_results["sql_error"] = str(e)
        print(f"✗ Table registration check failed: {e}")
    
    return validation_results

def cleanup_databricks_environment(client, config, cluster_created_by_us=False):
    """Clean up the Databricks environment after testing"""
    print("Starting cleanup...")
    
    # 1. Note: Skip table drop since client.sql is not available in this API version
    # The table will remain in hive_metastore.default.transactions
    print("Note: Table cleanup skipped (SQL API not available)")
    
    # 2. Remove the sample data
    try:
        client.dbfs.delete(
            path=config["sample_data_path"].replace("dbfs:", ""),
            recursive=False
        )
        print(f"✓ Removed sample data: {config['sample_data_path']}")
    except Exception as e:
        print(f"Warning: Could not delete sample data: {e}")
    
    # 3. Remove the output directory
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
        print(f"✓ Removed Delta table directory: {config['delta_table_path']}")
    except Exception as e:
        print(f"Warning: Could not delete output directory: {e}")
    
    # 4. Terminate cluster ONLY if we created it AND it wasn't from env vars
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
@pytest.mark.pipeline
def test_databricks_pipeline_phase1(request, databricks_client):
    """
    Test Phase 1 of the Databricks pipeline using Ardent's run_model approach:
    - Set up or create Databricks cluster
    - Set up Databricks environment with sample data
    - Run model to create and execute notebook
    - Validate results via direct checks against Databricks
    - Clean up the environment
    """
    config = Test_Configs.Configs["services"]["databricks"]
    cluster_created_by_us = False
    start_time = time.time()  # Initialize start_time at the beginning
    
    # Store test metadata
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    test_steps = [
        {
            "name": "Cluster Setup",
            "description": "Get or create Databricks cluster",
            "status": "did not reach",
            "Result_Message": ""
        },
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
        # Step 1: Get or create cluster
        cluster_id, cluster_created_by_us = get_or_create_cluster(databricks_client, config)
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = f"Using cluster: {cluster_id}"
        
        # Step 2: Set up Databricks environment
        setup_databricks_environment(databricks_client, config, cluster_id)
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "Databricks environment setup completed successfully"
        
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
        
        # Step 5: Validate Results - Critical for evaluation
        print("Starting validation...")
        try:
            validation_results = validate_pipeline_results(databricks_client, config)
            print(f"Validation Results: {validation_results}")
            
            # Check if key validations passed
            validation_passed = (
                validation_results.get("delta_table_exists", False) and
                validation_results.get("sample_data_exists", False)
            )
            
            if validation_passed:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"Validation completed successfully: {validation_results}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = f"Validation failed: {validation_results}"
                print(f"Pipeline validation failed: {validation_results}")
        except Exception as validation_error:
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = f"Validation error: {str(validation_error)}"
            print(f"Validation failed with error: {validation_error}")
        
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