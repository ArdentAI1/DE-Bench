"""
Databricks Sanity Check Test

This test directly implements the Hello World functionality without using the AI model.
It serves as a control test to validate that:
1. Databricks connectivity works
2. The validation logic is sound
3. The environment setup is correct

This test should be run before the main AI test to ensure the infrastructure is working.
"""

import os
import time
import uuid
import pytest
import importlib
from databricks_api import DatabricksAPI

from Fixtures.Databricks.databricks_resources import databricks_resource

# Import test configurations
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Import validation function from main test
from Tests.Databricks_Agent_Hello_World.test_databricks_hello_world import validate_hello_world_results, update_test_step


def create_sanity_check_config():
    """Create a separate config for sanity check with unique identifiers"""
    base_config = Test_Configs.Configs["services"]["databricks"].copy()
    
    # Generate unique identifiers for sanity check
    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    sanity_test_id = f"sanity_{test_timestamp}_{test_uuid}"
    sanity_unique_message = f"SANITY_CHECK_SUCCESS_{sanity_test_id}"
    table_suffix = sanity_test_id.replace('-', '_')
    
    # Update paths and identifiers
    base_config.update({
        "test_id": sanity_test_id,
        "unique_message": sanity_unique_message,
        "table": f"hello_world_sanity_{table_suffix}",
        "notebook_path": f"/Shared/de_bench/hello_world_sanity_{sanity_test_id}",
        "delta_table_path": f"dbfs:/tmp/hello_world_sanity_{sanity_test_id}"
    })
    
    return base_config


def create_hello_world_job(client: DatabricksAPI, config: dict, cluster_id: str) -> str:
    """
    Create a simple Hello World job using Databricks Jobs API.
    Returns the job_id of the created job.
    """
    unique_message = config["unique_message"]
    catalog = config["catalog"]
    schema = config["schema"]
    table = config["table"]
    delta_table_path = config["delta_table_path"]
    
    # Create a simple PySpark script
    pyspark_script = f'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
import time

# Initialize Spark session
spark = SparkSession.builder.appName("HelloWorldSanityCheck").getOrCreate()

print("🚀 Starting Hello World Sanity Check")

try:
    # Create a simple DataFrame with the unique message
    df = spark.createDataFrame([("{unique_message}",)], ["message"])
    
    # Add a timestamp column
    df = df.withColumn("timestamp", current_timestamp())
    df = df.withColumn("test_type", lit("sanity_check"))
    
    print(f"✓ Created DataFrame with message: {unique_message}")
    df.show()
    
    # Write to Delta table
    df.write.format("delta").mode("overwrite").save("{delta_table_path}")
    print(f"✓ Written to Delta table: {delta_table_path}")
    
    # Register table in hive_metastore
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")
    spark.sql(f"CREATE TABLE {catalog}.{schema}.{table} USING DELTA LOCATION '{delta_table_path}'")
    print(f"✓ Registered table: {catalog}.{schema}.{table}")
    
    # Validate the write
    result_df = spark.read.format("delta").load("{delta_table_path}")
    row_count = result_df.count()
    print(f"✓ Validation: Found {{row_count}} rows in Delta table")
    
    # Show the results
    result_df.show()
    
    print(f"🎉 Hello World Sanity Check completed successfully! Message: {unique_message}")
    
except Exception as e:
    print(f"❌ Hello World Sanity Check failed: {{str(e)}}")
    raise e
finally:
    # Don't stop Spark session in Databricks cluster - it's managed by the platform
    try:
        spark.catalog.clearCache()  # Just clear cache instead
    except:
        pass  # Ignore any cleanup errors
'''
    
    # Create job configuration
    job_config = {
        "name": f"HelloWorld_SanityCheck_{config['test_id']}",
        "existing_cluster_id": cluster_id,
        "spark_python_task": {
            "python_file": "dbfs:/tmp/hello_world_sanity_script.py"
        },
        "timeout_seconds": 300,  # 5 minutes timeout
        "max_retries": 0,
        "tags": {
            "purpose": "sanity-check",
            "test_id": config["test_id"]
        }
    }
    
    # Upload the script to DBFS
    import base64
    script_b64 = base64.b64encode(pyspark_script.encode('utf-8')).decode('utf-8')
    client.dbfs.put(
        path="/tmp/hello_world_sanity_script.py",
        contents=script_b64,
        overwrite=True
    )
    print(f"✓ Uploaded PySpark script to DBFS")
    
    # Create the job
    job_response = client.jobs.create_job(**job_config)
    job_id = job_response["job_id"]
    print(f"✓ Created job with ID: {job_id}")
    
    return job_id


def submit_and_monitor_job(client: DatabricksAPI, job_id: str, timeout: int = 300) -> dict:
    """
    Submit a job and monitor its execution.
    Returns the job run result.
    """
    print(f"🚀 Submitting job {job_id}")
    
    # Submit the job
    run_response = client.jobs.run_now(job_id=job_id)
    run_id = run_response["run_id"]
    print(f"✓ Job submitted with run ID: {run_id}")
    
    # Monitor the job
    start_time = time.time()
    while time.time() - start_time < timeout:
        run_info = client.jobs.get_run(run_id)
        state = run_info["state"]
        life_cycle_state = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        
        print(f"📊 Job {run_id} state: {life_cycle_state} / {result_state}")
        
        if life_cycle_state == "TERMINATED":
            if result_state == "SUCCESS":
                print(f"✅ Job {run_id} completed successfully!")
                return {
                    "success": True,
                    "run_id": run_id,
                    "state": state,
                    "run_info": run_info
                }
            else:
                print(f"❌ Job {run_id} failed with state: {result_state}")
                return {
                    "success": False,
                    "run_id": run_id,
                    "state": state,
                    "run_info": run_info,
                    "error": f"Job failed with result state: {result_state}"
                }
        
        time.sleep(10)  # Check every 10 seconds
    
    # Timeout
    print(f"⏰ Job {run_id} timed out after {timeout} seconds")
    return {
        "success": False,
        "run_id": run_id,
        "error": f"Job timed out after {timeout} seconds"
    }


@pytest.mark.parametrize("databricks_resource", [
    {
        "resource_id": "sanity_check_test",
        "use_shared_cluster": True,
        "cluster_fallback": True,
        "shared_cluster_timeout": 1200
    }
], indirect=True)
@pytest.mark.databricks
@pytest.mark.sanity_check
def test_databricks_sanity_check(request, databricks_resource):
    """
    Sanity check test that directly implements Hello World functionality.
    
    This test:
    - Uses the databricks_resource fixture for setup
    - Creates a direct PySpark job (no AI model)
    - Submits and monitors the job using Databricks Jobs API
    - Validates results using the same validation logic as the main test
    - Cleans up after itself
    """
    # Create sanity check specific config
    config = create_sanity_check_config()
    
    # Get cluster info from the databricks_resource fixture
    databricks_manager = databricks_resource["databricks_manager"]
    cluster_id = databricks_resource["cluster_id"]
    cluster_created_by_us = databricks_resource["cluster_created_by_us"]
    databricks_client = databricks_resource["client"]

    print(f"Worker {os.getpid()}: Using cluster: {cluster_id}")
    
    start_time = time.time()
    job_id = None
    
    # Store test metadata
    request.node.user_properties.append(("test_type", "sanity_check"))
    request.node.user_properties.append(("unique_message", config["unique_message"]))
    request.node.user_properties.append(("test_id", config["test_id"]))
    
    test_steps = {
        "Environment Setup": {
            "order": 1,
            "description": "Set up Databricks environment for sanity check",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Job Creation": {
            "order": 2,
            "description": "Create Hello World job using Jobs API",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Job Execution": {
            "order": 3,
            "description": "Submit and monitor job execution",
            "status": "did not reach",
            "Result_Message": ""
        },
        "Data Validation": {
            "order": 4,
            "description": f"Verify job created Delta table with unique message: {config['unique_message'][:20]}...",
            "status": "did not reach",
            "Result_Message": ""
        }
    }
    request.node.user_properties.append(("test_steps", test_steps))
    
    try:
        # Step 1: Set up Databricks environment
        databricks_manager.setup_databricks_environment(cluster_id, config)
        update_test_step(test_steps, "Environment Setup", "passed", 
                        f"Environment setup completed for sanity check ID: {config['test_id']}")
        
        # Step 2: Create job
        job_id = create_hello_world_job(databricks_client, config, cluster_id)
        update_test_step(test_steps, "Job Creation", "passed", f"Created job with ID: {job_id}")
        
        # Step 3: Submit and monitor job
        job_result = submit_and_monitor_job(databricks_client, job_id)
        
        if job_result["success"]:
            update_test_step(test_steps, "Job Execution", "passed", 
                            f"Job {job_result['run_id']} completed successfully")
        else:
            update_test_step(test_steps, "Job Execution", "failed", 
                            f"Job execution failed: {job_result.get('error', 'Unknown error')}")
            raise Exception(f"Job execution failed: {job_result.get('error', 'Unknown error')}")
        
        # Step 4: Validate Results
        print(f"🔍 Starting validation for sanity check with message: {config['unique_message']}")
        try:
            validation_results = validate_hello_world_results(databricks_client, config)
            print(f"Validation Results: {validation_results}")
            
            # Check if validation passed
            validation_passed = validation_results.get("overall_valid", False)
            
            # Store detailed validation information for reporting
            request.node.user_properties.append(("validation_details", validation_results))
            
            if validation_passed:
                update_test_step(test_steps, "Data Validation", "passed", 
                    f"✅ Sanity check validation passed! "
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
                    
                update_test_step(test_steps, "Data Validation", "failed", 
                                f"❌ Sanity check validation failed: {', '.join(failure_details) or 'Unknown error'}")
                raise Exception(f"Sanity check validation failed: {failure_details}")
                
        except Exception as validation_error:
            update_test_step(test_steps, "Data Validation", "failed", 
                            f"Validation error: {str(validation_error)}")
            raise validation_error

        # Check all steps for failure
        failed_steps = []
        for step_name, step_data in test_steps.items():
            if step_data["status"] == "failed":
                failed_steps.append(step_name)
        
        if failed_steps:
            raise Exception(f"Sanity check failed at steps: {', '.join(failed_steps)}")
            
        print(f"🎉 Sanity check completed successfully! Infrastructure is working.")
        
    except Exception as e:
        # Find first non-passed step and mark as failed
        for step_name, step_data in test_steps.items():
            if step_data["status"] == "did not reach":
                update_test_step(test_steps, step_name, "failed", str(e))
                break
        raise e
    
    finally:
        # Clean up job
        if job_id:
            try:
                print(f"🧹 Cleaning up job {job_id}")
                databricks_client.jobs.delete_job(job_id)
                print(f"✓ Deleted job {job_id}")
            except Exception as e:
                print(f"⚠️ Could not delete job {job_id}: {e}")
        
        # Clean up script
        try:
            databricks_client.dbfs.delete("/tmp/hello_world_sanity_script.py")
            print(f"✓ Deleted script file")
        except Exception as e:
            print(f"⚠️ Could not delete script: {e}")
        
        # Clean up Databricks environment
        databricks_manager.cleanup_databricks_environment(config)
        
        # Store execution time
        request.node.user_properties.append(
            ("execution_time", time.time() - start_time)
        )
