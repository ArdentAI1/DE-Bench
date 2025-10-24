# Braintrust-only Databricks test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Databricks test validates that AI can create a Hello World PySpark job.

    NOTE: This test requires a DatabricksFixture implementation that doesn't exist yet.
    The DatabricksFixture would need to:
    - Manage Databricks workspace connections
    - Handle cluster creation/management
    - Manage notebooks and job execution
    - Handle Delta table operations
    - Integrate with Databricks REST API
    """
    # TODO: Implement DatabricksFixture
    # from Fixtures.Databricks.databricks_fixture import DatabricksFixture

    # For now, return empty list - this test will need the DatabricksFixture
    # to be implemented before it can run

    # When DatabricksFixture is implemented, it should look like:
    # custom_databricks_config = {
    #     "resource_id": f"databricks_hello_world_test_{test_timestamp}_{test_uuid}",
    #     "cluster_config": {...},
    #     "workspace_config": {...},
    # }
    # databricks_fixture = DatabricksFixture(custom_config=custom_databricks_config)
    # return [databricks_fixture]

    return []  # Will need DatabricksFixture implementation


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.

    NOTE: This function currently returns a placeholder configuration.
    When DatabricksFixture is implemented, it should create proper Databricks configs.
    """
    from extract_test_configs import create_config_from_fixtures

    # For now, return basic structure since DatabricksFixture doesn't exist yet
    # When implemented, this should call create_config_from_fixtures(fixtures)

    print(f"🔧 Databricks test prepared with timestamp: {test_timestamp}_{test_uuid}", flush=True)

    # Placeholder config until DatabricksFixture is implemented
    return {
        **base_model_inputs,
        "model_configs": {
            "services": {
                "databricks": {
                    "host": os.getenv("DATABRICKS_HOST"),
                    "token": os.getenv("DATABRICKS_TOKEN"),
                    "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
                    "http_path": os.getenv(
                        "DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/default"
                    ),
                    "catalog": "hive_metastore",
                    "schema": "default",
                    "table": f"hello_world_test_{test_timestamp}_{test_uuid}",
                    "unique_message": f"HELLO_WORLD_SUCCESS_{test_timestamp}_{test_uuid}",
                    "test_id": f"{test_timestamp}_{test_uuid}",
                    "notebook_path": f"/Shared/de_bench/hello_world_test_{test_timestamp}_{test_uuid}",
                    "delta_table_path": f"dbfs:/tmp/hello_world_test_{test_timestamp}_{test_uuid}",
                }
            }
        },
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created a Databricks Hello World job.

    Expected behavior:
    - Agent should create a PySpark script
    - Script should create a DataFrame with specific structure
    - Data should be written to Delta format
    - Table should be registered in catalog
    - Job should be created and executed successfully

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details

    NOTE: This validation is currently incomplete as it requires DatabricksFixture
    to properly validate Databricks job execution, Delta tables, and cluster operations.
    """
    # Create comprehensive test steps for validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to create Databricks Hello World job",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Databricks job creation task...",
        },
        {
            "name": "PySpark Script Creation",
            "description": "Verify that PySpark script was created with correct structure",
            "status": "running",
            "Result_Message": "Checking PySpark script creation...",
        },
        {
            "name": "DataFrame Creation Validation",
            "description": "Verify that DataFrame was created with correct columns and data",
            "status": "running",
            "Result_Message": "Validating DataFrame structure...",
        },
        {
            "name": "Delta Table Creation",
            "description": "Verify that data was written to Delta format at correct path",
            "status": "running",
            "Result_Message": "Checking Delta table creation...",
        },
        {
            "name": "Table Registration",
            "description": "Verify that table was registered in catalog",
            "status": "running",
            "Result_Message": "Validating table registration...",
        },
        {
            "name": "Job Creation",
            "description": "Verify that Databricks job was created successfully",
            "status": "running",
            "Result_Message": "Checking job creation...",
        },
        {
            "name": "Job Execution",
            "description": "Verify that job executed successfully",
            "status": "running",
            "Result_Message": "Validating job execution...",
        },
        {
            "name": "Data Verification",
            "description": "Verify that correct data exists in the Delta table",
            "status": "running",
            "Result_Message": "Checking data integrity...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # NOTE: The following validation steps are placeholders
        # They would need to be implemented when DatabricksFixture is available

        # For now, mark remaining steps as incomplete due to missing DatabricksFixture
        for i in range(1, len(test_steps)):
            test_steps[i]["status"] = "failed"
            test_steps[i][
                "Result_Message"
            ] = "❌ Cannot validate - DatabricksFixture not implemented yet"

        # TODO: When DatabricksFixture is implemented, add proper validation:
        # - Check if PySpark script was uploaded to DBFS
        # - Verify DataFrame creation with correct schema
        # - Validate Delta table exists at specified path
        # - Check table registration in catalog
        # - Verify job creation via Databricks Jobs API
        # - Monitor job execution and check for SUCCESS state
        # - Query Delta table to verify data integrity
        # - Validate unique message and timestamp in data

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    passed_steps = sum([step["status"] == "passed" for step in test_steps])
    total_steps = len(test_steps)
    score = passed_steps / total_steps

    print(
        f"🎯 Validation completed: {passed_steps}/{total_steps} steps passed (Score: {score:.2f}, flush=True)"
    )
    print(
        "⚠️  NOTE: This test requires DatabricksFixture implementation to be fully functional"
    , flush=True)

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
