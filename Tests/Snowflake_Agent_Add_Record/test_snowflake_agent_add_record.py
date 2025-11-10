# Braintrust-only Snowflake test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import snowflake.connector
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
    This Snowflake test validates that AI can add a record to a Snowflake table.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_test_{test_timestamp}_{test_uuid}",
        "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
        "schema": f"TEST_SCHEMA_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/users_schema.sql",
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/users_simple_20250901_233609.parquet",
            "aws_key_id": "env:AWS_ACCESS_KEY",
            "aws_secret_key": "env:AWS_SECRET_KEY",
        },
    }

    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    return [snowflake_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup.
    """
    from extract_test_configs import create_config_from_fixtures

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully added a new user record to Snowflake.

    Expected behavior:
    - A new user record for "Sarah Johnson" should be added to the USERS table
    - The record should have all the correct field values
    - Data integrity should be maintained

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to add user record to Snowflake",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Snowflake record addition task...",
        },
        {
            "name": "User Record Addition",
            "description": "Verify that Sarah Johnson record was added to USERS table",
            "status": "running",
            "Result_Message": "Validating that Sarah Johnson record exists in Snowflake...",
        },
        {
            "name": "Record Data Validation",
            "description": "Verify that the record has correct field values",
            "status": "running",
            "Result_Message": "Validating record field values...",
        },
    ]

    overall_success = False

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"success": False, "test_steps": test_steps}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Use fixture to get Snowflake connection for validation
        snowflake_fixture = None
        if fixtures:
            snowflake_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "snowflake_resource"),
                None,
            )

        if not snowflake_fixture:
            raise Exception("Snowflake fixture not found")

        # Get connection using the fixture
        conn = snowflake_fixture.get_connection()
        cursor = conn.cursor()

        try:
            # Get the database and schema names from the fixture
            resource_data = getattr(snowflake_fixture, "_resource_data", None)
            if not resource_data:
                raise Exception("Snowflake resource data not available")

            database_name = resource_data.get("database")
            schema_name = resource_data.get("schema")

            # Step 2: Check if Sarah Johnson record was added
            query = f"""
            SELECT FIRST_NAME, LAST_NAME, EMAIL, AGE, CITY, STATE, IS_ACTIVE, TOTAL_PURCHASES 
            FROM {database_name}.{schema_name}.USERS 
            WHERE FIRST_NAME = 'Sarah' AND LAST_NAME = 'Johnson' AND EMAIL = 'sarah.johnson@newuser.com'
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = "❌ Sarah Johnson record not found in USERS table"
                return {"success": False, "test_steps": test_steps}

            test_steps[1]["status"] = "passed"
            test_steps[1][
                "Result_Message"
            ] = f"✅ Sarah Johnson record found in USERS table"

            # Step 3: Validate the record data
            record = results[0]
            expected_values = {
                "FIRST_NAME": "Sarah",
                "LAST_NAME": "Johnson",
                "EMAIL": "sarah.johnson@newuser.com",
                "AGE": 35,
                "CITY": "Austin",
                "STATE": "TX",
                "IS_ACTIVE": True,
                "TOTAL_PURCHASES": 0.00,
            }

            # Check each field
            validation_errors = []
            for i, (field, expected) in enumerate(expected_values.items()):
                actual = record[i]

                # Handle data type conversions for comparison
                if field == "TOTAL_PURCHASES":
                    # Convert Decimal to float for comparison
                    actual = float(actual) if actual is not None else actual
                    expected = float(expected)
                elif field in ["AGE"] and actual is not None:
                    # Ensure integer fields are compared as integers
                    actual = int(actual)
                    expected = int(expected)

                if actual != expected:
                    validation_errors.append(
                        f"{field}: expected {expected}, got {actual}"
                    )

            if validation_errors:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Record data validation failed: {'; '.join(validation_errors)}"
            else:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = "✅ All record field values are correct"
                overall_success = True

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Snowflake validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
