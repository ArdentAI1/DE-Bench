# Braintrust-only Snowflake test - COPY INTO from S3 with Schema Drift handling
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
    This Snowflake test validates that AI can handle schema drift with COPY INTO from S3.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_copy_s3_test_{test_timestamp}_{test_uuid}",
        "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
        "schema": f"SCHEMA_DRIFT_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/schema_drift_setup.sql",
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/customers/",  # Directory path for multiple files
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
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented a robust COPY INTO pipeline
    that can handle schema drift from S3 data files.

    Expected behavior:
    - A staging table should be created that can accommodate schema evolution
    - COPY INTO commands should use MATCH_BY_COLUMN_NAME for column order flexibility
    - Error handling should be implemented for schema mismatches
    - Pipeline should work with both initial and evolved schemas

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes schema drift handling task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the schema drift pipeline task...",
        },
        {
            "name": "Staging Table Creation",
            "description": "Verify flexible staging table exists for schema evolution",
            "status": "running",
            "Result_Message": "Validating staging table structure...",
        },
        {
            "name": "COPY INTO Configuration",
            "description": "Verify COPY INTO uses MATCH_BY_COLUMN_NAME for flexibility",
            "status": "running",
            "Result_Message": "Checking COPY INTO command configuration...",
        },
        {
            "name": "Schema Evolution Handling",
            "description": "Test that pipeline handles new columns gracefully",
            "status": "running",
            "Result_Message": "Testing schema drift scenarios...",
        },
        {
            "name": "Data Loading Validation",
            "description": "Verify data loads correctly with different schemas",
            "status": "running",
            "Result_Message": "Validating data loading results...",
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

            # Step 2: Check if staging table exists with flexible schema
            cursor.execute(
                f"""
                SELECT column_name, data_type 
                FROM {database_name}.information_schema.columns 
                WHERE table_schema = '{schema_name}' 
                AND table_name = 'CUSTOMER_STAGING'
                ORDER BY ordinal_position
            """
            )
            staging_columns = cursor.fetchall()

            if not staging_columns:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ CUSTOMER_STAGING table not found"
            else:
                # Check for key columns that support schema evolution
                column_names = [col[0] for col in staging_columns]
                required_cols = ["CUSTOMER_ID", "NAME", "EMAIL", "SIGNUP_DATE"]
                evolution_cols = ["PHONE", "ADDRESS", "CUSTOMER_TYPE", "LOYALTY_POINTS"]

                missing_required = [
                    col for col in required_cols if col not in column_names
                ]
                present_evolution = [
                    col for col in evolution_cols if col in column_names
                ]

                if missing_required:
                    test_steps[1]["status"] = "failed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"❌ Missing required columns: {missing_required}"
                elif len(present_evolution) >= 2:  # At least 2 evolution columns
                    test_steps[1]["status"] = "passed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"✅ Flexible staging table found with {len(column_names)} columns including evolution fields"
                else:
                    test_steps[1]["status"] = "partial"
                    test_steps[1][
                        "Result_Message"
                    ] = f"⚠️ Staging table exists but may not support full schema evolution"

            # Step 3: Check for COPY INTO configuration with MATCH_BY_COLUMN_NAME
            # This is checked by looking for evidence of proper file format and stage setup
            file_formats = []
            stages = []

            try:
                cursor.execute(
                    f"""
                    SELECT file_format_name, file_format_type, format_options
                    FROM {database_name}.information_schema.file_formats
                    WHERE file_format_schema = '{schema_name}'
                """
                )
                file_formats = cursor.fetchall()
            except Exception:
                # Fallback to SHOW FILE FORMATS
                try:
                    cursor.execute(
                        f"SHOW FILE FORMATS IN SCHEMA {database_name}.{schema_name}"
                    )
                    file_formats = cursor.fetchall()
                except Exception:
                    file_formats = []

            try:
                cursor.execute(
                    f"""
                    SELECT stage_name, stage_type, stage_url
                    FROM {database_name}.information_schema.stages  
                    WHERE stage_schema = '{schema_name}'
                """
                )
                stages = cursor.fetchall()
            except Exception:
                # Fallback to SHOW STAGES
                try:
                    cursor.execute(
                        f"SHOW STAGES IN SCHEMA {database_name}.{schema_name}"
                    )
                    stages = cursor.fetchall()
                except Exception:
                    stages = []

            if file_formats and stages:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Found {len(file_formats)} file format(s) and {len(stages)} stage(s) for flexible data loading"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = "❌ Missing file formats or stages for COPY INTO operation"

            # Step 4: Test schema evolution handling by checking for flexible column handling
            evolution_views = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) as view_count
                    FROM {database_name}.information_schema.views
                    WHERE table_schema = '{schema_name}' 
                    AND (table_name LIKE '%UNIFIED%' OR table_name LIKE '%EVOLUTION%' OR table_name LIKE '%FLEXIBLE%')
                """
                )
                evolution_views = cursor.fetchone()[0]
            except Exception:
                # Fallback to SHOW VIEWS
                try:
                    cursor.execute(
                        f"SHOW VIEWS IN SCHEMA {database_name}.{schema_name}"
                    )
                    views = cursor.fetchall()
                    evolution_views = sum(
                        1
                        for view in views
                        if any(
                            keyword in view[0].upper()
                            for keyword in ["UNIFIED", "EVOLUTION", "FLEXIBLE"]
                        )
                    )
                except Exception:
                    evolution_views = 0

            # Also check for procedures or functions that might handle schema evolution
            procedures_count = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) 
                    FROM {database_name}.information_schema.procedures
                    WHERE procedure_schema = '{schema_name}'
                """
                )
                procedures_count = cursor.fetchone()[0]
            except Exception:
                # Fallback to SHOW PROCEDURES
                try:
                    cursor.execute(
                        f"SHOW PROCEDURES IN SCHEMA {database_name}.{schema_name}"
                    )
                    procedures = cursor.fetchall()
                    procedures_count = len(procedures)
                except Exception:
                    procedures_count = 0

            if evolution_views > 0 or procedures_count > 0:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Schema evolution handling implemented with {evolution_views} views and {procedures_count} procedures"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = "❌ No evidence of schema evolution handling mechanisms"

            # Step 5: Validate data loading by checking staging table contents
            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.CUSTOMER_STAGING"
            )
            staging_count = cursor.fetchone()[0]

            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT COALESCE(CUSTOMER_TYPE, 'DEFAULT')) as type_variety
                FROM {database_name}.{schema_name}.CUSTOMER_STAGING
            """
            )
            type_variety = cursor.fetchone()[0]

            if staging_count >= 3:  # Initial test data should be present
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Data loading successful with {staging_count} records and {type_variety} customer types"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = f"❌ Insufficient data in staging table: {staging_count} records"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    passed_steps = sum(1 for step in test_steps if step["status"] == "passed")
    partial_steps = sum(0.5 for step in test_steps if step["status"] == "partial")
    total_steps = len(test_steps)

    score = (passed_steps + partial_steps) / total_steps

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
