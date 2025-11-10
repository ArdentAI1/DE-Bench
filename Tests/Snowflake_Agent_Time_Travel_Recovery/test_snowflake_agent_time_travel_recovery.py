# Braintrust-only Snowflake test - Time Travel Recovery & Accident Rollback
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
    This Snowflake test validates that AI can implement Time Travel recovery for production accidents.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_time_travel_test_{test_timestamp}_{test_uuid}",
        "database": f"PROD_DB_{test_timestamp}_{test_uuid}",
        "schema": f"TIME_TRAVEL_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/time_travel_setup.sql",
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
    Validates that the AI agent successfully implemented Time Travel recovery
    system for handling production accidents and data recovery.

    Expected behavior:
    - Time Travel queries using AT/BEFORE clauses should be implemented
    - UNDROP functionality should be demonstrated
    - Recovery procedures should be created for common accident scenarios
    - Audit trail and recovery validation should be implemented

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
            "description": "AI Agent executes Time Travel recovery task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Time Travel recovery task...",
        },
        {
            "name": "Production Data Validation",
            "description": "Verify production database has proper time travel configuration",
            "status": "running",
            "Result_Message": "Validating production database and time travel settings...",
        },
        {
            "name": "Recovery Procedures Implementation",
            "description": "Verify recovery procedures and stored functions exist",
            "status": "running",
            "Result_Message": "Checking for Time Travel recovery procedures...",
        },
        {
            "name": "Time Travel Query Capability",
            "description": "Test AT/BEFORE clause functionality for point-in-time recovery",
            "status": "running",
            "Result_Message": "Testing Time Travel query capabilities...",
        },
        {
            "name": "Recovery Management Framework",
            "description": "Verify audit trail and recovery tracking infrastructure",
            "status": "running",
            "Result_Message": "Validating recovery management and audit capabilities...",
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

            # Step 2: Validate production database setup with Time Travel configuration
            # Check for tables with proper data retention settings using SHOW TABLES
            cursor.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
            all_tables = cursor.fetchall()

            # Get column names from cursor description
            table_columns = (
                [desc[0].lower() for desc in cursor.description]
                if cursor.description
                else []
            )

            # Filter tables that have data retention > 0
            time_travel_tables = []
            for table in all_tables:
                try:
                    # Create a dict mapping column names to values
                    table_dict = (
                        dict(zip(table_columns, table)) if table_columns else {}
                    )

                    # Get retention_time with proper type handling
                    retention_time = table_dict.get(
                        "retention_time", table[10] if len(table) > 10 else 0
                    )

                    # Convert to int for comparison
                    retention_int = (
                        int(retention_time) if retention_time is not None else 0
                    )

                    if retention_int > 0:
                        time_travel_tables.append(table)
                except (ValueError, TypeError, IndexError) as e:
                    # Skip tables where we can't parse retention_time
                    continue

            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.CUSTOMERS"
            )
            customer_count = int(cursor.fetchone()[0])

            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
            order_count = int(cursor.fetchone()[0])

            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.AUDIT_LOG"
            )
            audit_count = int(cursor.fetchone()[0])

            if (
                len(time_travel_tables) >= 2
                and customer_count >= 3
                and order_count >= 5
                and audit_count >= 10
            ):
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Production setup validated: {len(time_travel_tables)} tables with time travel, {customer_count} customers, {order_count} orders, {audit_count} audit records"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Insufficient production setup: {len(time_travel_tables)} time travel tables, {customer_count} customers, {order_count} orders"

            # Step 3: Check for recovery procedures and stored functions
            cursor.execute(
                f"""
                SELECT procedure_name, argument_signature
                FROM {database_name}.information_schema.procedures
                WHERE procedure_schema = '{schema_name}'
                AND (UPPER(procedure_name) LIKE '%RECOVERY%' OR UPPER(procedure_name) LIKE '%RESTORE%' OR UPPER(procedure_name) LIKE '%ROLLBACK%')
            """
            )
            recovery_procedures = cursor.fetchall()

            cursor.execute(
                f"""
                SELECT function_name 
                FROM {database_name}.information_schema.functions
                WHERE function_schema = '{schema_name}'
                AND (UPPER(function_name) LIKE '%RECOVERY%' OR UPPER(function_name) LIKE '%TIME_TRAVEL%')
            """
            )
            recovery_functions = cursor.fetchall()

            # Check for recovery-related tables
            cursor.execute(
                f"""
                SELECT table_name
                FROM {database_name}.information_schema.tables
                WHERE table_schema = '{schema_name}' 
                AND (UPPER(table_name) LIKE '%RECOVERY%' OR UPPER(table_name) LIKE '%CHECKPOINT%')
            """
            )
            recovery_tables = cursor.fetchall()

            if recovery_procedures or recovery_functions or recovery_tables:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Recovery infrastructure found: {len(recovery_procedures)} procedures, {len(recovery_functions)} functions, {len(recovery_tables)} tables"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = "❌ No recovery procedures, functions, or management infrastructure found"

            # Step 4: Test Time Travel query capability
            # Try to execute a basic Time Travel query to verify functionality
            try:
                # Instead of looking back in time (which fails for newly created tables),
                # verify that the tables support time travel by checking retention settings
                cursor.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
                tables_with_retention = cursor.fetchall()

                # Get column names from cursor description
                show_tables_columns = (
                    [desc[0].lower() for desc in cursor.description]
                    if cursor.description
                    else []
                )

                # Count tables that have time travel enabled (retention > 0)
                time_travel_enabled_count = 0
                for table_row in tables_with_retention:
                    try:
                        table_dict = (
                            dict(zip(show_tables_columns, table_row))
                            if show_tables_columns
                            else {}
                        )
                        retention_time = table_dict.get("retention_time", 0)
                        retention_int = (
                            int(retention_time) if retention_time is not None else 0
                        )
                        if retention_int > 0:
                            time_travel_enabled_count += 1
                    except (ValueError, TypeError):
                        continue

                # Test BEFORE clause functionality (works for current timestamp)
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.{schema_name}.ORDERS BEFORE(TIMESTAMP => CURRENT_TIMESTAMP())
                """
                )
                current_order_count = int(cursor.fetchone()[0])

                # Check if we can execute time travel queries
                if time_travel_enabled_count > 0 and current_order_count >= 0:
                    test_steps[3]["status"] = "passed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"✅ Time Travel capability verified: {time_travel_enabled_count} tables with retention enabled, BEFORE clause returned {current_order_count} orders"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"❌ Time Travel not properly configured: {time_travel_enabled_count} tables with retention"

            except Exception as e:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ Time Travel query test failed: {str(e)}"

            # Step 5: Validate recovery management framework
            # Check for proper audit trail and recovery tracking
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {database_name}.{schema_name}.AUDIT_LOG"
                )
                total_audit_records = int(cursor.fetchone()[0])

                cursor.execute(
                    f"""
                    SELECT DISTINCT TABLE_NAME 
                    FROM {database_name}.{schema_name}.AUDIT_LOG
                """
                )
                audited_tables = cursor.fetchall()

                # Check for business metrics or validation views
                cursor.execute(
                    f"""
                    SELECT table_name
                    FROM {database_name}.information_schema.views
                    WHERE table_schema = '{schema_name}'
                    AND (UPPER(table_name) LIKE '%METRIC%' OR UPPER(table_name) LIKE '%BUSINESS%' OR UPPER(table_name) LIKE '%VALIDATION%')
                """
                )
                validation_views = cursor.fetchall()

                # Check for checkpoint or recovery tracking capabilities
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.information_schema.tables
                    WHERE table_schema = '{schema_name}'
                    AND UPPER(table_name) LIKE '%CHECKPOINT%'
                """
                )
                checkpoint_tables_raw = cursor.fetchone()[0]
                checkpoint_tables = (
                    int(checkpoint_tables_raw)
                    if checkpoint_tables_raw is not None
                    else 0
                )

                if (
                    total_audit_records >= 10
                    and len(audited_tables) >= 2
                    and (validation_views or checkpoint_tables > 0)
                ):
                    test_steps[4]["status"] = "passed"
                    test_steps[4][
                        "Result_Message"
                    ] = f"✅ Recovery framework validated: {total_audit_records} audit records, {len(audited_tables)} audited tables, {len(validation_views)} validation views, {checkpoint_tables} checkpoint tables"
                else:
                    test_steps[4]["status"] = "failed"
                    test_steps[4][
                        "Result_Message"
                    ] = f"❌ Incomplete recovery framework: {total_audit_records} audit records, {len(audited_tables)} audited tables"

            except Exception as e:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = f"❌ Recovery framework validation failed: {str(e)}"

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
    score = sum(1 for step in test_steps if step["status"] == "passed") / len(
        test_steps
    )

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
