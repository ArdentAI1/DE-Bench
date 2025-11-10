# Braintrust-only Snowflake test - Change Data Capture Pipeline with Streams and Tasks
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
    This Snowflake test validates that AI can implement a comprehensive CDC pipeline.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_cdc_pipeline_test_{test_timestamp}_{test_uuid}",
        "database": f"CDC_DB_{test_timestamp}_{test_uuid}",
        "schema": f"CDC_PIPELINE_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/cdc_pipeline_setup.sql",
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
    Validates that the AI agent successfully implemented a comprehensive CDC pipeline
    using Snowflake Streams and Tasks for enterprise-grade data synchronization.

    Expected behavior:
    - Multiple streams should be created on source tables
    - Tasks should be created for different CDC processing workflows
    - Target tables should have SCD Type 2 and audit trail capabilities
    - Data quality monitoring and error handling should be implemented
    - Cross-system referential integrity should be maintained

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
            "description": "AI Agent executes comprehensive CDC pipeline task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the CDC pipeline task...",
        },
        {
            "name": "Source System Setup Validation",
            "description": "Verify multi-system source tables with realistic business data",
            "status": "running",
            "Result_Message": "Validating source system setup...",
        },
        {
            "name": "Stream Architecture Implementation",
            "description": "Verify comprehensive streams on all source tables",
            "status": "running",
            "Result_Message": "Checking CDC stream architecture...",
        },
        {
            "name": "Task-Based Processing Pipeline",
            "description": "Verify automated tasks for different CDC workflows",
            "status": "running",
            "Result_Message": "Testing task-based processing implementation...",
        },
        {
            "name": "Target Architecture and SCD Implementation",
            "description": "Verify target tables with audit trails and versioning",
            "status": "running",
            "Result_Message": "Validating target architecture and SCD implementation...",
        },
        {
            "name": "Data Quality and Monitoring Framework",
            "description": "Verify monitoring, error handling and data quality checks",
            "status": "running",
            "Result_Message": "Testing data quality and monitoring capabilities...",
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

            # Step 2: Validate source system setup with multiple business systems
            source_table_counts = {}
            source_tables = [
                "CRM_CUSTOMERS",
                "ERP_TRANSACTIONS",
                "ECOMMERCE_ORDERS",
                "INVENTORY_UPDATES",
            ]

            for table in source_tables:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {database_name}.{schema_name}.{table}"
                    )
                    count = cursor.fetchone()[0]
                    source_table_counts[table] = count
                except Exception:
                    source_table_counts[table] = 0

            total_source_records = sum(source_table_counts.values())
            systems_with_data = sum(
                1 for count in source_table_counts.values() if count > 0
            )

            if systems_with_data >= 3 and total_source_records >= 15:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Multi-system source setup validated: {systems_with_data} systems, {total_source_records} total records"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Insufficient source setup: {systems_with_data} systems, {total_source_records} records"

            # Step 3: Check for comprehensive stream architecture
            cursor.execute(f"SHOW STREAMS IN SCHEMA {database_name}.{schema_name}")
            streams = cursor.fetchall()

            # Check for streams on source tables
            source_streams = []
            for stream in streams:
                # SHOW STREAMS returns: name, database_name, schema_name, table_name, ...
                stream_name = stream[0] if len(stream) > 0 else ""
                table_name = stream[3] if len(stream) > 3 else ""
                for source_table in source_tables:
                    if source_table in table_name.upper():
                        source_streams.append(stream)
                        break

            if len(source_streams) >= 3:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Comprehensive stream architecture: {len(source_streams)} streams on source tables"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Insufficient stream coverage: {len(source_streams)} streams found"

            # Step 4: Validate task-based processing pipeline
            cursor.execute(f"SHOW TASKS IN SCHEMA {database_name}.{schema_name}")
            tasks = cursor.fetchall()

            # Look for different types of CDC tasks
            cdc_task_types = []
            for task in tasks:
                # SHOW TASKS returns: name, database_name, schema_name, warehouse, schedule, state, definition...
                task_name = task[0].upper() if len(task) > 0 else ""
                task_def = task[6].upper() if len(task) > 6 and task[6] else ""

                if any(keyword in task_name for keyword in ["CUSTOMER", "CRM"]):
                    cdc_task_types.append("CUSTOMER_SYNC")
                elif any(
                    keyword in task_name
                    for keyword in ["TRANSACTION", "ERP", "FINANCIAL"]
                ):
                    cdc_task_types.append("TRANSACTION_PROCESSING")
                elif any(keyword in task_name for keyword in ["INVENTORY", "STOCK"]):
                    cdc_task_types.append("INVENTORY_SYNC")
                elif any(keyword in task_name for keyword in ["QUALITY", "MONITOR"]):
                    cdc_task_types.append("DATA_QUALITY")

            unique_task_types = list(set(cdc_task_types))

            if len(tasks) >= 2 and len(unique_task_types) >= 2:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Task-based processing pipeline: {len(tasks)} tasks covering {len(unique_task_types)} business areas"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ Insufficient task pipeline: {len(tasks)} tasks, {len(unique_task_types)} business areas"

            # Step 5: Validate target architecture with SCD and audit trails
            target_tables = [
                "UNIFIED_CUSTOMERS",
                "CUSTOMER_TRANSACTION_HISTORY",
                "INVENTORY_SNAPSHOT",
            ]
            target_capabilities = {
                "scd_type2": False,
                "audit_trails": False,
                "versioning": False,
            }

            for table in target_tables:
                try:
                    cursor.execute(
                        f"""
                        SELECT column_name
                        FROM {database_name}.information_schema.columns
                        WHERE table_schema = '{schema_name}' AND table_name = '{table}'
                    """
                    )
                    columns = [row[0] for row in cursor.fetchall()]

                    # Check for SCD Type 2 patterns
                    if any(
                        col in columns
                        for col in ["EFFECTIVE_DATE", "EXPIRY_DATE", "IS_CURRENT"]
                    ):
                        target_capabilities["scd_type2"] = True

                    # Check for audit trail patterns
                    if any(
                        col in columns
                        for col in ["CDC_OPERATION", "CDC_TIMESTAMP", "CDC_VERSION"]
                    ):
                        target_capabilities["audit_trails"] = True

                    # Check for versioning
                    if any(
                        col in columns
                        for col in ["VERSION", "CDC_VERSION", "RECORD_HASH"]
                    ):
                        target_capabilities["versioning"] = True

                except Exception:
                    continue

            capability_count = sum(target_capabilities.values())

            if capability_count >= 2:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Target architecture validated: SCD2={target_capabilities['scd_type2']}, Audit={target_capabilities['audit_trails']}, Versioning={target_capabilities['versioning']}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = f"❌ Limited target capabilities: {capability_count}/3 advanced features implemented"

            # Step 6: Validate data quality and monitoring framework
            monitoring_tables = ["CDC_STREAM_LOG", "CDC_DATA_QUALITY_LOG"]
            monitoring_capabilities = 0

            for table in monitoring_tables:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {database_name}.{schema_name}.{table}"
                    )
                    cursor.fetchone()  # Just check if table exists and is queryable
                    monitoring_capabilities += 1
                except Exception:
                    pass

            # Check for monitoring procedures or functions
            monitoring_procedures = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.information_schema.procedures
                    WHERE procedure_schema = '{schema_name}'
                    AND (UPPER(procedure_name) LIKE '%MONITOR%' OR UPPER(procedure_name) LIKE '%QUALITY%' OR UPPER(procedure_name) LIKE '%ERROR%')
                """
                )
                monitoring_procedures = cursor.fetchone()[0]
            except Exception:
                # Try alternative approach using SHOW PROCEDURES
                try:
                    cursor.execute(
                        f"SHOW PROCEDURES IN SCHEMA {database_name}.{schema_name}"
                    )
                    procedures = cursor.fetchall()
                    monitoring_procedures = sum(
                        1
                        for proc in procedures
                        if any(
                            keyword in proc[0].upper()
                            for keyword in ["MONITOR", "QUALITY", "ERROR"]
                        )
                    )
                except Exception:
                    monitoring_procedures = 0

            # Check for data quality views or functions
            monitoring_views = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.information_schema.views
                    WHERE table_schema = '{schema_name}'
                    AND (UPPER(view_name) LIKE '%QUALITY%' OR UPPER(view_name) LIKE '%MONITOR%' OR UPPER(view_name) LIKE '%HEALTH%')
                """
                )
                monitoring_views = cursor.fetchone()[0]
            except Exception:
                # Try alternative approach using SHOW VIEWS
                try:
                    cursor.execute(
                        f"SHOW VIEWS IN SCHEMA {database_name}.{schema_name}"
                    )
                    views = cursor.fetchall()
                    monitoring_views = sum(
                        1
                        for view in views
                        if any(
                            keyword in view[0].upper()
                            for keyword in ["QUALITY", "MONITOR", "HEALTH"]
                        )
                    )
                except Exception:
                    monitoring_views = 0

            total_monitoring_features = (
                monitoring_capabilities + monitoring_procedures + monitoring_views
            )

            if total_monitoring_features >= 2:
                test_steps[5]["status"] = "passed"
                test_steps[5][
                    "Result_Message"
                ] = f"✅ Monitoring framework validated: {monitoring_capabilities} log tables, {monitoring_procedures} procedures, {monitoring_views} views"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5][
                    "Result_Message"
                ] = f"❌ Limited monitoring capabilities: {total_monitoring_features} monitoring features found"

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
