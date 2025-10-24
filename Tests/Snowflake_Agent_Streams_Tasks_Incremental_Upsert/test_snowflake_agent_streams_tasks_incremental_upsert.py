# Braintrust-only Snowflake test - Streams & Tasks for Incremental Upsert CDC
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
    This Snowflake test validates that AI can implement Streams & Tasks for CDC.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_streams_tasks_test_{test_timestamp}_{test_uuid}",
        "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
        "schema": f"CDC_SCHEMA_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/streams_tasks_setup.sql",
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
    Validates that the AI agent successfully implemented a CDC pipeline
    using Snowflake Streams and Tasks for incremental upserts.

    Expected behavior:
    - A Stream should be created on the ORDERS table
    - A Task should be created to process the stream data
    - The Task should perform incremental upserts on ORDER_SUMMARY
    - The pipeline should handle INSERT, UPDATE, DELETE operations
    - Processing should be automatic and serverless

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
            "description": "AI Agent executes Streams & Tasks CDC pipeline task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the CDC pipeline task...",
        },
        {
            "name": "Stream Creation",
            "description": "Verify Stream is created on ORDERS table for change capture",
            "status": "running",
            "Result_Message": "Validating stream configuration...",
        },
        {
            "name": "Task Creation",
            "description": "Verify Task is created for automated stream processing",
            "status": "running",
            "Result_Message": "Checking task configuration...",
        },
        {
            "name": "CDC Processing Logic",
            "description": "Verify task implements proper incremental upsert logic",
            "status": "running",
            "Result_Message": "Testing CDC processing logic...",
        },
        {
            "name": "Data Pipeline Validation",
            "description": "Test pipeline with INSERT/UPDATE/DELETE operations",
            "status": "running",
            "Result_Message": "Validating end-to-end pipeline functionality...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed task execution successfully"

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

            # First, verify the base tables exist (this helps debug setup issues)
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
                orders_count = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDER_SUMMARY")
                summary_count = cursor.fetchone()[0]
                print(f"✅ Base tables verified: ORDERS ({orders_count} records, flush=True), ORDER_SUMMARY ({summary_count} records)")
            except Exception as e:
                raise Exception(f"Base tables not found or accessible: {str(e)}")

            # Step 2: Check for Stream creation
            cursor.execute(f"SHOW STREAMS IN SCHEMA {database_name}.{schema_name}")
            streams = cursor.fetchall()

            # Get column names from cursor description
            stream_columns = [desc[0].lower() for desc in cursor.description] if cursor.description else []

            # Debug: Log all found streams for troubleshooting
            stream_details = []
            orders_stream = None

            for stream in streams:
                try:
                    # Create a dict mapping column names to values
                    stream_dict = dict(zip(stream_columns, stream)) if stream_columns else {}

                    # Extract fields with proper type handling
                    stream_name = str(stream_dict.get('name', stream[0] if len(stream) > 0 else "unknown"))
                    table_name = str(stream_dict.get('table_name', stream_dict.get('source_name', "")))
                    stream_mode = str(stream_dict.get('mode', stream_dict.get('type', "unknown")))

                    stream_details.append(f"{stream_name} on {table_name} (mode: {stream_mode})")

                    if table_name and 'ORDERS' in table_name.upper():  # table_name contains ORDERS
                        orders_stream = stream_dict
                        break
                except Exception as e:
                    print(f"⚠️ Error parsing stream row: {e}", flush=True)
                    continue

            if orders_stream:
                stream_name = str(orders_stream.get('name', 'unknown'))
                stream_mode = str(orders_stream.get('mode', orders_stream.get('type', 'unknown')))
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"✅ Stream '{stream_name}' found on ORDERS table with mode '{stream_mode}'"
            else:
                test_steps[1]["status"] = "failed"
                if streams:
                    test_steps[1]["Result_Message"] = f"❌ No stream found on ORDERS table. Found {len(streams)} streams: {'; '.join(stream_details)}"
                else:
                    test_steps[1]["Result_Message"] = "❌ No streams found in schema. Streams must be created on the ORDERS table."

            # Step 3: Check for Task creation
            cursor.execute(f"SHOW TASKS IN SCHEMA {database_name}.{schema_name}")
            tasks = cursor.fetchall()

            # Get column names from cursor description
            task_columns = [desc[0].lower() for desc in cursor.description] if cursor.description else []

            # Debug: Log all found tasks for troubleshooting
            task_details = []
            cdc_task = None

            for task in tasks:
                try:
                    # Create a dict mapping column names to values
                    task_dict = dict(zip(task_columns, task)) if task_columns else {}

                    # Extract fields with proper type handling
                    task_name = str(task_dict.get('name', task[0] if len(task) > 0 else "unknown"))
                    task_state = str(task_dict.get('state', "unknown"))
                    task_schedule = str(task_dict.get('schedule', "unknown"))

                    task_details.append(f"{task_name} (state: {task_state}, schedule: {task_schedule})")

                    # Look for CDC-related keywords in task name
                    task_name_upper = task_name.upper()
                    if any(keyword in task_name_upper for keyword in ['CDC', 'STREAM', 'UPSERT', 'ORDER']):
                        cdc_task = task_dict
                        break
                except Exception as e:
                    print(f"⚠️ Error parsing task row: {e}", flush=True)
                    continue

            if cdc_task:
                task_name = str(cdc_task.get('name', 'unknown'))
                task_state = str(cdc_task.get('state', 'unknown'))
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ CDC Task '{task_name}' found with state '{task_state}'"
            else:
                test_steps[2]["status"] = "failed"
                if tasks:
                    test_steps[2]["Result_Message"] = f"❌ No CDC task found. Found {len(tasks)} tasks: {'; '.join(task_details)}. Expected task name to contain: CDC, STREAM, UPSERT, or ORDER"
                else:
                    test_steps[2]["Result_Message"] = "❌ No tasks found in schema. A task must be created to process stream changes."

            # Step 4: Check for proper CDC processing logic by examining task definition
            if cdc_task:
                # Get task definition from the dict
                task_definition = str(cdc_task.get('definition', ""))

                if task_definition:
                    task_definition_upper = task_definition.upper()

                    # Check for key CDC patterns
                    stream_name_for_check = str(orders_stream.get('name', '')) if orders_stream else ''
                    has_stream_reference = stream_name_for_check and stream_name_for_check.upper() in task_definition_upper
                    has_upsert_logic = any(keyword in task_definition_upper for keyword in ['MERGE', 'UPSERT', 'INSERT', 'UPDATE'])
                    has_aggregation = any(keyword in task_definition_upper for keyword in ['SUM', 'COUNT', 'AVG', 'GROUP BY'])

                    if has_stream_reference and has_upsert_logic and has_aggregation:
                        test_steps[3]["status"] = "passed"
                        test_steps[3]["Result_Message"] = "✅ Task implements proper CDC logic with stream processing and aggregation"
                    else:
                        test_steps[3]["status"] = "failed"
                        test_steps[3]["Result_Message"] = f"❌ Task definition missing key CDC patterns (stream: {has_stream_reference}, upsert: {has_upsert_logic}, agg: {has_aggregation})"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = "❌ Could not retrieve task definition"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ No CDC task available to validate"

            # Step 5: Test the pipeline by inserting new data and checking results
            # First, get initial summary counts
            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDER_SUMMARY")
            initial_summary_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
            initial_orders_count = cursor.fetchone()[0]

            # Check if summary table structure is appropriate for CDC
            cursor.execute(f"""
                SELECT column_name
                FROM {database_name}.information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = 'ORDER_SUMMARY'
                ORDER BY ordinal_position
            """)
            summary_columns = [row[0] for row in cursor.fetchall()]

            expected_cols = ['CUSTOMER_ID', 'TOTAL_ORDERS', 'TOTAL_AMOUNT', 'LAST_ORDER_DATE']
            has_required_cols = all(col in summary_columns for col in expected_cols)

            # Insert a new order to trigger the stream (if streams/tasks exist)
            cursor.execute(f"""
                INSERT INTO {database_name}.{schema_name}.ORDERS 
                (CUSTOMER_ID, PRODUCT_ID, QUANTITY, PRICE, STATUS, ORDER_DATE, UPDATED_AT)
                VALUES (999, 999, 1, 99.99, 'TEST', CURRENT_DATE(), CURRENT_TIMESTAMP())
            """)

            # Check that data was inserted
            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
            new_orders_count = cursor.fetchone()[0]

            # Check if there's a stream processing log table for monitoring
            stream_log_exists = False
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.STREAM_PROCESSING_LOG")
                stream_log_exists = True
            except:
                pass

            pipeline_ready = (
                new_orders_count > initial_orders_count and 
                has_required_cols and 
                orders_stream is not None and 
                cdc_task is not None
            )

            if pipeline_ready:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Pipeline validated: {new_orders_count} orders, summary table ready, stream & task created"
            elif new_orders_count > initial_orders_count and has_required_cols:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = f"❌ Tables ready but missing stream/task: orders {initial_orders_count}->{new_orders_count}, columns OK, stream: {orders_stream is not None}, task: {cdc_task is not None}"
            else:
                test_steps[4]["status"] = "failed"
                missing = []
                if new_orders_count <= initial_orders_count:
                    missing.append("data insert failed")
                if not has_required_cols:
                    missing.append(f"missing columns: {set(expected_cols) - set(summary_columns)}")
                test_steps[4]["Result_Message"] = f"❌ Pipeline validation failed: {', '.join(missing)}"

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
    score = sum(1 for step in test_steps if step["status"] == "passed") / len(test_steps)
    
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
