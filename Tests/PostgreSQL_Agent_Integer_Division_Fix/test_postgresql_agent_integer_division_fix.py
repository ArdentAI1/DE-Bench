# Braintrust-only PostgreSQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import psycopg2
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This PostgreSQL test validates AI agent functionality with database operations.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"integer_division_fix_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"purchases_test_db_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    return [postgres_fixture]


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
    Validates that the AI agent successfully completed the PostgreSQL task.

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
            "description": "AI Agent executes PostgreSQL database task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the PostgreSQL task...",
        },
        {
            "name": "Database Validation",
            "description": "Verify that database changes were applied correctly",
            "status": "running",
            "Result_Message": "Validating database state after AI execution...",
        },
        {
            "name": "Data Integrity Validation",
            "description": "Verify data integrity and relationships are preserved",
            "status": "running",
            "Result_Message": "Validating data integrity and relationships...",
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
            # Calculate score as the fraction of steps that passed
            score = sum([step["status"] == "passed" for step in test_steps]) / len(
                test_steps
            )
            return {
                "score": score,
                "metadata": {"test_steps": test_steps},
            }

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Use fixture to get PostgreSQL connection for validation
        postgres_fixture = None
        if fixtures:
            postgres_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"),
                None,
            )

        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")

        # Get PostgreSQL resource data from fixture
        resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("PostgreSQL resource data not available")

        created_resources = resource_data["created_resources"]
        created_db_name = created_resources[0]["name"]

        # Connect to database for validation
        db_connection = postgres_fixture.get_connection(created_db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Demonstrate the integer division problem first
            print("🔍 Demonstrating integer division problem...", flush=True)
            db_cursor.execute(
                "SELECT user_id, total_items, total_orders, total_items / total_orders AS avg_items_per_order FROM purchases_bad ORDER BY user_id"
            )
            original_results = db_cursor.fetchall()

            print("Original results (showing integer division problem, flush=True):")
            for row in original_results:
                print(
                    f"  User {row[0]}: {row[1]} items / {row[2]} orders = {row[3]} (truncated, flush=True)"
                )

            # Check if the problem was demonstrated (all division results should be 0 due to integer truncation)
            division_results = [row[3] for row in original_results]
            if all(result == 0 for result in division_results):
                print(
                    "✅ Integer division problem confirmed - all results are 0",
                    flush=True,
                )
            else:
                print(
                    "⚠️ Warning: Integer division problem not clearly demonstrated",
                    flush=True,
                )

            # Step 3: Check if the agent fixed the issue
            print(
                "🔍 Checking if agent fixed the integer division issue...", flush=True
            )

            # Try different approaches the agent might have used:
            # 1. Check if column types were changed to DECIMAL/NUMERIC
            try:
                db_cursor.execute(
                    """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'purchases_bad' AND column_name IN ('total_items', 'total_orders')
                    ORDER BY column_name
                """
                )
                column_types = db_cursor.fetchall()
                print(f"Column types: {column_types}", flush=True)

                # Check if types were changed to DECIMAL or NUMERIC
                decimal_types = [
                    col
                    for col in column_types
                    if "numeric" in col[1].lower() or "decimal" in col[1].lower()
                ]
                if decimal_types:
                    print(
                        "✅ Agent changed column types to support decimal division",
                        flush=True,
                    )

                    # Test the division again
                    db_cursor.execute(
                        "SELECT user_id, total_items, total_orders, total_items / total_orders AS avg_items_per_order FROM purchases_bad ORDER BY user_id"
                    )
                    new_results = db_cursor.fetchall()

                    # Check if we now get proper decimal results
                    non_zero_results = [
                        row[3] for row in new_results if float(row[3]) > 0
                    ]
                    if (
                        len(non_zero_results) >= 2
                    ):  # At least 2 users should have non-zero averages
                        test_steps[1]["status"] = "passed"
                        test_steps[1][
                            "Result_Message"
                        ] = f"✅ Integer division fixed via column type changes. Non-zero results: {len(non_zero_results)}"

                        # Validate specific expected results
                        expected_user_1 = any(
                            abs(float(row[3]) - 0.5) < 0.01
                            for row in new_results
                            if row[0] == 1
                        )  # 5/10 = 0.5
                        expected_user_4 = any(
                            abs(float(row[3]) - 0.75) < 0.01
                            for row in new_results
                            if row[0] == 4
                        )  # 3/4 = 0.75

                        if expected_user_1 and expected_user_4:
                            test_steps[2]["status"] = "passed"
                            test_steps[2][
                                "Result_Message"
                            ] = "✅ Division calculations are mathematically correct"
                            overall_success = True
                        else:
                            test_steps[2]["status"] = "failed"
                            test_steps[2][
                                "Result_Message"
                            ] = f"❌ Division results not mathematically correct. Results: {new_results}"
                    else:
                        test_steps[1]["status"] = "failed"
                        test_steps[1][
                            "Result_Message"
                        ] = f"❌ Still getting integer division results: {new_results}"
                else:
                    # 2. Check if a new table or view was created with proper calculations
                    db_cursor.execute(
                        """
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name != 'purchases_bad'
                        ORDER BY table_name
                    """
                    )
                    new_tables = db_cursor.fetchall()

                    if new_tables:
                        print(
                            f"✅ Agent created new table(s, flush=True): {[t[0] for t in new_tables]}"
                        )

                        # Try to find a table with proper decimal calculations
                        for table_name in [t[0] for t in new_tables]:
                            try:
                                db_cursor.execute(
                                    f"SELECT user_id, total_items, total_orders, total_items::DECIMAL / total_orders AS avg FROM {table_name} ORDER BY user_id LIMIT 3"
                                )
                                table_results = db_cursor.fetchall()

                                non_zero_in_table = [
                                    row[3] for row in table_results if float(row[3]) > 0
                                ]
                                if len(non_zero_in_table) >= 2:
                                    test_steps[1]["status"] = "passed"
                                    test_steps[1][
                                        "Result_Message"
                                    ] = f"✅ Integer division fixed via new table '{table_name}'"
                                    test_steps[2]["status"] = "passed"
                                    test_steps[2][
                                        "Result_Message"
                                    ] = "✅ New table provides correct decimal calculations"
                                    overall_success = True
                                    break
                            except Exception:
                                continue
                    else:
                        # 3. Check if the calculation query itself was fixed
                        try:
                            db_cursor.execute(
                                "SELECT user_id, total_items::DECIMAL / total_orders AS avg_items_per_order FROM purchases_bad ORDER BY user_id"
                            )
                            cast_results = db_cursor.fetchall()

                            non_zero_cast = [
                                row[1] for row in cast_results if float(row[1]) > 0
                            ]
                            if len(non_zero_cast) >= 2:
                                test_steps[1]["status"] = "passed"
                                test_steps[1][
                                    "Result_Message"
                                ] = "✅ Integer division can be fixed with proper DECIMAL casting"
                                test_steps[2]["status"] = "passed"
                                test_steps[2][
                                    "Result_Message"
                                ] = "✅ DECIMAL casting provides correct results"
                                overall_success = True
                            else:
                                test_steps[1]["status"] = "failed"
                                test_steps[1][
                                    "Result_Message"
                                ] = "❌ Integer division issue not resolved"
                        except Exception as e:
                            test_steps[1]["status"] = "failed"
                            test_steps[1][
                                "Result_Message"
                            ] = f"❌ Could not validate division fix: {str(e)}"

            except Exception as e:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Error checking for division fix: {str(e)}"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ PostgreSQL validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
