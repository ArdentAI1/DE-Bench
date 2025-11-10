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
    This PostgreSQL test validates that AI can add multiple records with proper relationships.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"add_multiple_record_postgresql_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"add_multiple_record_test_db_{test_timestamp}_{test_uuid}",
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
    Validates that the AI agent can add multiple related records to PostgreSQL.

    Expected behavior:
    - AI should add Alice Green to users table
    - AI should create linked customer, order, and payment records
    - AI should preserve existing data integrity

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
            "description": "AI Agent executes task to add new records",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the PostgreSQL record addition task...",
        },
        {
            "name": "Record Insertion Validation",
            "description": "Verify that Alice Green was added to the users table & her orders are accessible",
            "status": "running",
            "Result_Message": "Validating that Alice Green was added with proper relationships...",
        },
        {
            "name": "Data Integrity Validation",
            "description": "Verify that existing records were not modified",
            "status": "running",
            "Result_Message": "Validating that existing records remain unchanged...",
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

        # Connect to database to verify results
        db_connection = postgres_fixture.get_connection(created_db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2a: Verify Alice Green was added to users
            db_cursor.execute(
                "SELECT id, name, email, age FROM users WHERE name = 'Alice Green'"
            )
            alice_record = db_cursor.fetchone()

            if alice_record and alice_record[1:] == (
                "Alice Green",
                "alice@example.com",
                28,
            ):
                alice_user_id = alice_record[0]
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Alice Green record found with correct data: {alice_record}"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Alice Green record incorrect or missing. Found: {alice_record}"
                raise Exception("Agent failed to insert Alice Green correctly")

            # Step 2b: Verify linked customer record
            db_cursor.execute(
                "SELECT id, user_id, phone, address FROM customers WHERE user_id = %s",
                (alice_user_id,),
            )
            customer_record = db_cursor.fetchone()

            if customer_record and customer_record[2:] == (
                "111-222-3333",
                "101 Elm St, Springfield",
            ):
                alice_customer_id = customer_record[0]
            else:
                raise Exception(
                    f"Customer record for Alice missing/incorrect: {customer_record}"
                )

            # Step 2c: Verify linked order
            db_cursor.execute(
                "SELECT id, customer_id, total_amount, status FROM orders WHERE customer_id = %s",
                (alice_customer_id,),
            )
            order_record = db_cursor.fetchone()

            if order_record and order_record[2:] == (320.00, "Processing"):
                alice_order_id = order_record[0]
            else:
                raise Exception(
                    f"Order record for Alice missing/incorrect: {order_record}"
                )

            # Step 2d: Verify linked payment
            db_cursor.execute(
                "SELECT id, order_id, amount, method, status FROM payments WHERE order_id = %s",
                (alice_order_id,),
            )
            payment_record = db_cursor.fetchone()

            if payment_record and payment_record[2:] == (
                320.00,
                "Credit Card",
                "Completed",
            ):
                pass
            else:
                raise Exception(
                    f"Payment record for Alice missing/incorrect: {payment_record}"
                )

            # Step 3: Verify original records are intact
            db_cursor.execute(
                "SELECT name, email, age FROM users WHERE name IN ('John Doe', 'Jane Smith', 'Bob Johnson') ORDER BY name"
            )
            original_records = db_cursor.fetchall()

            expected_original = [
                ("Bob Johnson", "bob@example.com", 35),
                ("Jane Smith", "jane@example.com", 25),
                ("John Doe", "john@example.com", 30),
            ]

            if original_records == expected_original:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = "✅ Original records preserved correctly"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Original records modified. Expected: {expected_original}, Got: {original_records}"
                raise Exception("Agent modified existing records incorrectly")

            # Final verification: Total record count should be 4
            db_cursor.execute("SELECT COUNT(*) FROM users")
            total_count = db_cursor.fetchone()[0]

            if total_count == 4:
                # Test completed successfully - Alice Green added without modifying existing records
                overall_success = True
                print(
                    "✅ Add Multiple Records to PostgreSQL Agent test passed - records inserted correctly",
                    flush=True,
                )
            else:
                raise Exception(
                    f"Unexpected record count. Expected 4, got {total_count}"
                )

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
