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
        "resource_id": f"add_record_postgresql_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"add_record_test_db_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
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
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        print("🔍 Model result:", flush=True)
        print(model_result, flush=True)

        print("🔍 Fixtures:", flush=True)
        print(fixtures, flush=True)

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
            print("❌ PostgreSQL fixture not found", flush=True)
            raise Exception("PostgreSQL fixture not found")

        # Get PostgreSQL resource data from fixture
        resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not resource_data:
            print("❌ PostgreSQL resource data not available", flush=True)
            raise Exception("PostgreSQL resource data not available")

        created_resources = resource_data["created_resources"]
        created_db_name = created_resources[0]["name"]

        # Connect to database for validation
        db_connection = postgres_fixture.get_connection(created_db_name)
        db_cursor = db_connection.cursor()

        print("🔍 Database connection:", flush=True)
        print(db_connection, flush=True)
        print("🔍 Database cursor:", flush=True)
        print(db_cursor, flush=True)

        try:
            # Step 2: Validate that Alice Green was added correctly
            print("🔍 Checking if Alice Green was added...", flush=True)
            db_cursor.execute(
                "SELECT id, name, email, age FROM users WHERE name = 'Alice Green'"
            )
            alice_record = db_cursor.fetchone()

            if alice_record and alice_record[1:] == (
                "Alice Green",
                "alice@example.com",
                28,
            ):
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

            # Step 3: Verify original records are preserved
            print("🔍 Checking that original records are preserved...", flush=True)
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
                # Test completed successfully
                overall_success = True
                print(
                    "✅ Add Record to PostgreSQL Agent test passed - record inserted correctly"
                , flush=True)
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

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
