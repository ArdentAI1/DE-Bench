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
    This PostgreSQL test validates that AI can fix missing users in JOIN queries.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"missing_users_fix_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"users_subs_test_db_{test_timestamp}_{test_uuid}",
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
    Validates that the AI agent can fix schema design issues causing missing users in queries.

    Expected behavior:
    - AI should identify the INNER JOIN issue causing users without subscriptions to disappear
    - AI should implement proper LEFT JOIN or similar solution
    - AI should preserve existing data relationships
    - All users should be visible in queries after the fix

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Missing Users Problem Demonstration",
            "description": "Verify the current schema demonstrates users disappearing from INNER JOIN queries",
            "status": "running",
            "Result_Message": "Demonstrating the missing users problem with current schema...",
        },
        {
            "name": "Agent Analysis and Fix",
            "description": "AI Agent analyzes the schema design issue and implements proper relational database solution",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the schema fix task...",
        },
        {
            "name": "Schema Design Validation",
            "description": "Verify the agent created proper normalized schema with FK constraints and optional relationships",
            "status": "running",
            "Result_Message": "Validating proper schema design with normalized relationships...",
        },
        {
            "name": "Data Preservation Validation",
            "description": "Verify all original users are preserved and queryable with proper LEFT JOIN logic",
            "status": "running",
            "Result_Message": "Validating that all users are preserved and queryable...",
        },
    ]

    overall_success = False

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[1]["status"] = "failed"
            test_steps[1][
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

        test_steps[1]["status"] = "passed"
        test_steps[1][
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
            # Step 2: Demonstrate the missing users problem first
            print("🔍 Step 1: Demonstrating missing users problem...", flush=True)

            # Check initial user count
            db_cursor.execute("SELECT COUNT(*) FROM users")
            total_users = db_cursor.fetchone()[0]
            print(f"Total users in database: {total_users}", flush=True)

            # Check users with subscriptions via INNER JOIN (problematic query)
            db_cursor.execute(
                """
                SELECT COUNT(DISTINCT u.id) 
                FROM users u 
                INNER JOIN subscriptions s ON u.id = s.user_id
            """
            )
            users_with_subs_inner = db_cursor.fetchone()[0]
            print(f"Users visible with INNER JOIN: {users_with_subs_inner}", flush=True)

            if users_with_subs_inner < total_users:
                test_steps[0]["status"] = "passed"
                test_steps[0][
                    "Result_Message"
                ] = f"✅ Missing users problem demonstrated: {total_users} total users, only {users_with_subs_inner} visible with INNER JOIN"
            else:
                test_steps[0]["status"] = "failed"
                test_steps[0][
                    "Result_Message"
                ] = f"❌ Missing users problem not demonstrated: All {total_users} users are visible"

            # Step 3: Validate that the agent's fix shows all users
            print("🔍 Step 3: Validating agent's fix...", flush=True)

            # Try to find a query/view/function that shows all users with subscription data
            # This could be a new view, a corrected query, or a stored procedure

            # Check for common solution patterns:
            # 1. LEFT JOIN query
            try:
                db_cursor.execute(
                    """
                    SELECT COUNT(DISTINCT u.id) 
                    FROM users u 
                    LEFT JOIN subscriptions s ON u.id = s.user_id
                """
                )
                users_with_left_join = db_cursor.fetchone()[0]

                if users_with_left_join == total_users:
                    test_steps[2]["status"] = "passed"
                    test_steps[2][
                        "Result_Message"
                    ] = f"✅ Schema fix validated: LEFT JOIN shows all {total_users} users"

                    # Validate data preservation
                    db_cursor.execute("SELECT name FROM users ORDER BY name")
                    user_names = [row[0] for row in db_cursor.fetchall()]

                    if len(user_names) >= 3:  # Assuming at least 3 users from schema
                        test_steps[3]["status"] = "passed"
                        test_steps[3][
                            "Result_Message"
                        ] = f"✅ Data preservation validated: {len(user_names)} users preserved"
                        overall_success = True
                    else:
                        test_steps[3]["status"] = "failed"
                        test_steps[3][
                            "Result_Message"
                        ] = f"❌ Data preservation failed: Only {len(user_names)} users found"
                else:
                    test_steps[2]["status"] = "failed"
                    test_steps[2][
                        "Result_Message"
                    ] = f"❌ Schema fix incomplete: LEFT JOIN shows {users_with_left_join} users, expected {total_users}"

            except Exception as e:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Schema validation failed: {str(e)}"

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
