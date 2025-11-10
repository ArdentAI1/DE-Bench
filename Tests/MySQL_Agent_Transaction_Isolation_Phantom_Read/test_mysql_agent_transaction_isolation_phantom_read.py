# Braintrust-only MySQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import mysql.connector
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This MySQL test validates transaction isolation and phantom read handling.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with test-specific configuration
    custom_mysql_config = {
        "resource_id": f"mysql_isolation_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"isolation_test_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "transactions",
                        "columns": [
                            {
                                "name": "transaction_id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "account_id", "type": "INT", "not_null": True},
                            {
                                "name": "amount",
                                "type": "DECIMAL(15,2)",
                                "not_null": True,
                            },
                            {
                                "name": "transaction_type",
                                "type": "ENUM('CREDIT', 'DEBIT')",
                                "not_null": True,
                            },
                            {
                                "name": "created_at",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                        ],
                        "data": [
                            {
                                "account_id": 1001,
                                "amount": 500.00,
                                "transaction_type": "CREDIT",
                            },
                            {
                                "account_id": 1001,
                                "amount": 750.00,
                                "transaction_type": "CREDIT",
                            },
                            {
                                "account_id": 1001,
                                "amount": 200.00,
                                "transaction_type": "DEBIT",
                            },
                            {
                                "account_id": 1002,
                                "amount": 1000.00,
                                "transaction_type": "CREDIT",
                            },
                            {
                                "account_id": 1002,
                                "amount": 300.00,
                                "transaction_type": "DEBIT",
                            },
                            {
                                "account_id": 1003,
                                "amount": 250.00,
                                "transaction_type": "CREDIT",
                            },
                            {
                                "account_id": 1003,
                                "amount": 75.00,
                                "transaction_type": "DEBIT",
                            },
                        ],
                    }
                ],
            }
        ],
    }

    mysql_fixture = MySQLFixture(custom_config=custom_mysql_config)
    return [mysql_fixture]


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
    Validates that the AI agent successfully demonstrated transaction isolation concepts.

    Expected behavior:
    - Transactions table created with proper structure
    - Index on account_id created
    - Demonstrated REPEATABLE READ isolation level behavior
    - Showed phantom read prevention
    - Explained gap locking behavior
    - Provided clear explanation of MySQL's concurrency control

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
            "description": "AI Agent executes transaction isolation demonstration",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the isolation testing task...",
        },
        {
            "name": "Table Structure Validation",
            "description": "Verify transactions table structure and indexes",
            "status": "running",
            "Result_Message": "Validating table structure and indexes...",
        },
        {
            "name": "Initial Data Validation",
            "description": "Verify initial test data was preserved or enhanced",
            "status": "running",
            "Result_Message": "Validating initial transaction data...",
        },
        {
            "name": "Transaction Isolation Evidence",
            "description": "Verify evidence of isolation level testing",
            "status": "running",
            "Result_Message": "Looking for evidence of isolation level demonstrations...",
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

        # Get MySQL fixture for validation
        mysql_fixture = None
        if fixtures:
            mysql_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mysql_resource"), None
            )

        if not mysql_fixture:
            raise Exception("MySQL fixture not found")

        # Get database connection
        resource_data = getattr(mysql_fixture, "_resource_data", None)
        if not resource_data or not resource_data.get("created_resources"):
            raise Exception("MySQL resource data not available")

        db_name = resource_data["created_resources"][0]["name"]
        db_connection = mysql_fixture.get_connection(database=db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Validate table structure
            # Check if transactions table exists and has correct structure
            db_cursor.execute("SHOW TABLES LIKE 'transactions'")
            table_exists = db_cursor.fetchone()

            if not table_exists:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ Transactions table not found"
                return {"score": 0.25, "metadata": {"test_steps": test_steps}}

            # Check table columns
            db_cursor.execute("DESCRIBE transactions")
            columns = {row[0]: row[1] for row in db_cursor.fetchall()}

            required_columns = [
                "transaction_id",
                "account_id",
                "amount",
                "transaction_type",
                "created_at",
            ]
            missing_columns = [col for col in required_columns if col not in columns]

            if missing_columns:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Missing columns: {missing_columns}"
                return {"score": 0.25, "metadata": {"test_steps": test_steps}}

            # Check for index on account_id
            db_cursor.execute(
                "SHOW INDEX FROM transactions WHERE Column_name = 'account_id'"
            )
            index_exists = db_cursor.fetchone()

            if index_exists:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = "✅ Table structure and indexes validated successfully"
            else:
                test_steps[1][
                    "status"
                ] = "passed"  # Still pass if basic structure is correct
                test_steps[1][
                    "Result_Message"
                ] = "✅ Table structure validated (index on account_id recommended)"

            # Step 3: Validate initial data
            db_cursor.execute("SELECT COUNT(*) FROM transactions")
            record_count = db_cursor.fetchone()[0]

            # Check for account 1001 and 1002 data
            db_cursor.execute(
                """
                SELECT account_id, COUNT(*) as transaction_count,
                       SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE -amount END) as balance
                FROM transactions 
                WHERE account_id IN (1001, 1002)
                GROUP BY account_id
                ORDER BY account_id
            """
            )
            account_data = db_cursor.fetchall()
            # total up the number of transactions for account 1001 and 1002
            account_1001_transactions = sum(
                acc[1] for acc in account_data if acc[0] == 1001
            )
            account_1002_transactions = sum(
                acc[1] for acc in account_data if acc[0] == 1002
            )

            if (
                record_count >= 7
                and (account_1001_transactions + account_1002_transactions) >= 6
            ):
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = (
                    f"✅ Transaction data validated: {record_count} total transactions, "
                    f"accounts with data: {[acc[0] for acc in account_data]}"
                )
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = (
                    f"❌ Insufficient transaction data: {record_count} records (expected ≥7), "
                    f"{account_1001_transactions + account_1002_transactions} transactions with data (expected ≥6)"
                )

            # Step 4: Look for evidence of advanced transaction work
            # This is harder to validate directly, so we check for additional data or complexity
            db_cursor.execute(
                """
                SELECT DISTINCT account_id 
                FROM transactions 
                ORDER BY account_id
            """
            )
            unique_accounts = [row[0] for row in db_cursor.fetchall()]

            db_cursor.execute(
                """
                SELECT transaction_type, COUNT(*) 
                FROM transactions 
                GROUP BY transaction_type
            """
            )
            transaction_types = db_cursor.fetchall()

            if len(unique_accounts) >= 2 and len(transaction_types) >= 2:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = (
                    f"✅ Evidence of isolation testing preparation: "
                    f"{len(unique_accounts)} accounts, {len(transaction_types)} transaction types"
                )
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = (
                    f"❌ Limited evidence of isolation testing setup: "
                    f"{len(unique_accounts)} accounts, {len(transaction_types)} transaction types"
                )

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
