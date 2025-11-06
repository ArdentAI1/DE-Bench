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
    This MySQL test validates SCD Type 2 implementation for tracking customer changes.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with SCD2 table structure and initial customer data
    custom_mysql_config = {
        "resource_id": f"mysql_scd2_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"scd2_test_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "customers_scd2",
                        "columns": [
                            {
                                "name": "customer_key",
                                "type": "BIGINT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "customer_id", "type": "INT", "not_null": True},
                            {"name": "first_name", "type": "VARCHAR(100)"},
                            {"name": "last_name", "type": "VARCHAR(100)"},
                            {"name": "email", "type": "VARCHAR(255)"},
                            {"name": "phone", "type": "VARCHAR(20)"},
                            {"name": "address", "type": "VARCHAR(500)"},
                            {"name": "city", "type": "VARCHAR(100)"},
                            {"name": "state", "type": "VARCHAR(50)"},
                            {
                                "name": "subscription_plan",
                                "type": "ENUM('BASIC', 'PREMIUM', 'ENTERPRISE')",
                            },
                            {
                                "name": "effective_start_date",
                                "type": "DATE",
                                "not_null": True,
                            },
                            {
                                "name": "effective_end_date",
                                "type": "DATE",
                                "default": "'9999-12-31'",
                            },
                            {
                                "name": "is_current",
                                "type": "BOOLEAN",
                                "default": "TRUE",
                            },
                            {
                                "name": "created_timestamp",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                            {
                                "name": "updated_timestamp",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                            },
                        ],
                        # Initial customer data in SCD2 format - all current records
                        "data": [
                            {
                                "customer_id": 1001,
                                "first_name": "John",
                                "last_name": "Doe",
                                "email": "john.doe@email.com",
                                "phone": "555-0101",
                                "address": "123 Main St",
                                "city": "New York",
                                "state": "NY",
                                "subscription_plan": "BASIC",
                                "effective_start_date": "2024-01-01",
                                "effective_end_date": "9999-12-31",
                                "is_current": True,
                            },
                            {
                                "customer_id": 1002,
                                "first_name": "Jane",
                                "last_name": "Smith",
                                "email": "jane.smith@email.com",
                                "phone": "555-0102",
                                "address": "456 Oak Ave",
                                "city": "Los Angeles",
                                "state": "CA",
                                "subscription_plan": "PREMIUM",
                                "effective_start_date": "2024-01-01",
                                "effective_end_date": "9999-12-31",
                                "is_current": True,
                            },
                            {
                                "customer_id": 1003,
                                "first_name": "Bob",
                                "last_name": "Johnson",
                                "email": "bob.johnson@email.com",
                                "phone": "555-0103",
                                "address": "789 Pine Rd",
                                "city": "Chicago",
                                "state": "IL",
                                "subscription_plan": "ENTERPRISE",
                                "effective_start_date": "2024-01-01",
                                "effective_end_date": "9999-12-31",
                                "is_current": True,
                            },
                            {
                                "customer_id": 1004,
                                "first_name": "Alice",
                                "last_name": "Williams",
                                "email": "alice.williams@email.com",
                                "phone": "555-0104",
                                "address": "321 Elm St",
                                "city": "Houston",
                                "state": "TX",
                                "subscription_plan": "BASIC",
                                "effective_start_date": "2024-01-01",
                                "effective_end_date": "9999-12-31",
                                "is_current": True,
                            },
                            {
                                "customer_id": 1005,
                                "first_name": "Charlie",
                                "last_name": "Brown",
                                "email": "charlie.brown@email.com",
                                "phone": "555-0105",
                                "address": "654 Maple Ave",
                                "city": "Phoenix",
                                "state": "AZ",
                                "subscription_plan": "PREMIUM",
                                "effective_start_date": "2024-01-01",
                                "effective_end_date": "9999-12-31",
                                "is_current": True,
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
    Validates that the AI agent successfully implemented SCD Type 2 pattern.

    Expected behavior:
    - customers_scd2 table created with proper SCD2 structure
    - Historical tracking with effective dates and current flags
    - Staging table for processing updates
    - SCD2 logic implemented (stored procedures or complex queries)
    - Demonstration of historical preservation and current state tracking

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
            "description": "AI Agent executes SCD2 implementation",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the SCD2 task...",
        },
        {
            "name": "SCD2 Table Structure",
            "description": "Verify customers_scd2 table with proper SCD2 columns",
            "status": "running",
            "Result_Message": "Validating SCD2 table structure...",
        },
        {
            "name": "Initial Customer Data",
            "description": "Verify initial customer data with current flags",
            "status": "running",
            "Result_Message": "Validating initial customer data...",
        },
        {
            "name": "Historical Tracking",
            "description": "Verify SCD2 logic creates historical records",
            "status": "running",
            "Result_Message": "Validating historical record tracking...",
        },
        {
            "name": "SCD2 Processing Logic",
            "description": "Verify stored procedures or views for SCD2 operations",
            "status": "running",
            "Result_Message": "Validating SCD2 processing infrastructure...",
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
            # Step 2: Validate SCD2 table structure
            db_cursor.execute("SHOW TABLES LIKE 'customers_scd2'")
            table_exists = db_cursor.fetchone()

            if not table_exists:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ customers_scd2 table not found"
                return {"score": 0.2, "metadata": {"test_steps": test_steps}}

            # Check SCD2 columns
            db_cursor.execute("DESCRIBE customers_scd2")
            columns = {row[0]: row[1] for row in db_cursor.fetchall()}

            scd2_required_columns = [
                "customer_key",
                "customer_id",
                "effective_start_date",
                "effective_end_date",
                "is_current",
            ]
            business_columns = ["first_name", "last_name", "email"]

            missing_scd2_columns = [
                col for col in scd2_required_columns if col not in columns
            ]
            missing_business_columns = [
                col for col in business_columns if col not in columns
            ]

            if not missing_scd2_columns and not missing_business_columns:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ SCD2 table structure validated: {len(columns)} columns including all SCD2 fields"
            else:
                test_steps[1]["status"] = "failed"
                missing_all = missing_scd2_columns + missing_business_columns
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Missing required columns: {missing_all}"
                return {"score": 0.2, "metadata": {"test_steps": test_steps}}

            # Step 3: Validate initial customer data
            db_cursor.execute("SELECT COUNT(*) FROM customers_scd2")
            total_records = db_cursor.fetchone()[0]

            db_cursor.execute(
                "SELECT COUNT(*) FROM customers_scd2 WHERE is_current = TRUE"
            )
            current_records = db_cursor.fetchone()[0]

            db_cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM customers_scd2")
            unique_customers = db_cursor.fetchone()[0]

            if total_records >= 5 and current_records >= 5 and unique_customers >= 5:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = (
                    f"✅ Initial customer data validated: {total_records} total records, "
                    f"{current_records} current records, {unique_customers} unique customers"
                )
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = (
                    f"❌ Insufficient customer data: {total_records} total, "
                    f"{current_records} current, {unique_customers} unique"
                )

            # Step 4: Check for historical tracking evidence
            # Look for customers with multiple records (indicating SCD2 processing)
            db_cursor.execute(
                """
                SELECT customer_id, COUNT(*) as record_count,
                       SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) as current_count,
                       SUM(CASE WHEN is_current = FALSE THEN 1 ELSE 0 END) as historical_count
                FROM customers_scd2
                GROUP BY customer_id
                HAVING COUNT(*) > 1
                ORDER BY record_count DESC
                LIMIT 5
            """
            )

            customers_with_history = db_cursor.fetchall()

            if customers_with_history and len(customers_with_history) > 0:
                test_steps[3]["status"] = "passed"
                total_historical = sum(row[3] for row in customers_with_history)
                test_steps[3]["Result_Message"] = (
                    f"✅ Historical tracking validated: {len(customers_with_history)} customers "
                    f"with history, {total_historical} historical records"
                )
            else:
                # Check if we have proper SCD2 structure even without processed updates
                db_cursor.execute(
                    """
                    SELECT COUNT(*) 
                    FROM customers_scd2 
                    WHERE effective_end_date != '9999-12-31' OR is_current = FALSE
                """
                )
                processed_records = db_cursor.fetchone()[0]

                if processed_records > 0:
                    test_steps[3]["status"] = "passed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"✅ SCD2 processing evidence found: {processed_records} processed records"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3][
                        "Result_Message"
                    ] = "❌ No evidence of SCD2 processing - all records appear to be initial inserts"

            # Step 5: Check for SCD2 processing infrastructure
            infrastructure_components = []

            # Check for staging table
            db_cursor.execute("SHOW TABLES LIKE 'customers_staging'")
            if db_cursor.fetchone():
                infrastructure_components.append("staging table")

            # Check for stored procedures
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.routines 
                WHERE routine_schema = %s 
                AND routine_type = 'PROCEDURE'
            """,
                (db_name,),
            )

            procedure_count = db_cursor.fetchone()[0]
            if procedure_count > 0:
                infrastructure_components.append(f"{procedure_count} stored procedures")

            # Check for views
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.views 
                WHERE table_schema = %s
            """,
                (db_name,),
            )

            view_count = db_cursor.fetchone()[0]
            if view_count > 0:
                infrastructure_components.append(f"{view_count} views")

            if len(infrastructure_components) >= 2:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ SCD2 infrastructure implemented: {', '.join(infrastructure_components)}"
            elif len(infrastructure_components) >= 1:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Basic SCD2 infrastructure: {', '.join(infrastructure_components)}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = "❌ No SCD2 processing infrastructure found"

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
