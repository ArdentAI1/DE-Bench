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
    This MySQL test validates bulk data ingestion with LOAD DATA INFILE and edge cases.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with test-specific configuration
    custom_mysql_config = {
        "resource_id": f"mysql_bulk_ingest_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"bulk_ingest_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "products",
                        "columns": [
                            {
                                "name": "product_id",
                                "type": "INT",
                                "primary_key": True,
                            },
                            {"name": "name", "type": "VARCHAR(255)"},
                            {"name": "description", "type": "TEXT"},
                            {"name": "price", "type": "DECIMAL(10,2)"},
                            {"name": "category", "type": "VARCHAR(100)"},
                            {
                                "name": "created_at",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                        ],
                        # Initial product data - agent will add more via LOAD DATA INFILE
                        "data": [
                            {
                                "product_id": 1,
                                "name": "Basic Widget",
                                "description": "A simple widget for basic needs",
                                "price": 19.99,
                                "category": "Widgets",
                            },
                            {
                                "product_id": 2,
                                "name": "Premium Gadget",
                                "description": "High-quality gadget with advanced features",
                                "price": 99.99,
                                "category": "Gadgets",
                            },
                            {
                                "product_id": 3,
                                "name": "Standard Tool",
                                "description": "Reliable tool for everyday use",
                                "price": 45.50,
                                "category": "Tools",
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
    Validates that the AI agent successfully performed bulk data ingestion with LOAD DATA INFILE.

    Expected behavior:
    - CSV file created with sample data including edge cases
    - Products table populated via LOAD DATA INFILE
    - UTF-8 characters preserved
    - Duplicate records handled appropriately
    - NULL values correctly represented

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
            "description": "AI Agent executes bulk data ingestion task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the bulk import task...",
        },
        {
            "name": "Data Import Validation",
            "description": "Verify that data was imported into products table",
            "status": "running",
            "Result_Message": "Validating that data was successfully imported...",
        },
        {
            "name": "Edge Cases Handling",
            "description": "Verify UTF-8 chars, quotes, and special cases handled",
            "status": "running",
            "Result_Message": "Validating edge cases were handled correctly...",
        },
        {
            "name": "Data Integrity Check",
            "description": "Verify data types and constraints are correct",
            "status": "running",
            "Result_Message": "Validating data integrity and types...",
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
            # Step 2: Verify data was imported (should be more than initial 3 records)
            db_cursor.execute("SELECT COUNT(*) FROM products")
            record_count = db_cursor.fetchone()[0]

            if record_count > 3:  # Initial data had 3 records
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Data successfully imported: {record_count} total records (started with 3)"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ No additional data imported: only {record_count} records found (expected > 3)"
                return {"score": 0.25, "metadata": {"test_steps": test_steps}}

            # Step 3: Check for edge cases handling
            # Look for records with special characters, quotes, or NULL values
            db_cursor.execute(
                """
                SELECT name, description, price 
                FROM products 
                WHERE description LIKE '%,%' 
                   OR description LIKE '%"%' 
                   OR description LIKE '%''%'
                   OR name REGEXP '[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]'
                   OR price IS NULL
                ORDER BY product_id
                LIMIT 5
            """
            )
            edge_case_records = db_cursor.fetchall()

            if edge_case_records and len(edge_case_records) > 0:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Edge cases handled correctly: found {len(edge_case_records)} records with special characters/cases"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = "❌ No edge cases found - may indicate encoding or parsing issues"

            # Step 4: Data integrity checks
            # Check that data types are correct and no obvious corruption
            db_cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT product_id) as unique_products,
                    SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) as valid_prices,
                    SUM(CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END) as valid_names
                FROM products
            """
            )
            integrity_check = db_cursor.fetchone()

            total_records, unique_products, valid_prices, valid_names = integrity_check

            if total_records > 0 and valid_names > 0:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = (
                    f"✅ Data integrity validated: {total_records} total, "
                    f"{unique_products} unique products, {valid_prices} valid prices, "
                    f"{valid_names} valid names"
                )
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = (
                    f"❌ Data integrity issues: {total_records} total, "
                    f"{valid_names} valid names, {valid_prices} valid prices"
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
