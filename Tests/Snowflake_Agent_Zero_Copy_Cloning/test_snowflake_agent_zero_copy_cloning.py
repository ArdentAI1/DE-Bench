# Braintrust-only Snowflake test - Zero-Copy Cloning for Development Environments
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
    This Snowflake test validates that AI can implement zero-copy cloning for dev environments.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_zero_copy_test_{test_timestamp}_{test_uuid}",
        "database": f"PROD_DB_{test_timestamp}_{test_uuid}",
        "schema": f"PRODUCTION_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/zero_copy_setup.sql",
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
    Validates that the AI agent successfully implemented zero-copy cloning
    for creating isolated development environments.

    Expected behavior:
    - Clone databases should be created instantly using zero-copy technology
    - Clones should be independent (changes in one don't affect others)
    - Storage should only be consumed for modified data (copy-on-write)
    - Clone management procedures should be implemented

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
            "description": "AI Agent executes zero-copy cloning task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the cloning task...",
        },
        {
            "name": "Production Database Validation",
            "description": "Verify production database has sufficient sample data",
            "status": "running",
            "Result_Message": "Validating production database setup...",
        },
        {
            "name": "Clone Database Creation",
            "description": "Verify development environment clones were created",
            "status": "running",
            "Result_Message": "Checking for clone database creation...",
        },
        {
            "name": "Data Isolation Testing",
            "description": "Test that clones are independent from production",
            "status": "running",
            "Result_Message": "Testing data isolation between environments...",
        },
        {
            "name": "Clone Management Implementation",
            "description": "Verify clone lifecycle management features",
            "status": "running",
            "Result_Message": "Validating clone management procedures...",
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

            # Step 2: Validate production database has sufficient sample data
            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.CUSTOMERS"
            )
            customer_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.PRODUCTS"
            )
            product_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT COUNT(*) FROM {database_name}.{schema_name}.TRANSACTIONS"
            )
            transaction_count = cursor.fetchone()[0]

            if (
                customer_count >= 100
                and product_count >= 10
                and transaction_count >= 100
            ):
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Production data validated: {customer_count} customers, {product_count} products, {transaction_count} transactions"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Insufficient production data: {customer_count} customers, {product_count} products, {transaction_count} transactions"

            # Step 3: Check for clone database creation
            cursor.execute("SHOW DATABASES")
            all_databases = cursor.fetchall()

            database_names = [
                db[1] for db in all_databases
            ]  # Database name is in second column
            clone_databases = []

            for db_name in database_names:
                if any(
                    env in db_name.upper()
                    for env in ["DEV", "TEST", "STAGING", "CLONE"]
                ):
                    clone_databases.append(db_name)

            # Also check for clone-related objects or procedures
            clone_procedures = []
            try:
                cursor.execute(
                    f"""
                    SELECT procedure_name
                    FROM {database_name}.information_schema.procedures
                    WHERE procedure_schema = '{schema_name}'
                    AND (UPPER(procedure_name) LIKE '%CLONE%' OR UPPER(procedure_name) LIKE '%REFRESH%')
                """
                )
                clone_procedures = cursor.fetchall()
            except Exception:
                # Fallback to SHOW PROCEDURES
                try:
                    cursor.execute(
                        f"SHOW PROCEDURES IN SCHEMA {database_name}.{schema_name}"
                    )
                    all_procedures = cursor.fetchall()
                    clone_procedures = [
                        proc
                        for proc in all_procedures
                        if any(
                            keyword in proc[0].upper()
                            for keyword in ["CLONE", "REFRESH"]
                        )
                    ]
                except Exception:
                    clone_procedures = []

            if clone_databases or clone_procedures:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Clone infrastructure found: {len(clone_databases)} clone databases, {len(clone_procedures)} clone procedures"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = "❌ No evidence of clone database creation or clone management"

            # Step 4: Test data isolation by checking for CLONE_METADATA table or similar tracking
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {database_name}.{schema_name}.CLONE_METADATA"
                )
                clone_metadata_count = cursor.fetchone()[0]

                try:
                    cursor.execute(
                        f"""
                        SELECT column_name 
                        FROM {database_name}.information_schema.columns 
                        WHERE table_schema = '{schema_name}' AND table_name = 'CLONE_METADATA'
                    """
                    )
                    metadata_columns = [row[0] for row in cursor.fetchall()]
                except Exception:
                    # Fallback to DESCRIBE TABLE
                    try:
                        cursor.execute(
                            f"DESCRIBE TABLE {database_name}.{schema_name}.CLONE_METADATA"
                        )
                        desc_results = cursor.fetchall()
                        metadata_columns = [row[0] for row in desc_results]
                    except Exception:
                        metadata_columns = []

                expected_metadata_cols = ["CLONE_NAME", "SOURCE_DATABASE", "CREATED_AT"]
                has_metadata_structure = all(
                    col in metadata_columns for col in expected_metadata_cols
                )

                if has_metadata_structure:
                    test_steps[3]["status"] = "passed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"✅ Clone tracking implemented with metadata table containing {len(metadata_columns)} columns"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"❌ Clone metadata table missing or incomplete structure"

            except Exception:
                # If CLONE_METADATA doesn't exist, check for other evidence of isolation testing
                summary_views = []
                try:
                    cursor.execute(
                        f"""
                        SELECT view_name 
                        FROM {database_name}.information_schema.views
                        WHERE table_schema = '{schema_name}'
                        AND (UPPER(view_name) LIKE '%SUMMARY%' OR UPPER(view_name) LIKE '%BUSINESS%')
                    """
                    )
                    summary_views = cursor.fetchall()
                except Exception:
                    # Fallback to SHOW VIEWS
                    try:
                        cursor.execute(
                            f"SHOW VIEWS IN SCHEMA {database_name}.{schema_name}"
                        )
                        all_views = cursor.fetchall()
                        summary_views = [
                            view
                            for view in all_views
                            if any(
                                keyword in view[0].upper()
                                for keyword in ["SUMMARY", "BUSINESS"]
                            )
                        ]
                    except Exception:
                        summary_views = []

                if summary_views:
                    test_steps[3]["status"] = "passed"
                    test_steps[3][
                        "Result_Message"
                    ] = f"✅ Data isolation testing setup with {len(summary_views)} comparison views"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3][
                        "Result_Message"
                    ] = "❌ No evidence of data isolation testing or comparison mechanisms"

            # Step 5: Check for clone management implementation
            # Look for stored procedures, functions, or scripts for clone lifecycle management
            total_procedures = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.information_schema.procedures
                    WHERE procedure_schema = '{schema_name}'
                """
                )
                total_procedures = cursor.fetchone()[0]
            except Exception:
                try:
                    cursor.execute(
                        f"SHOW PROCEDURES IN SCHEMA {database_name}.{schema_name}"
                    )
                    procedures = cursor.fetchall()
                    total_procedures = len(procedures)
                except Exception:
                    total_procedures = 0

            total_functions = 0
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {database_name}.information_schema.functions
                    WHERE function_schema = '{schema_name}'
                """
                )
                total_functions = cursor.fetchone()[0]
            except Exception:
                try:
                    cursor.execute(
                        f"SHOW FUNCTIONS IN SCHEMA {database_name}.{schema_name}"
                    )
                    functions = cursor.fetchall()
                    total_functions = len(functions)
                except Exception:
                    total_functions = 0

            # Check for tables that might support clone management
            management_tables = []
            try:
                cursor.execute(
                    f"""
                    SELECT table_name
                    FROM {database_name}.information_schema.tables
                    WHERE table_schema = '{schema_name}'
                    AND (UPPER(table_name) LIKE '%CLONE%' OR UPPER(table_name) LIKE '%METADATA%' OR UPPER(table_name) LIKE '%MANAGEMENT%')
                """
                )
                management_tables = cursor.fetchall()
            except Exception:
                try:
                    cursor.execute(
                        f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}"
                    )
                    all_tables = cursor.fetchall()
                    management_tables = [
                        table
                        for table in all_tables
                        if any(
                            keyword in table[1].upper()
                            for keyword in ["CLONE", "METADATA", "MANAGEMENT"]
                        )
                    ]
                except Exception:
                    management_tables = []

            if total_procedures > 0 or total_functions > 0 or management_tables:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Clone management infrastructure: {total_procedures} procedures, {total_functions} functions, {len(management_tables)} management tables"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = "❌ No clone management infrastructure found (procedures, functions, or management tables)"

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
