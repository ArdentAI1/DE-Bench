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
    This PostgreSQL test validates JSONB ingestion and schema evolution handling.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"jsonb_schema_evolution_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"jsonb_ingestion_db_{test_timestamp}_{test_uuid}",
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
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented JSONB ingestion with schema evolution.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes JSONB ingestion task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "JSONB Data Processing",
            "description": "Verify JSONB data extraction and processing across schema versions",
            "status": "running",
            "Result_Message": "Validating JSONB data processing...",
        },
        {
            "name": "Schema Evolution Handling",
            "description": "Verify system handles different schema versions gracefully",
            "status": "running",
            "Result_Message": "Testing schema evolution handling...",
        },
        {
            "name": "Field Extraction Consistency",
            "description": "Verify consistent field extraction despite format differences",
            "status": "running",
            "Result_Message": "Validating field extraction logic...",
        },
        {
            "name": "Performance Optimization",
            "description": "Verify proper JSONB indexing and query patterns",
            "status": "running",
            "Result_Message": "Checking performance optimization...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
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
        postgres_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"),
                None,
            )
            if fixtures
            else None
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
            # Step 2: Verify JSONB data processing
            print("🔍 Checking JSONB data processing...", flush=True)

            # Check if raw JSONB data exists
            db_cursor.execute("SELECT COUNT(*) FROM raw_product_data")
            raw_count = db_cursor.fetchone()[0]

            # Check if normalized data was extracted
            db_cursor.execute("SELECT COUNT(*) FROM products_normalized")
            normalized_count = db_cursor.fetchone()[0]

            if (
                raw_count >= 5 and normalized_count > 0
            ):  # Should have seed data plus any agent additions
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ JSONB processing working - {raw_count} raw records, {normalized_count} normalized records"
            elif raw_count >= 5:
                test_steps[1]["status"] = "partial"
                test_steps[1][
                    "Result_Message"
                ] = f"⚠️ Raw JSONB data present ({raw_count} records) but normalization incomplete ({normalized_count} records)"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Insufficient JSONB data processing - {raw_count} raw, {normalized_count} normalized"

            # Step 3: Test schema evolution handling
            print("🔍 Testing schema evolution handling...", flush=True)

            # Check if different schema versions are handled
            db_cursor.execute(
                "SELECT schema_version, COUNT(*) FROM raw_product_data GROUP BY schema_version ORDER BY schema_version"
            )
            schema_versions = db_cursor.fetchall()

            # Look for processing of multiple schema versions
            version_count = len([v for v in schema_versions if v[1] > 0])

            if version_count >= 3:  # v1, v2, v3 minimum
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ Multiple schema versions handled: {schema_versions}"
            elif version_count >= 2:
                test_steps[2]["status"] = "partial"
                test_steps[2][
                    "Result_Message"
                ] = f"⚠️ Some schema versions handled: {schema_versions}"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Limited schema evolution handling: {schema_versions}"

            # Step 4: Verify field extraction consistency
            print("🔍 Checking field extraction consistency...", flush=True)

            # Discover actual column names from products_normalized
            db_cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'products_normalized'
                AND column_name NOT IN ('id', 'created_at', 'updated_at')
                ORDER BY ordinal_position
            """
            )
            available_columns = [row[0] for row in db_cursor.fetchall()]

            # Find columns that match semantic intent
            name_col = next(
                (col for col in available_columns if "name" in col.lower()), None
            )
            price_col = next(
                (col for col in available_columns if "price" in col.lower()), None
            )
            category_col = next(
                (col for col in available_columns if "category" in col.lower()), None
            )

            # Build test queries based on discovered columns
            test_queries = []
            if name_col:
                test_queries.append(
                    f"SELECT COUNT(*) FROM products_normalized WHERE {name_col} IS NOT NULL"
                )
            if price_col:
                # Handle both numeric and text price columns
                try:
                    db_cursor.execute(
                        f"SELECT data_type FROM information_schema.columns WHERE table_name = 'products_normalized' AND column_name = '{price_col}'"
                    )
                    price_type = db_cursor.fetchone()
                    if price_type and "numeric" in price_type[0].lower():
                        test_queries.append(
                            f"SELECT COUNT(*) FROM products_normalized WHERE {price_col} > 0"
                        )
                    else:
                        test_queries.append(
                            f"SELECT COUNT(*) FROM products_normalized WHERE {price_col} IS NOT NULL"
                        )
                except:
                    test_queries.append(
                        f"SELECT COUNT(*) FROM products_normalized WHERE {price_col} IS NOT NULL"
                    )
            if category_col:
                test_queries.append(
                    f"SELECT COUNT(*) FROM products_normalized WHERE {category_col} IS NOT NULL"
                )

            # If no semantic columns found, use ANY columns available
            if not test_queries and available_columns:
                test_queries = [
                    f"SELECT COUNT(*) FROM products_normalized WHERE {col} IS NOT NULL"
                    for col in available_columns[:3]
                ]

            extraction_results = []
            for query in test_queries:
                try:
                    db_cursor.execute(query)
                    result = db_cursor.fetchone()[0]
                    extraction_results.append(result)
                except Exception as e:
                    db_connection.rollback()  # Rollback on error to clear transaction state
                    extraction_results.append(0)

            # Should have consistent extraction across different schema formats
            min_extractions = min(extraction_results) if extraction_results else 0

            if (
                min_extractions >= normalized_count * 0.7
            ):  # At least 70% successful extraction
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Consistent field extraction - {extraction_results} successful extractions"
            elif (
                min_extractions >= normalized_count * 0.4
            ):  # At least 40% successful extraction
                test_steps[3]["status"] = "partial"
                test_steps[3][
                    "Result_Message"
                ] = f"⚠️ Partial field extraction - {extraction_results} successful extractions"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ Poor field extraction consistency - {extraction_results} successful extractions"

            # Step 5: Check performance optimization (JSONB indexes)
            print("🔍 Checking performance optimization...", flush=True)

            # Check if GIN indexes exist for JSONB columns
            db_cursor.execute(
                """
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND indexdef LIKE '%gin%' 
                AND tablename LIKE '%product%'
            """
            )
            gin_indexes = db_cursor.fetchall()

            # Test a JSONB query performance (should use index)
            try:
                db_cursor.execute(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT * FROM raw_product_data
                    WHERE product_data @> '{"category": "Electronics"}'
                """
                )
                query_plan = db_cursor.fetchone()
                uses_index = "Index" in str(query_plan) if query_plan else False
            except Exception as e:
                db_connection.rollback()  # Rollback on error
                uses_index = False

            if len(gin_indexes) > 0:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ JSONB performance optimized - {len(gin_indexes)} GIN indexes found"
            elif uses_index:
                test_steps[4]["status"] = "partial"
                test_steps[4][
                    "Result_Message"
                ] = "⚠️ Some indexing present but may not be optimal for JSONB"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = "❌ No JSONB performance optimization detected"

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
