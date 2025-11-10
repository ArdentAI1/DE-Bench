# Braintrust-only PostgreSQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import psycopg2
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
    This PostgreSQL test validates full-text search implementation.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"fulltext_search_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"product_catalog_db_{test_timestamp}_{test_uuid}",
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
    Validates that the AI agent successfully implemented full-text search.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes full-text search implementation",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "tsvector Column Creation",
            "description": "Verify tsvector column added and populated",
            "status": "running",
            "Result_Message": "Validating tsvector column...",
        },
        {
            "name": "GIN Index Validation",
            "description": "Verify GIN index created and being used",
            "status": "running",
            "Result_Message": "Checking GIN index...",
        },
        {
            "name": "Basic Search Functionality",
            "description": "Test basic search returns relevant results",
            "status": "running",
            "Result_Message": "Testing basic search...",
        },
        {
            "name": "Advanced Search Features",
            "description": "Test phrase search and filtering",
            "status": "running",
            "Result_Message": "Testing advanced search features...",
        },
        {
            "name": "Search Ranking Quality",
            "description": "Verify search result ranking",
            "status": "running",
            "Result_Message": "Validating search ranking...",
        },
        {
            "name": "Trigger Maintenance",
            "description": "Verify tsvector auto-updates on INSERT/UPDATE",
            "status": "running",
            "Result_Message": "Testing trigger functionality...",
        },
        {
            "name": "Performance Validation",
            "description": "Verify search performance meets requirements",
            "status": "running",
            "Result_Message": "Measuring search performance...",
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
            # Step 2: Verify tsvector column creation
            print("🔍 Checking tsvector column...", flush=True)

            # Check if tsvector column exists
            db_cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'products' 
                AND data_type = 'tsvector'
            """
            )
            tsvector_columns = db_cursor.fetchall()

            if len(tsvector_columns) > 0:
                tsvector_col_name = tsvector_columns[0][0]

                # Check if tsvector is populated
                db_cursor.execute(
                    f"SELECT COUNT(*) FROM products WHERE {tsvector_col_name} IS NOT NULL"
                )
                populated_count = db_cursor.fetchone()[0]

                db_cursor.execute("SELECT COUNT(*) FROM products")
                total_count = db_cursor.fetchone()[0]

                if populated_count == total_count and total_count > 0:
                    test_steps[1]["status"] = "passed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"✅ tsvector column '{tsvector_col_name}' created and populated ({populated_count}/{total_count} products)"
                else:
                    test_steps[1]["status"] = "failed"
                    test_steps[1][
                        "Result_Message"
                    ] = f"❌ tsvector column exists but not fully populated ({populated_count}/{total_count})"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = "❌ No tsvector column found on products table"

            # Step 3: Verify GIN index
            print("🔍 Checking GIN index...", flush=True)

            db_cursor.execute(
                """
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = 'products' 
                AND indexdef LIKE '%gin%'
            """
            )
            gin_indexes = db_cursor.fetchall()

            if len(gin_indexes) > 0:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ GIN index created: {gin_indexes[0][0]}"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = "❌ No GIN index found on products table"

            # Step 4: Test basic search functionality
            print("🔍 Testing basic search...", flush=True)

            # Try to find search function or perform direct search
            search_results = []
            search_worked = False

            # Try using search function if it exists
            db_cursor.execute(
                """
                SELECT routine_name 
                FROM information_schema.routines 
                WHERE routine_name LIKE '%search%product%'
                OR routine_name LIKE '%search%'
                LIMIT 1
            """
            )
            search_function = db_cursor.fetchone()

            if search_function:
                try:
                    db_cursor.execute(
                        f"SELECT * FROM {search_function[0]}('laptop') LIMIT 5"
                    )
                    search_results = db_cursor.fetchall()
                    search_worked = len(search_results) > 0
                except Exception as e:
                    print(f"Search function error: {e}", flush=True)

            # Fallback: Try direct tsvector search
            if not search_worked and len(tsvector_columns) > 0:
                try:
                    tsvector_col = tsvector_columns[0][0]
                    db_cursor.execute(
                        f"""
                        SELECT id, name 
                        FROM products 
                        WHERE {tsvector_col} @@ to_tsquery('english', 'laptop')
                        LIMIT 5
                    """
                    )
                    search_results = db_cursor.fetchall()
                    search_worked = len(search_results) > 0
                except Exception as e:
                    print(f"Direct search error: {e}", flush=True)

            if search_worked:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Basic search working - found {len(search_results)} results for 'laptop'"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = "❌ Search functionality not working or returns no results"

            # Step 5: Test advanced search features
            print("🔍 Testing advanced search features...", flush=True)

            advanced_features_count = 0

            # Check for phrase search support
            if len(tsvector_columns) > 0:
                tsvector_col = tsvector_columns[0][0]
                try:
                    db_cursor.execute(
                        f"""
                        SELECT COUNT(*) 
                        FROM products 
                        WHERE {tsvector_col} @@ phraseto_tsquery('english', 'red leather')
                    """
                    )
                    phrase_count = db_cursor.fetchone()[0]
                    if phrase_count > 0:
                        advanced_features_count += 1
                except:
                    pass

            # Check for filtering function
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.routines 
                WHERE routine_name LIKE '%search%'
                AND routine_name LIKE '%advanced%'
                OR routine_type = 'FUNCTION'
            """
            )
            has_advanced_function = db_cursor.fetchone()[0] > 0
            if has_advanced_function:
                advanced_features_count += 1

            if advanced_features_count >= 1:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Advanced search features implemented ({advanced_features_count} features detected)"
            else:
                test_steps[4]["status"] = "partial"
                test_steps[4][
                    "Result_Message"
                ] = "⚠️ Limited advanced search features detected"

            # Step 6: Verify search ranking
            print("🔍 Checking search ranking...", flush=True)

            if len(tsvector_columns) > 0 and search_worked:
                tsvector_col = tsvector_columns[0][0]
                try:
                    # Search for "macbook" - should rank exact matches highest
                    db_cursor.execute(
                        f"""
                        SELECT name, ts_rank_cd({tsvector_col}, to_tsquery('english', 'macbook')) as rank
                        FROM products 
                        WHERE {tsvector_col} @@ to_tsquery('english', 'macbook')
                        ORDER BY rank DESC
                        LIMIT 3
                    """
                    )
                    ranked_results = db_cursor.fetchall()

                    if len(ranked_results) > 0:
                        # Check if results have different ranks
                        ranks = [r[1] for r in ranked_results]
                        has_ranking = len(set(ranks)) > 1 or ranks[0] > 0

                        if has_ranking:
                            test_steps[5]["status"] = "passed"
                            test_steps[5][
                                "Result_Message"
                            ] = f"✅ Search ranking implemented - found {len(ranked_results)} ranked results"
                        else:
                            test_steps[5]["status"] = "partial"
                            test_steps[5][
                                "Result_Message"
                            ] = "⚠️ Search works but ranking may not be optimized"
                    else:
                        test_steps[5]["status"] = "failed"
                        test_steps[5][
                            "Result_Message"
                        ] = "❌ No ranked results returned"
                except Exception as e:
                    test_steps[5]["status"] = "failed"
                    test_steps[5][
                        "Result_Message"
                    ] = f"❌ Ranking query failed: {str(e)}"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5][
                    "Result_Message"
                ] = "❌ Cannot test ranking without working search"

            # Step 7: Test trigger maintenance
            print("🔍 Testing trigger maintenance...", flush=True)

            if len(tsvector_columns) > 0:
                tsvector_col = tsvector_columns[0][0]
                try:
                    # Insert a new product
                    db_cursor.execute(
                        """
                        INSERT INTO products (name, description, category, price, brand)
                        VALUES ('Test Search Product', 'Testing full text search triggers', 'Test', 99.99, 'TestBrand')
                        RETURNING id
                    """
                    )
                    new_id = db_cursor.fetchone()[0]
                    db_connection.commit()

                    # Check if tsvector was auto-populated
                    db_cursor.execute(
                        f"""
                        SELECT {tsvector_col} IS NOT NULL as has_tsvector
                        FROM products
                        WHERE id = %s
                    """,
                        (new_id,),
                    )
                    has_tsvector = db_cursor.fetchone()[0]

                    # Clean up test product
                    db_cursor.execute("DELETE FROM products WHERE id = %s", (new_id,))
                    db_connection.commit()

                    if has_tsvector:
                        test_steps[6]["status"] = "passed"
                        test_steps[6][
                            "Result_Message"
                        ] = "✅ Trigger maintains tsvector on INSERT"
                    else:
                        test_steps[6]["status"] = "failed"
                        test_steps[6][
                            "Result_Message"
                        ] = "❌ tsvector not auto-populated on INSERT"
                except Exception as e:
                    test_steps[6]["status"] = "failed"
                    test_steps[6][
                        "Result_Message"
                    ] = f"❌ Trigger test failed: {str(e)}"
            else:
                test_steps[6]["status"] = "failed"
                test_steps[6][
                    "Result_Message"
                ] = "❌ Cannot test triggers without tsvector column"

            # Step 8: Performance validation
            print("🔍 Measuring search performance...", flush=True)

            if search_worked and len(tsvector_columns) > 0:
                tsvector_col = tsvector_columns[0][0]
                try:
                    # Measure search query time
                    import time as time_module

                    start_time = time_module.time()

                    db_cursor.execute(
                        f"""
                        SELECT id, name
                        FROM products 
                        WHERE {tsvector_col} @@ to_tsquery('english', 'laptop | phone | shoes')
                        LIMIT 20
                    """
                    )
                    results = db_cursor.fetchall()

                    end_time = time_module.time()
                    query_time_ms = (end_time - start_time) * 1000

                    # Check EXPLAIN to verify index usage
                    db_cursor.execute(
                        f"""
                        EXPLAIN SELECT id, name
                        FROM products 
                        WHERE {tsvector_col} @@ to_tsquery('english', 'laptop')
                    """
                    )
                    explain_output = db_cursor.fetchall()
                    uses_index = any(
                        "Index Scan" in str(row) or "Bitmap" in str(row)
                        for row in explain_output
                    )

                    if query_time_ms < 100 and uses_index:
                        test_steps[7]["status"] = "passed"
                        test_steps[7][
                            "Result_Message"
                        ] = f"✅ Search performance excellent: {query_time_ms:.2f}ms, uses index"
                    elif uses_index:
                        test_steps[7]["status"] = "passed"
                        test_steps[7][
                            "Result_Message"
                        ] = f"✅ Search uses index: {query_time_ms:.2f}ms"
                    else:
                        test_steps[7]["status"] = "partial"
                        test_steps[7][
                            "Result_Message"
                        ] = f"⚠️ Search works ({query_time_ms:.2f}ms) but may not use index optimally"
                except Exception as e:
                    test_steps[7]["status"] = "failed"
                    test_steps[7][
                        "Result_Message"
                    ] = f"❌ Performance test failed: {str(e)}"
            else:
                test_steps[7]["status"] = "failed"
                test_steps[7][
                    "Result_Message"
                ] = "❌ Cannot measure performance without working search"

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
