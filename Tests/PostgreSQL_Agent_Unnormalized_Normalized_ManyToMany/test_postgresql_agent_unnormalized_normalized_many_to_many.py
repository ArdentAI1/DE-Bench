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
        "resource_id": f"unnormalized_normalized_many_to_many_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"authors_schema_test_db_{test_timestamp}_{test_uuid}_{test_timestamp}_{test_uuid}",
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
            # Step 2: Demonstrate the normalization problem first
            print("🔍 Analyzing the unnormalized schema problem...")
            db_cursor.execute(
                "SELECT book_id, title, authors FROM books_bad ORDER BY book_id"
            )
            original_data = db_cursor.fetchall()

            print("Original unnormalized data:")
            for row in original_data:
                print(f"  Book {row[0]}: '{row[1]}' by {row[2]}")

            # Demonstrate the problem: searching for 'Gamma' doesn't show co-authors properly
            db_cursor.execute(
                "SELECT title, authors FROM books_bad WHERE authors LIKE '%Gamma%'"
            )
            gamma_books = db_cursor.fetchall()
            print(f"Books by Gamma (showing incomplete author info): {gamma_books}")

            # Step 3: Check if the agent created a normalized schema
            print("🔍 Checking for normalized schema...")

            # Check what tables exist
            db_cursor.execute(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """
            )
            all_tables = [row[0] for row in db_cursor.fetchall()]
            print(f"Available tables: {all_tables}")

            # Look for signs of normalization
            normalized_tables = []
            junction_tables = []

            # Check for books table (normalized)
            if "books" in all_tables:
                normalized_tables.append("books")
                print("✅ Found normalized 'books' table")

            # Check for authors table
            if "authors" in all_tables:
                normalized_tables.append("authors")
                print("✅ Found 'authors' table")

            # Check for junction table (various naming patterns)
            junction_patterns = [
                "book_authors",
                "books_authors",
                "author_books",
                "book_author",
            ]
            for pattern in junction_patterns:
                if pattern in all_tables:
                    junction_tables.append(pattern)
                    print(f"✅ Found junction table '{pattern}'")

            if len(normalized_tables) >= 2 and junction_tables:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Normalized schema created with tables: {normalized_tables + junction_tables}"

                # Step 4: Validate that all author information is preserved
                print("🔍 Validating data preservation and queryability...")

                try:
                    junction_table = junction_tables[
                        0
                    ]  # Use the first junction table found

                    # Discover actual column names from schema
                    db_cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'books'
                        AND column_name LIKE '%id%'
                        ORDER BY ordinal_position
                        LIMIT 1
                    """)
                    books_pk = db_cursor.fetchone()
                    books_pk_col = books_pk[0] if books_pk else 'id'

                    # Discover book title column
                    db_cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'books'
                        AND column_name LIKE '%title%'
                        ORDER BY ordinal_position
                        LIMIT 1
                    """)
                    book_title_result = db_cursor.fetchone()
                    book_title_col = book_title_result[0] if book_title_result else 'title'

                    db_cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'authors'
                        AND column_name LIKE '%id%'
                        ORDER BY ordinal_position
                        LIMIT 1
                    """)
                    authors_pk = db_cursor.fetchone()
                    authors_pk_col = authors_pk[0] if authors_pk else 'id'

                    # Discover author name column
                    db_cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'authors'
                        AND column_name LIKE '%name%'
                        ORDER BY ordinal_position
                        LIMIT 1
                    """)
                    author_name_result = db_cursor.fetchone()
                    author_name_col = author_name_result[0] if author_name_result else 'name'

                    # Discover junction table foreign keys
                    db_cursor.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{junction_table}'
                        AND column_name LIKE '%book%'
                    """)
                    junction_book_fk = db_cursor.fetchone()
                    junction_book_col = junction_book_fk[0] if junction_book_fk else 'book_id'

                    db_cursor.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{junction_table}'
                        AND column_name LIKE '%author%'
                    """)
                    junction_author_fk = db_cursor.fetchone()
                    junction_author_col = junction_author_fk[0] if junction_author_fk else 'author_id'

                    # Test query to get all authors for each book
                    db_cursor.execute(
                        f"""
                        SELECT b.{book_title_col}, a.{author_name_col} as author_name
                        FROM books b
                        JOIN {junction_table} ba ON b.{books_pk_col} = ba.{junction_book_col}
                        JOIN authors a ON ba.{junction_author_col} = a.{authors_pk_col}
                        ORDER BY b.{book_title_col}, a.{author_name_col}
                    """
                    )
                    normalized_results = db_cursor.fetchall()

                    print("Normalized query results:")
                    current_book = None
                    authors_for_book = []
                    for row in normalized_results:
                        if current_book != row[0]:
                            if current_book:
                                print(
                                    f"  '{current_book}' by {', '.join(authors_for_book)}"
                                )
                            current_book = row[0]
                            authors_for_book = [row[1]]
                        else:
                            authors_for_book.append(row[1])
                    if current_book:
                        print(f"  '{current_book}' by {', '.join(authors_for_book)}")

                    # Check that we can properly query for Gamma's books and see all co-authors
                    db_cursor.execute(
                        f"""
                        SELECT DISTINCT b.{book_title_col},
                               string_agg(a2.{author_name_col}, ', ') as all_authors
                        FROM books b
                        JOIN {junction_table} ba1 ON b.{books_pk_col} = ba1.{junction_book_col}
                        JOIN authors a1 ON ba1.{junction_author_col} = a1.{authors_pk_col}
                        JOIN {junction_table} ba2 ON b.{books_pk_col} = ba2.{junction_book_col}
                        JOIN authors a2 ON ba2.{junction_author_col} = a2.{authors_pk_col}
                        WHERE a1.{author_name_col} LIKE '%Gamma%'
                        GROUP BY b.{book_title_col}
                    """
                    )
                    gamma_normalized = db_cursor.fetchall()

                    if gamma_normalized:
                        print(f"Gamma's books with all co-authors: {gamma_normalized}")

                        # Check if we have complete author information
                        has_design_patterns = any(
                            "Design Patterns" in row[0] for row in gamma_normalized
                        )
                        has_multiple_authors = any(
                            "," in row[1]
                            or "Others" in row[1]
                            or len(row[1].split()) > 2
                            for row in gamma_normalized
                        )

                        if has_design_patterns and has_multiple_authors:
                            test_steps[2]["status"] = "passed"
                            test_steps[2][
                                "Result_Message"
                            ] = "✅ Normalized schema preserves all author relationships and enables proper querying"
                            overall_success = True
                        else:
                            test_steps[2]["status"] = "failed"
                            test_steps[2][
                                "Result_Message"
                            ] = f"❌ Author relationships not fully preserved. Gamma results: {gamma_normalized}"
                    else:
                        test_steps[2]["status"] = "failed"
                        test_steps[2][
                            "Result_Message"
                        ] = "❌ Could not find Gamma's books in normalized schema"

                except psycopg2.Error as e:
                    db_connection.rollback()
                    test_steps[2]["status"] = "failed"
                    test_steps[2][
                        "Result_Message"
                    ] = f"❌ Error validating normalized schema: {str(e)}"
                except Exception as e:
                    test_steps[2]["status"] = "failed"
                    test_steps[2][
                        "Result_Message"
                    ] = f"❌ Error validating normalized schema: {str(e)}"

            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Normalized schema not created. Found tables: {all_tables}"

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
