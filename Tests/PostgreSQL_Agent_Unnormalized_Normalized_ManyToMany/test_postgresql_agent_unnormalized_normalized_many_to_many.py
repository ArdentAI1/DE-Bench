# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
import os
import importlib
import pytest
import time
import psycopg2
import uuid

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.postgresql
@pytest.mark.database
@pytest.mark.schema_design
@pytest.mark.three
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"unnormalized_normalized_many_to_many_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"authors_schema_test_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "books_bad",
                    "columns": [
                        {"name": "book_id", "type": "INT", "primary_key": True},
                        {"name": "title", "type": "TEXT", "not_null": True},
                        {"name": "authors", "type": "TEXT", "not_null": True}
                    ],
                    "data": [
                        {"book_id": 1, "title": "Design Patterns", "authors": "Gamma,Others"},
                        {"book_id": 2, "title": "Clean Code", "authors": "Robert Martin"}
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_postgresql_agent_unnormalized_normalized_many_to_many(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can transform unnormalized (1NF violation) author data into properly normalized many-to-many relationships."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Unnormalized (1NF) Problem Demonstration",
            "description": "Verify the current unnormalized schema (1NF violation) demonstrates the co-authorship issue",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Agent Normalization Process",
            "description": "AI Agent analyzes unnormalized data and implements normalized solution",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Normalized Structure Validation",
            "description": "Verify the agent created a properly normalized database structure",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Preservation Validation", 
            "description": "Verify all original book and author data was preserved during transformation",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Co-Author Separation Validation",
            "description": "Verify co-authors are now properly separated (1NF issue resolved)",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    created_db_name = postgres_resource["created_resources"][0]["name"]
    # Database: {created_db_name}
    
    try:
        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]
        config_results = set_up_model_configs(
            Configs=test_configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # DEMONSTRATE THE UNNORMALIZED (1NF) PROBLEM FIRST (Layer 1: Basic validation)
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()
        
        # Demonstrate the 1NF violation problem (multi-valued attribute in single column)
        db_cursor.execute("SELECT title, authors FROM books_bad WHERE authors ILIKE '%Gamma%'")
        problem_result = db_cursor.fetchall()
        
        if len(problem_result) == 1 and 'Gamma,Others' in str(problem_result[0]):
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Unnormalized (1NF) problem confirmed: Co-authors bundled as comma-separated string: {problem_result}"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Unnormalized (1NF) problem demonstration failed: {problem_result}"
            raise AssertionError("Initial denormalized problem setup validation failed")
        
        db_cursor.close()
        db_connection.close()

        # Running model on database: {created_db_name}

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=test_configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        
        # Model run completed in {end_time - start_time:.2f} seconds

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "AI Agent completed unnormalized (1NF) → normalized transformation"

        # SECTION 3: VERIFY THE OUTCOMES
        
        # Reconnect to verify the agent's normalized solution
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()
        
        try:
            # Strict schema validation: Require exact target tables
            db_cursor.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='public' AND table_name IN ('books','authors','book_authors')
                ORDER BY table_name
                """
            )
            required_tables = [r[0] for r in db_cursor.fetchall()]
            assert required_tables == ['authors', 'book_authors', 'books'], f"Expected normalized tables not found. Got: {required_tables}"
            test_steps[2]["status"] = "passed"
            test_steps[2]["Result_Message"] = f"Found expected normalized tables: {required_tables}"

            # Columns in junction table (at minimum must include these two integer columns)
            db_cursor.execute(
                """
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name='book_authors' ORDER BY column_name
                """
            )
            ba_columns = [tuple(r) for r in db_cursor.fetchall()]
            assert ('author_id', 'integer') in ba_columns and ('book_id', 'integer') in ba_columns, f"book_authors must include integer author_id and book_id. Got: {ba_columns}"

            # Foreign keys on junction table
            db_cursor.execute(
                """
                SELECT tc.table_name, kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name=tc.constraint_name
                WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_name='book_authors'
                ORDER BY kcu.column_name
                """
            )
            fks = [tuple(r) for r in db_cursor.fetchall()]
            assert ('book_authors','author_id','authors','author_id') in fks, f"Missing FK book_authors.author_id -> authors.author_id. FKs: {fks}"
            assert ('book_authors','book_id','books','book_id') in fks, f"Missing FK book_authors.book_id -> books.book_id. FKs: {fks}"

            # Unique constraint on authors.name
            db_cursor.execute(
                """
                SELECT tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name
                WHERE tc.table_name='authors' AND tc.constraint_type='UNIQUE' AND kcu.column_name='name'
                """
            )
            unique_on_name = db_cursor.fetchall()
            assert len(unique_on_name) >= 1, "authors.name must be UNIQUE"

            # Data checks: exact counts and sets
            db_cursor.execute("SELECT COUNT(*) FROM books")
            assert db_cursor.fetchone()[0] == 2, "books count must be 2"

            db_cursor.execute("SELECT name FROM authors ORDER BY name")
            assert [r[0] for r in db_cursor.fetchall()] == ['Gamma','Others','Robert Martin'], "authors set mismatch"

            # Junction correctness for specific titles
            db_cursor.execute(
                """
                SELECT a.name FROM book_authors ba
                JOIN books b ON b.book_id=ba.book_id
                JOIN authors a ON a.author_id=ba.author_id
                WHERE b.title='Design Patterns' ORDER BY a.name
                """
            )
            assert [r[0] for r in db_cursor.fetchall()] == ['Gamma','Others'], "Design Patterns authors mismatch"

            db_cursor.execute(
                """
                SELECT a.name FROM book_authors ba
                JOIN books b ON b.book_id=ba.book_id
                JOIN authors a ON a.author_id=ba.author_id
                WHERE b.title='Clean Code' ORDER BY a.name
                """
            )
            assert [r[0] for r in db_cursor.fetchall()] == ['Robert Martin'], "Clean Code authors mismatch"

            # No commas in any author names
            db_cursor.execute("SELECT COUNT(*) FROM authors WHERE name LIKE '%,%'")
            assert db_cursor.fetchone()[0] == 0, "authors names should not contain commas"

            test_steps[3]["status"] = "passed"
            test_steps[3]["Result_Message"] = "Schema and data validation passed (tables, FKs, uniques, exact rows)"

            # Idempotency: capture counts, run agent again, ensure stability
            db_cursor.execute("SELECT COUNT(*) FROM authors")
            authors_count_1 = db_cursor.fetchone()[0]
            db_cursor.execute("SELECT COUNT(*) FROM book_authors")
            book_authors_count_1 = db_cursor.fetchone()[0]

            # Ensure original denormalized table remains intact
            db_cursor.execute("SELECT COUNT(*) FROM books_bad")
            books_bad_count_1 = db_cursor.fetchone()[0]

            db_cursor.close()
            db_connection.close()

            # Run model again
            run_model(
                container=None,
                task=Test_Configs.User_Input,
                configs=test_configs,
                extra_information={
                    "useArdent": True,
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                },
            )

            # Reconnect and re-check counts
            db_connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOSTNAME"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USERNAME"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=created_db_name,
                sslmode="require",
            )
            db_cursor = db_connection.cursor()

            db_cursor.execute("SELECT COUNT(*) FROM authors")
            authors_count_2 = db_cursor.fetchone()[0]
            db_cursor.execute("SELECT COUNT(*) FROM book_authors")
            book_authors_count_2 = db_cursor.fetchone()[0]
            db_cursor.execute("SELECT COUNT(*) FROM books_bad")
            books_bad_count_2 = db_cursor.fetchone()[0]

            assert authors_count_1 == authors_count_2, "authors count changed after second run"
            assert book_authors_count_1 == book_authors_count_2, "book_authors count changed after second run"
            assert books_bad_count_1 == books_bad_count_2 == 2, "books_bad should remain intact with 2 rows"

            # Mark final step as passed
            # Ensure the last test step reflects successful co-author separation and idempotency
            test_steps[4]["status"] = "passed"
            test_steps[4]["Result_Message"] = "Many-to-many mapping correct and idempotent; source table preserved"

            # Final assertion to make test outcome explicit
            assert True, "Unnormalized (1NF) → Normalized many-to-many transformation validated rigorously"
        
        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Update any remaining test steps that didn't reach
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        raise
    
    finally:
        # CLEANUP
        if config_results:
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            ) 