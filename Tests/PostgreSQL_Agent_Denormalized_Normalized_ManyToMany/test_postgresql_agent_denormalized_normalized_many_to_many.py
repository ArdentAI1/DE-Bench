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
    "resource_id": f"denormalized_normalized_many_to_many_{test_timestamp}_{test_uuid}",
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
def test_postgresql_agent_denormalized_normalized_many_to_many(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can transform denormalized author data into properly normalized many-to-many relationships."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Denormalized Problem Demonstration",
            "description": "Verify the current denormalized schema demonstrates the co-authorship issue",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Agent Normalization Process",
            "description": "AI Agent analyzes denormalized data and implements normalized solution",
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
            "description": "Verify co-authors are now properly separated (denormalization issue resolved)",
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

        # DEMONSTRATE THE DENORMALIZED PROBLEM FIRST (Layer 1: Basic validation)
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()
        
        # Demonstrate the denormalization problem
        db_cursor.execute("SELECT title, authors FROM books_bad WHERE authors ILIKE '%Gamma%'")
        problem_result = db_cursor.fetchall()
        
        if len(problem_result) == 1 and 'Gamma,Others' in str(problem_result[0]):
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Denormalized problem confirmed: Co-authors bundled as comma-separated string: {problem_result}"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Denormalized problem demonstration failed: {problem_result}"
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
        test_steps[1]["Result_Message"] = "AI Agent completed denormalized → normalized transformation"

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
            # Layer 1: Basic existence checks - Did agent create normalized tables?
            db_cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name != 'books_bad'
                ORDER BY table_name
            """)
            
            new_tables = [row[0] for row in db_cursor.fetchall()]
            
            if len(new_tables) >= 2:  # Expecting normalized structure (e.g., authors, books, junction)
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"Normalized structure created with tables: {new_tables}"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Insufficient normalization. New tables: {new_tables}"
                raise AssertionError("Agent did not create properly normalized structure")
            
            # Layer 2: Content validation - Is original data preserved in normalized form?
            books_found = False
            authors_found = False
            
            for table in new_tables:
                try:
                    db_cursor.execute(f"SELECT * FROM {table} LIMIT 10")
                    sample_data = db_cursor.fetchall()
                    
                    # Check for book titles
                    if any('Design Patterns' in str(row) or 'Clean Code' in str(row) for row in sample_data):
                        books_found = True
                    
                    # Check for author names  
                    if any('Gamma' in str(row) or 'Robert Martin' in str(row) or 'Others' in str(row) for row in sample_data):
                        authors_found = True
                        
                except Exception as e:
                    pass  # Table query failed, likely expected
                    continue
            
            if books_found and authors_found:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = "Data preservation verified: Books and authors found in normalized structure"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = f"Data preservation failed during normalization: books={books_found}, authors={authors_found}"
                raise AssertionError("Original data not preserved during denormalized → normalized transformation")
            
            # Layer 3: Functional validation - THE CRITICAL TEST
            # Are co-authors properly separated (denormalization resolved)?
            co_authors_separated = False
            separation_evidence = ""
            
            for table in new_tables:
                try:
                    db_cursor.execute(f"SELECT * FROM {table}")
                    all_data = db_cursor.fetchall()
                    
                    # Look for evidence that "Gamma" and "Others" exist as separate entities
                    gamma_rows = [row for row in all_data if 'Gamma' in str(row)]
                    others_rows = [row for row in all_data if 'Others' in str(row)]
                    
                    # Critical check: Are Gamma and Others in separate rows (not bundled)?
                    gamma_separate = any('Gamma' in str(row) and 'Others' not in str(row) for row in gamma_rows)
                    others_separate = any('Others' in str(row) and 'Gamma' not in str(row) for row in others_rows)
                    
                    if gamma_separate and others_separate:
                        co_authors_separated = True
                        separation_evidence = f"Table '{table}': Gamma and Others found as separate normalized entities"
                        break
                        
                except Exception as e:
                    continue
            
            if co_authors_separated:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"SUCCESS: Denormalized → Normalized transformation complete! {separation_evidence}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = "Co-authors still appear bundled - denormalized → normalized transformation incomplete"
                raise AssertionError("Core denormalization issue not resolved - many-to-many relationships still bundled")
            
            # Final success
                    # Test completed successfully - denormalized co-authorship issue resolved
            assert True, "Denormalized → Normalized many-to-many transformation successful - co-authors properly separated"
        
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