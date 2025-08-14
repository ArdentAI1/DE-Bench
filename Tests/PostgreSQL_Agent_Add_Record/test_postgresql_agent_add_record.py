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
@pytest.mark.code_writing
@pytest.mark.two
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"add_record_postgresql_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"add_record_test_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_postgresql_agent_add_record(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can add a record to PostgreSQL database via fixture."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to add new user record",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Record Insertion Validation", 
            "description": "Verify that Alice Green was added to the users table",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Integrity Validation",
            "description": "Verify that existing records were not modified",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    created_db_name = postgres_resource["created_resources"][0]["name"]
    
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
        
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "AI Agent completed task execution"

        # SECTION 3: VERIFY THE OUTCOMES
        
        # Connect to database to verify results
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
            # Step 2: Verify Alice Green was added
            db_cursor.execute("SELECT name, email, age FROM users WHERE name = 'Alice Green'")
            alice_record = db_cursor.fetchone()
            
            if alice_record and alice_record == ("Alice Green", "alice@example.com", 28):
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"Alice Green record found with correct data: {alice_record}"
            else:
                test_steps[1]["status"] = "failed" 
                test_steps[1]["Result_Message"] = f"Alice Green record incorrect or missing. Found: {alice_record}"
                raise AssertionError("Agent failed to insert Alice Green correctly")
            
            # Step 3: Verify original records are intact
            db_cursor.execute("SELECT name, email, age FROM users WHERE name IN ('John Doe', 'Jane Smith', 'Bob Johnson') ORDER BY name")
            original_records = db_cursor.fetchall()
            
            expected_original = [
                ("Bob Johnson", "bob@example.com", 35),
                ("Jane Smith", "jane@example.com", 25), 
                ("John Doe", "john@example.com", 30)
            ]
            
            if original_records == expected_original:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = "Original records preserved correctly"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Original records modified. Expected: {expected_original}, Got: {original_records}"
                raise AssertionError("Agent modified existing records incorrectly")
            
            # Final verification: Total record count should be 4
            db_cursor.execute("SELECT COUNT(*) FROM users")
            total_count = db_cursor.fetchone()[0]
            
            if total_count == 4:
                    # Test completed successfully - Alice Green added without modifying existing records
                assert True, "Add Record to PostgreSQL Agent test passed - record inserted correctly"
            else:
                raise AssertionError(f"Unexpected record count. Expected 4, got {total_count}")
        
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