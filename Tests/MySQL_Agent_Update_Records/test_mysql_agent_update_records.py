# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
import os
import importlib
import pytest
import time
import mysql.connector
import uuid

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.mysql
@pytest.mark.database
@pytest.mark.code_writing
@pytest.mark.two
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("mysql_resource", [{
    "resource_id": f"update_records_mysql_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"update_records_test_db_{test_timestamp}_{test_uuid}",
            "sql_file": "mysql_schema.sql"
        }
    ]
}], indirect=True)
def test_mysql_agent_update_records(request, mysql_resource, supabase_account_resource):
    """Test that validates AI agent can update records in MySQL database via fixture."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to update user ages",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Age Update Validation", 
            "description": "Verify that users over 30 were updated to age 35",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Younger Users Unchanged",
            "description": "Verify that users 30 and under were not modified",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    created_db_name = mysql_resource["created_resources"][0]["name"]
    print(f"OLD FORMAT mysql_resource: {mysql_resource}")  # Add this line
    # Database: {created_db_name}
    
    try:
        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["mysql"]["databases"] = [{"name": created_db_name}]
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
        db_connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USERNAME"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=created_db_name,
            connect_timeout=10,
        )
        db_cursor = db_connection.cursor()
        
        try:
            # Step 2: Verify users over 30 were updated to 35
            db_cursor.execute("SELECT name, age FROM users WHERE name IN ('John Doe', 'Bob Johnson') ORDER BY name")
            older_users = db_cursor.fetchall()
            
            expected_older = [("Bob Johnson", 35), ("John Doe", 35)]
            
            if older_users == expected_older:
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"Users over 30 correctly updated to age 35: {older_users}"
            else:
                test_steps[1]["status"] = "failed" 
                test_steps[1]["Result_Message"] = f"Age updates incorrect. Expected: {expected_older}, Got: {older_users}"
                raise AssertionError("Agent failed to update ages correctly")
            
            # Step 3: Verify users 30 and under were not changed
            db_cursor.execute("SELECT name, age FROM users WHERE name IN ('Jane Smith', 'Carol White') ORDER BY name")
            younger_users = db_cursor.fetchall()
            
            expected_younger = [("Carol White", 29), ("Jane Smith", 25)]
            
            if younger_users == expected_younger:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"Younger users correctly unchanged: {younger_users}"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Younger users modified incorrectly. Expected: {expected_younger}, Got: {younger_users}"
                raise AssertionError("Agent incorrectly modified younger users")
            
            # Final verification: Check all records
            db_cursor.execute("SELECT name, age FROM users ORDER BY name")
            all_users = db_cursor.fetchall()
            
            expected_final = [
                ("Bob Johnson", 35),   # Was 38, updated to 35
                ("Carol White", 29),   # Was 29, unchanged
                ("Jane Smith", 25),    # Was 25, unchanged  
                ("John Doe", 35)       # Was 32, updated to 35
            ]
            
            if all_users == expected_final:
                    # Test completed successfully - users over 30 updated to age 35
                assert True, "Update Records in MySQL Agent test passed - ages updated correctly"
            else:
                raise AssertionError(f"Final state incorrect. Expected: {expected_final}, Got: {all_users}")
        
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