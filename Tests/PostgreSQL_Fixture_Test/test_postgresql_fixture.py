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
@pytest.mark.fixture_test
@pytest.mark.one
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"postgresql_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"test_fixture_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "SERIAL", "primary_key": True},
                        {"name": "name", "type": "VARCHAR(100)", "not_null": True},
                        {"name": "email", "type": "VARCHAR(255)", "unique": True, "not_null": True},
                        {"name": "age", "type": "INTEGER"},
                        {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"}
                    ],
                    "data": [
                        {"name": "John Doe", "email": "john@example.com", "age": 30},
                        {"name": "Jane Smith", "email": "jane@example.com", "age": 25},
                        {"name": "Bob Johnson", "email": "bob@example.com", "age": 35}
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_postgresql_fixture_validation(request, postgres_resource):
    """Test that validates PostgreSQL fixture creates and cleans up resources correctly."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Database Creation Validation",
            "description": "Verify that the test database was created",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Table Creation Validation", 
            "description": "Verify that the users table was created with correct schema",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Insertion Validation",
            "description": "Verify that test data was inserted correctly",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Resource Data Validation",
            "description": "Verify that fixture returns correct resource metadata",
            "status": "did not reach", 
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST - No additional setup needed, fixture handles it
    config_results = None
    print(f"PostgreSQL fixture resource: {postgres_resource}")
    
    try:
        # SECTION 2: RUN THE MODEL - Skipped for fixture validation test
        start_time = time.time()
        # (No model execution for fixture validation)
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        
        # SECTION 3: VERIFY THE OUTCOMES
        
        # Step 1: Validate database creation
        try:
            # Get the actual database name from fixture resource
            created_db_name = postgres_resource["created_resources"][0]["name"]
            
            # Connect to the created database
            db_connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOSTNAME"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USERNAME"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=created_db_name,
                sslmode="require",
            )
            db_cursor = db_connection.cursor()
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Successfully connected to {created_db_name} database"
            
        except Exception as e:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Failed to connect to database: {str(e)}"
            raise AssertionError(f"Database creation failed: {str(e)}")
        
        # Step 2: Validate table creation and schema
        try:
            # Check if users table exists and has correct columns
            db_cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'users' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            columns = db_cursor.fetchall()
            
            expected_columns = ['id', 'name', 'email', 'age', 'created_at']
            actual_columns = [col[0] for col in columns]
            
            if all(expected_col in actual_columns for expected_col in expected_columns):
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"Table created with correct schema. Columns: {actual_columns}"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"Table schema mismatch. Expected: {expected_columns}, Got: {actual_columns}"
                raise AssertionError("Table schema validation failed")
                
        except Exception as e:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = f"Table validation failed: {str(e)}"
            raise AssertionError(f"Table creation validation failed: {str(e)}")
        
        # Step 3: Validate data insertion
        try:
            # Check if data was inserted correctly
            db_cursor.execute("SELECT name, email, age FROM users ORDER BY id")
            inserted_data = db_cursor.fetchall()
            
            expected_data = [
                ("John Doe", "john@example.com", 30),
                ("Jane Smith", "jane@example.com", 25),
                ("Bob Johnson", "bob@example.com", 35)
            ]
            
            if inserted_data == expected_data:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"All {len(inserted_data)} records inserted correctly"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Data mismatch. Expected: {expected_data}, Got: {inserted_data}"
                raise AssertionError("Data insertion validation failed")
                
        except Exception as e:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = f"Data validation failed: {str(e)}"
            raise AssertionError(f"Data insertion validation failed: {str(e)}")
        
        # Step 4: Validate resource metadata
        try:
            # Check fixture resource data
            expected_resource_id = f"postgresql_test_{test_timestamp}_{test_uuid}"
            expected_db_name = f"test_fixture_db_{test_timestamp}_{test_uuid}"
            
            assert postgres_resource["resource_id"] == expected_resource_id, f"Resource ID mismatch. Expected: {expected_resource_id}, Got: {postgres_resource['resource_id']}"
            assert postgres_resource["type"] == "postgresql_resource", "Resource type mismatch"
            assert postgres_resource["status"] == "active", "Resource status not active"
            assert len(postgres_resource["created_resources"]) == 1, "Should have created 1 database"
            assert postgres_resource["created_resources"][0]["name"] == expected_db_name, f"Database name mismatch. Expected: {expected_db_name}, Got: {postgres_resource['created_resources'][0]['name']}"
            assert "users" in postgres_resource["created_resources"][0]["tables"], "Users table not tracked"
            
            test_steps[3]["status"] = "passed"
            test_steps[3]["Result_Message"] = "Resource metadata validation passed"
            
        except Exception as e:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = f"Resource metadata validation failed: {str(e)}"
            raise AssertionError(f"Resource metadata validation failed: {str(e)}")
        
        finally:
            # Close database connection
            if 'db_cursor' in locals():
                db_cursor.close()
            if 'db_connection' in locals():
                db_connection.close()
        
        # ALL VALIDATIONS PASSED
        print("PostgreSQL fixture validation completed successfully!")
        print(f"Created database: {postgres_resource['created_resources'][0]['name']}")
        print(f"Created tables: {postgres_resource['created_resources'][0]['tables']}")
        print("Fixture will automatically clean up resources after test completes.")
        
        assert True, "PostgreSQL fixture validation passed - resources created and validated successfully"

    except Exception as e:
        # Update any remaining test steps that didn't reach
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        raise
    
    finally:
        # CLEANUP - No model configs to clean up for fixture validation test
        if config_results:
            remove_model_configs(
                Configs=Test_Configs.Configs, 
                custom_info=config_results
            ) 