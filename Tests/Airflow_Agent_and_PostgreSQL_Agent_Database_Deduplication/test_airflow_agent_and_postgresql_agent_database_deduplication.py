import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2

from model.Configure_Model import remove_model_configs
from model.Configure_Model import set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.database
@pytest.mark.postgres
@pytest.mark.postgresql
@pytest.mark.three  # Difficulty 3 - involves database stored procedures, SQL window functions, and data deduplication
@pytest.mark.database_computation  # New marker for tests that push computation to database
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"db_dedup_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"dedup_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_and_postgresql_agent_database_deduplication(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "user_deduplication_dag"
    pr_title = "Add Database-Side User Deduplication Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource and postgres_resource fixtures
    print("=== Starting PostgreSQL Database-Side Deduplication Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
    print(f"Airflow base URL: {airflow_resource['base_url']}")
    print(f"Test directory: {input_dir}")

    test_steps = [
        {
            "name": "Checking Git Branch Existence",
            "description": "Checking if the git branch exists with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking PR Creation",
            "description": "Checking if the PR was created with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Verifying Database Stored Procedure Exists",
            "description": "Checking that the deduplicate_users() stored procedure is available",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Database-Side Deduplication Results",
            "description": "Verifying that deduplication was performed entirely in the database with correct results",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Performance Benefits",
            "description": "Confirming computation happened database-side rather than in Airflow containers",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]

    # SECTION 1: SETUP THE TEST
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = postgres_resource["created_resources"][0]["name"]

    config_results = None  # Initialize before try block
    try:
        # Get the actual database name from the fixture
        print(f"Using PostgreSQL database: {created_db_name}")

        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]

        # Configure Airflow connection details
        test_configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        test_configs["services"]["airflow"]["username"] = airflow_resource["username"]
        test_configs["services"]["airflow"]["password"] = airflow_resource["password"]
        test_configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        config_results = set_up_model_configs(
            Configs=test_configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        print("Running model to create database-centric DAG...")
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
        print(f"Model execution completed. Result: {model_result}")
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)  # Give the model time to create the branch and PR
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/database-user-deduplication", test_steps[0])
        if not branch_exists:
            raise Exception(test_steps[0]["Result_Message"])

        pr_exists, test_steps[1] = github_manager.find_and_merge_pr(
            pr_title=pr_title, 
            test_step=test_steps[1], 
            commit_title=pr_title, 
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource["deployment_id"],
                "deploymentName": airflow_resource["deployment_name"],
            }
        )
        if not pr_exists:
            raise Exception("Unable to find and merge PR. Please check the PR title and commit title.")

        # Use the airflow instance from the fixture to run the DAG
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Wait for DAG to appear and trigger it
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 3: VERIFY DATABASE-SIDE COMPUTATION RESULTS
        print("Verifying database-side deduplication results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Check if deduplicated_users table exists and has correct structure
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'deduplicated_users'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        assert len(columns) > 0, "deduplicated_users table was not created"
        
        expected_columns = ['id', 'email', 'first_name', 'last_name', 'organization', 'department', 'role', 'source']
        actual_columns = [col[0] for col in columns]
        for expected_col in expected_columns:
            assert expected_col in actual_columns, f"Missing expected column '{expected_col}' in deduplicated_users table"
        
        # Verify database and stored procedure exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_proc 
            WHERE proname = 'deduplicate_users'
        """)
        proc_count = cur.fetchone()[0]
        assert proc_count > 0, "deduplicate_users() stored procedure not found"
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "deduplicate_users() stored procedure exists"
        
        cur.close()
        conn.close()


        # Check unique email constraint
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.table_constraints 
            WHERE table_name = 'deduplicated_users' 
            AND constraint_type = 'UNIQUE'
            AND constraint_name LIKE '%email%'
        """)
        email_unique_count = cur.fetchone()[0]
        assert email_unique_count > 0, "Email uniqueness constraint not found"
        
        # Verify deduplication results
        cur.execute("SELECT COUNT(*) FROM deduplicated_users")
        deduplicated_count = cur.fetchone()[0]
        
        # Calculate expected unique emails from source data
        cur.execute("""
            SELECT COUNT(DISTINCT email) FROM (
                SELECT email FROM users_source_1
                UNION 
                SELECT email FROM users_source_2
                UNION
                SELECT email FROM users_source_3
            ) all_emails
        """)
        expected_unique_count = cur.fetchone()[0]
        
        assert deduplicated_count == expected_unique_count, f"Expected {expected_unique_count} unique users, found {deduplicated_count}"
        assert deduplicated_count > 0, "No deduplicated users found"
        
        # Verify specific deduplication cases - check that duplicates were properly merged
        cur.execute("""
            SELECT email, COUNT(*) 
            FROM deduplicated_users 
            GROUP BY email 
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()
        assert len(duplicates) == 0, f"Found duplicate emails in deduplicated table: {duplicates}"
        
        # Verify data quality - check that best information was preserved
        cur.execute("""
            SELECT email, first_name, last_name, organization, department, role 
            FROM deduplicated_users 
            WHERE email IN ('john.doe@example.com', 'jane.smith@example.com', 'bob.wilson@example.com')
            ORDER BY email
        """)
        sample_records = cur.fetchall()
        assert len(sample_records) >= 3, "Expected deduplication sample records not found"
        
        # Test database performance statistics
        cur.execute("SELECT * FROM get_deduplication_stats()")
        stats = dict(cur.fetchall())
        
        total_raw = stats['total_raw_records']
        unique_after = stats['unique_emails_after_dedup']
        duplicates_removed = stats['duplicates_removed']
        
        assert duplicates_removed > 0, "No duplicates were removed during deduplication"
        assert unique_after < total_raw, "Deduplication did not reduce record count"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = f"Database-side deduplication successful: {total_raw} raw records → {unique_after} unique records, {duplicates_removed} duplicates removed"
        
        # SECTION 4: VALIDATE PERFORMANCE BENEFITS
        print("Validating database-centric performance benefits...")
        
        # Check task logs to ensure database operations were used
        logs = airflow_instance.get_task_instance_logs(dag_id=dag_name, dag_run_id=dag_run_id, task_id="deduplicate_data")
        
        # Look for evidence of database-side processing
        database_keywords = ['PostgreSQLOperator', 'CALL deduplicate_users', 'SQL execution', 'database function']
        database_processing_found = any(keyword in logs for keyword in database_keywords)
        
        # Look for absence of data-pulling keywords (good - means we're not pulling data to Airflow)
        data_pulling_keywords = ['pandas', 'DataFrame', 'to_csv', 'read_sql']
        data_pulling_found = any(keyword in logs for keyword in data_pulling_keywords)
        
        # We want database processing but NOT data pulling
        assert database_processing_found or not data_pulling_found, "Evidence suggests data was processed in Airflow rather than database"
        
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = "Confirmed database-side computation - no data movement to Airflow containers detected"
        
        print("✓ Successfully validated database-side deduplication with optimal performance")
        print(f"✓ All {len(test_steps)} validation steps passed")
        print(f"✓ Performance benefits: {duplicates_removed} duplicates removed server-side without data movement")
        
        # Close database connection
        cur.close()
        conn.close()

    finally:
        try:
            # Clean up configurations
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/database-deduplication")

        except Exception as e:
            print(f"Error during cleanup: {e}")