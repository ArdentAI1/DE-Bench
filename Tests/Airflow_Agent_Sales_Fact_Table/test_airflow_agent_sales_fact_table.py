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


@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.database
@pytest.mark.three  # Difficulty 3 - involves DAG creation, fact table creation, and foreign key validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"sales_fact_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"sales_data_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_sales_fact_table(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "sales_fact_creation_dag"
    pr_title = "Add Sales Fact Table Creation Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting Sales Fact Table Creation Airflow Pipeline Test ===")
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
            "name": "Checking Sales Fact Table Creation",
            "description": "Checking if the sales_fact table was properly created with foreign keys",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    # SECTION 1: SETUP THE TEST
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = postgres_resource["created_resources"][0]["name"]

    config_results = None  # Initialize before try block
    try:
        # The dags folder is already set up by the fixture
        # The PostgreSQL database is already set up by the postgres_resource fixture

        # Get the actual database name from the fixture
        print(f"Using PostgreSQL database: {created_db_name}")

        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]

        # set the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
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
        print("Running model to create DAG and PR...")
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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/sales-fact-table", test_steps[0])
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

        # Use the airflow instance from the fixture to pull DAGs from GitHub
        # The fixture already has the Docker instance running
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Use the connection details from the fixture
        airflow_base_url = airflow_resource["base_url"]
        airflow_api_token = airflow_resource["api_token"]
        
        print(f"Connecting to Airflow at: {airflow_base_url}")
        print(f"Using API Token: {airflow_api_token}")

        # Wait for DAG to appear and trigger it
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 3: VERIFY THE OUTCOMES
        print("Verifying sales fact table creation...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Check if sales_fact table exists and has data
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_fact
        """)
        row_count = cur.fetchone()[0]
        
        assert row_count > 0, "No sales fact data found in the database"
        print(f"✓ Found {row_count} sales fact records")

        # Check table structure
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'sales_fact'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        expected_columns = ['sales_id', 'transaction_id', 'item_id', 'customer_id', 'quantity', 'unit_price', 'total_amount', 'sale_date']
        actual_columns = [col[0] for col in columns]
        
        # Check that all expected columns exist
        for expected_col in expected_columns:
            assert expected_col in actual_columns, f"Missing expected column '{expected_col}' in sales_fact table"
        
        # Verify foreign key constraints exist
        cur.execute("""
            SELECT 
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_name = 'sales_fact'
            ORDER BY kcu.column_name
        """)
        foreign_keys = cur.fetchall()
        
        # Should have 3 foreign keys: transaction_id, item_id, customer_id
        assert len(foreign_keys) >= 3, f"Expected at least 3 foreign key constraints, found {len(foreign_keys)}"
        
        # Verify specific foreign key relationships
        fk_columns = [fk[2] for fk in foreign_keys]
        expected_fk_columns = ['transaction_id', 'item_id', 'customer_id']
        for expected_fk in expected_fk_columns:
            assert expected_fk in fk_columns, f"Missing foreign key constraint for '{expected_fk}'"
        
        # Verify data integrity - check that foreign keys reference valid records
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_fact sf
            LEFT JOIN transactions t ON sf.transaction_id = t.transaction_id
            LEFT JOIN items i ON sf.item_id = i.item_id
            LEFT JOIN customers c ON sf.customer_id = c.customer_id
            WHERE t.transaction_id IS NULL OR i.item_id IS NULL OR c.customer_id IS NULL
        """)
        orphaned_records = cur.fetchone()[0]
        assert orphaned_records == 0, f"Found {orphaned_records} sales fact records with invalid foreign key references"
        
        # Verify some sample data relationships
        cur.execute("""
            SELECT 
                sf.sales_id,
                sf.quantity,
                sf.unit_price,
                sf.total_amount,
                t.transaction_date,
                i.item_name,
                c.first_name || ' ' || c.last_name as customer_name
            FROM sales_fact sf
            JOIN transactions t ON sf.transaction_id = t.transaction_id
            JOIN items i ON sf.item_id = i.item_id
            JOIN customers c ON sf.customer_id = c.customer_id
            LIMIT 5
        """)
        sample_records = cur.fetchall()
        
        assert len(sample_records) > 0, "No valid sales fact records with proper foreign key relationships found"
        
        # Verify that total_amount = quantity * unit_price (basic business logic)
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_fact 
            WHERE ABS(total_amount - (quantity * unit_price)) > 0.01
        """)
        invalid_totals = cur.fetchone()[0]
        assert invalid_totals == 0, f"Found {invalid_totals} records where total_amount doesn't equal quantity * unit_price"
        
        print(f"✓ Successfully verified sales_fact table with {row_count} records and proper foreign key relationships")
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Successfully created sales_fact table with {row_count} records and proper foreign key constraints"

    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/sales-fact-table")

        except Exception as e:
            print(f"Error during cleanup: {e}")
