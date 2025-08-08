import importlib
import os
import pytest
import re
import time
import uuid
from datetime import datetime, timedelta
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
@pytest.mark.postgres
@pytest.mark.amazon_sp_api
@pytest.mark.pipeline
@pytest.mark.api_integration
@pytest.mark.three  # Difficulty 3 - involves API integration, DAG creation, and database validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"amazon_sp_api_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"amazon_sales_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "orders",
                    "columns": [
                        {"name": "order_id", "type": "VARCHAR(50)", "primary_key": True},
                        {"name": "order_date", "type": "DATE"},
                        {"name": "buyer_email", "type": "VARCHAR(255)"},
                        {"name": "order_status", "type": "VARCHAR(50)"},
                        {"name": "order_total", "type": "DECIMAL(10,2)"}
                    ],
                    "data": []  # Will be populated by the DAG
                },
                {
                    "name": "order_items",
                    "columns": [
                        {"name": "item_id", "type": "VARCHAR(50)", "primary_key": True},
                        {"name": "order_id", "type": "VARCHAR(50)", "not_null": True},
                        {"name": "product_id", "type": "VARCHAR(50)", "not_null": True},
                        {"name": "quantity", "type": "INTEGER"},
                        {"name": "item_price", "type": "DECIMAL(10,2)"}
                    ],
                    "data": []  # Will be populated by the DAG
                },
                {
                    "name": "products",
                    "columns": [
                        {"name": "product_id", "type": "VARCHAR(50)", "primary_key": True},
                        {"name": "product_name", "type": "VARCHAR(255)"},
                        {"name": "category", "type": "VARCHAR(100)"},
                        {"name": "price", "type": "DECIMAL(10,2)"}
                    ],
                    "data": []  # Will be populated by the DAG
                },
                {
                    "name": "inventory",
                    "columns": [
                        {"name": "inventory_id", "type": "VARCHAR(50)", "primary_key": True},
                        {"name": "product_id", "type": "VARCHAR(50)", "not_null": True},
                        {"name": "quantity_available", "type": "INTEGER"},
                        {"name": "warehouse_location", "type": "VARCHAR(100)"}
                    ],
                    "data": []  # Will be populated by the DAG
                }
            ]
        }
    ]
}], indirect=True)
def test_airflow_agent_amazon_sp_api_to_postgresql(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "amazon_sp_api_to_postgres"
    pr_title = "Add Amazon SP-API to Postgres Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting Amazon SP-API to PostgreSQL Airflow Pipeline Test ===")
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
            "name": "Checking DAG Results",
            "description": "Checking if the DAG correctly transforms and stores data",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None  # Initialize before try block
    try:
        # The dags folder is already set up by the fixture
        # The PostgreSQL database is already set up by the postgres_resource fixture

        # Get the actual database name from the fixture
        db_name = postgres_resource["created_resources"][0]["name"]
        print(f"Using PostgreSQL database: {db_name}")

        # Update the configs to use the fixture-created database
        Test_Configs.Configs["services"]["postgreSQL"]["databases"][0]["name"] = db_name

        # set the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        config_results = set_up_model_configs(
            Configs=Test_Configs.Configs,
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
            configs=Test_Configs.Configs,
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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/amazon_sp_api_pipeline", test_steps[0])
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
        print("Verifying database results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Check tables exist
        cur.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        )
        tables = {row[0] for row in cur.fetchall()}
        required_tables = {"orders", "order_items", "products", "inventory"}
        assert required_tables.issubset(
            tables
        ), f"Missing tables: {required_tables - tables}"

        # Check foreign key constraints
        cur.execute(
            """
            SELECT 
                tc.table_name, kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY'
        """
        )
        foreign_keys = cur.fetchall()

        # Verify essential foreign key relationships
        fk_relationships = {(fk[0], fk[2]) for fk in foreign_keys}
        required_relationships = {
            ("order_items", "orders"),
            ("order_items", "products"),
            ("inventory", "products"),
        }
        assert required_relationships.issubset(
            fk_relationships
        ), "Missing foreign key relationships"

        # Verify data was imported
        for table in required_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            assert count > 0, f"No data found in table {table}"

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "DAG successfully transformed and stored Amazon SP-API data in Postgres"

    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            remove_model_configs(
                Configs=Test_Configs.Configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/amazon_sp_api_pipeline")

        except Exception as e:
            print(f"Error during cleanup: {e}")
