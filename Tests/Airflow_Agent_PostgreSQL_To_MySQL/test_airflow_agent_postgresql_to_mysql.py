import importlib
import os
import pytest
import re
import time
import uuid
from datetime import datetime

import psycopg2
import requests
from github import Github
from requests.auth import HTTPBasicAuth

from Configs.MySQLConfig import connection as mysql_connection
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
@pytest.mark.mysql
@pytest.mark.pipeline
@pytest.mark.database
@pytest.mark.three  # Difficulty 3 - involves database operations, DAG creation, and data validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"postgresql_to_mysql_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"sales_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "transactions",
                    "columns": [
                        {"name": "transaction_id", "type": "SERIAL", "primary_key": True},
                        {"name": "user_id", "type": "INTEGER", "not_null": True},
                        {"name": "product_id", "type": "INTEGER", "not_null": True},
                        {"name": "sale_amount", "type": "DECIMAL(10,2)", "not_null": True},
                        {"name": "cost_amount", "type": "DECIMAL(10,2)", "not_null": True},
                        {"name": "transaction_date", "type": "DATE", "not_null": True}
                    ],
                    "data": [
                        {"user_id": 1, "product_id": 101, "sale_amount": 100.00, "cost_amount": 60.00, "transaction_date": "2024-01-01"},
                        {"user_id": 1, "product_id": 102, "sale_amount": 150.00, "cost_amount": 90.00, "transaction_date": "2024-01-01"},
                        {"user_id": 2, "product_id": 101, "sale_amount": 200.00, "cost_amount": 120.00, "transaction_date": "2024-01-01"}
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_airflow_agent_postgresql_to_mysql(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "sales_profit_pipeline"
    pr_title = "Merge_Sales_Profit_Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting PostgreSQL to MySQL Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
    print(f"Airflow base URL: {airflow_resource['base_url']}")
    print(f"Test directory: {input_dir}")

    test_steps = [
        {
            "name": "Database Setup",
            "description": "Setting up PostgreSQL and MySQL databases with test data",
            "status": "did not reach",
            "Result_Message": "",
        },
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
            "description": "Checking if the DAG produces the expected results",
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

        # Setup MySQL database
        print("Setting up MySQL database...")
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS analytics_db")
        mysql_cursor.execute("USE analytics_db")

        # Create result table
        mysql_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_profits (
                date DATE,
                user_id INTEGER,
                total_sales DECIMAL(10,2),
                total_costs DECIMAL(10,2),
                total_profit DECIMAL(10,2),
                PRIMARY KEY (date, user_id)
            )
        """
        )

        mysql_connection.commit()

        # Verify table creation
        mysql_cursor.execute("SHOW TABLES")
        tables = mysql_cursor.fetchall()
        print(f"MySQL tables created: {tables}")

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "PostgreSQL and MySQL databases set up successfully with test data"

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
        
        branch_exists, test_steps[1] = github_manager.verify_branch_exists("feature/sales_profit_pipeline", test_steps[1])
        if not branch_exists:
            raise Exception(test_steps[1]["Result_Message"])

        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title, 
            test_step=test_steps[2], 
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
        if not airflow_instance.wait_for_airflow_to_be_ready(6):
            raise Exception("Airflow instance did not redeploy successfully.")
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready(3):
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
        print("Verifying the outcomes...")
        # Verify data in MySQL
        mysql_cursor.execute(
            """
            SELECT * FROM daily_profits 
            WHERE date = '2024-01-01'
            ORDER BY user_id
        """
        )

        results = mysql_cursor.fetchall()
        assert len(results) == 2, "Expected 2 records in daily_profits"

        # Check user 1's profits
        # They had two transactions: $100 sale ($60 cost) and $150 sale ($90 cost)
        assert results[0][0] == datetime(2024, 1, 1).date(), "Incorrect date"
        assert results[0][1] == 1, "Incorrect user_id"
        assert float(results[0][2]) == 250.00, "Incorrect total sales"  # 100 + 150
        assert float(results[0][3]) == 150.00, "Incorrect total costs"  # 60 + 90
        assert float(results[0][4]) == 100.00, "Incorrect total profit"  # 250 - 150

        # Check user 2's profits
        # They had one transaction: $200 sale ($120 cost)
        assert results[1][0] == datetime(2024, 1, 1).date(), "Incorrect date"
        assert results[1][1] == 2, "Incorrect user_id"
        assert float(results[1][2]) == 200.00, "Incorrect total sales"
        assert results[1][3] == 120.00, "Incorrect total costs"
        assert float(results[1][4]) == 80.00, "Incorrect total profit"  # 200 - 120

        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "DAG successfully transferred and transformed data from Postgres to MySQL"

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
            
            # Clean up MySQL database
            print("Starting MySQL cleanup...")
            try:
                mysql_cursor.execute("DROP DATABASE IF EXISTS analytics_db")
                mysql_connection.commit()
                mysql_cursor.close()
                mysql_connection.close()
                print("MySQL cleanup completed")
            except Exception as e:
                print(f"Error during MySQL cleanup: {e}")

            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/sales_profit_pipeline")

        except Exception as e:
            print(f"Error during cleanup: {e}")
