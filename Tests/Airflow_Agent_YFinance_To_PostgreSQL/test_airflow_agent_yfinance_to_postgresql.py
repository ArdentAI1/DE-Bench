import importlib
import os
import pytest
import re
import time
import psycopg2

from model.Configure_Model import remove_model_configs
from model.Configure_Model import set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.database
@pytest.mark.two  # Difficulty 2 - involves DAG creation, PR management, and database validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
def test_airflow_agent_yfinance_to_postgresql(request, airflow_resource, github_resource, supabase_account_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "tesla_stock_dag"
    pr_title = "Add Tesla Stock Data Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print(f"=== Starting YFinance Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
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
            "name": "Checking Database Results",
            "description": "Checking if the Tesla stock data was properly stored in PostgreSQL",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None  # Initialize before try block
    try:
        # The dags folder is already set up by the fixture

        # Setup Postgres database
        print("Setting up PostgreSQL database...")
        postgres_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="postgres",
            sslmode="require",
        )
        postgres_connection.autocommit = True
        postgres_cursor = postgres_connection.cursor()

        # Check and kill any existing connections (if we have permission)
        postgres_cursor.execute(
            """
            SELECT pid, usename, datname 
            FROM pg_stat_activity 
            WHERE datname = 'stock_data'
            """
        )
        connections = postgres_cursor.fetchall()
        print(f"Found connections to stock_data db: {connections}")

        if connections:
            try:
                postgres_cursor.execute(
                    """
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = 'stock_data'
                    """
                )
                print("Terminated all connections to stock_data db")
            except Exception as e:
                print(f"Could not terminate connections (permission issue): {e}")
                print("Continuing with database operations...")

        # Now safe to drop and recreate
        postgres_cursor.execute("DROP DATABASE IF EXISTS stock_data")
        print("Dropped existing stock_data db if it existed")
        postgres_cursor.execute("CREATE DATABASE stock_data")
        print("Created new stock_data db")

        # Close connection to postgres database
        postgres_cursor.close()
        postgres_connection.close()

        # Reconnect to the new database
        postgres_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="stock_data",
            sslmode="require",
        )
        postgres_cursor = postgres_connection.cursor()

        # Create tesla_stock table
        postgres_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tesla_stock (
                date DATE PRIMARY KEY,
                open DECIMAL(10,2),
                high DECIMAL(10,2),
                low DECIMAL(10,2),
                close DECIMAL(10,2),
                volume BIGINT
            )
            """
        )
        postgres_connection.commit()
        print("Created tesla_stock table")

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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/tesla_stock", test_steps[0])
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
        print("Verifying database results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="stock_data",
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Check if table exists and has data
        cur.execute("""
            SELECT COUNT(*) 
            FROM tesla_stock 
            WHERE date >= CURRENT_DATE - INTERVAL '10 days'
        """)
        row_count = cur.fetchone()[0]
        
        assert row_count > 0, "No Tesla stock data found in the database"
        assert row_count <= 10, "Too many days of data found"

        # Check table structure
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tesla_stock'
        """)
        columns = cur.fetchall()
        expected_columns = {'date', 'open', 'high', 'low', 'close', 'volume'}
        actual_columns = {col[0] for col in columns}
        
        assert expected_columns.issubset(actual_columns), "Missing expected columns in tesla_stock table"
        
        print(f"✓ Successfully verified {row_count} days of Tesla stock data")
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Successfully stored {row_count} days of Tesla stock data"

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
            github_manager.delete_branch("feature/tesla_stock")

            # Clean up database
            postgres_connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOSTNAME"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USERNAME"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database="stock_data",
                sslmode="require",
            )
            postgres_cursor = postgres_connection.cursor()

            # Drop tesla_stock table
            postgres_cursor.execute("DROP TABLE IF EXISTS tesla_stock")
            postgres_connection.commit()

            # Close connection to postgres database
            postgres_cursor.close()
            postgres_connection.close()

            # Connect to postgres database for cleanup
            postgres_connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOSTNAME"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USERNAME"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database="postgres",
                sslmode="require",
            )
            postgres_connection.autocommit = True
            postgres_cursor = postgres_connection.cursor()

            # Check and kill any remaining connections (if we have permission)
            postgres_cursor.execute(
                """
                SELECT pid, usename, datname 
                FROM pg_stat_activity 
                WHERE datname = 'stock_data'
                """
            )
            connections = postgres_cursor.fetchall()
            print(f"Found connections to stock_data db during cleanup:", connections)

            if connections:
                try:
                    postgres_cursor.execute(
                        """
                        SELECT pg_terminate_backend(pid) 
                        FROM pg_stat_activity 
                        WHERE datname = 'stock_data'
                        """
                    )
                    print("Terminated all connections to stock_data db")
                except Exception as e:
                    print(f"Could not terminate connections during cleanup (permission issue): {e}")
                    print("Continuing with cleanup...")

            # Now safe to drop
            postgres_cursor.execute("DROP DATABASE IF EXISTS stock_data")
            print("Dropped stock_data db in cleanup")

            # Close final connection
            postgres_cursor.close()
            postgres_connection.close()
            print("Database cleanup completed successfully")

        except Exception as e:
            print(f"Error during cleanup: {e}")
