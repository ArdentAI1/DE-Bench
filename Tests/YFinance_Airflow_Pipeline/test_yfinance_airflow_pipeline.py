import os
import importlib
import pytest
import time
from github import Github
import psycopg2

from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs
from model.Configure_Model import remove_model_configs

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.database
def test_yfinance_airflow_pipeline(request, airflow_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print(f"=== Starting YFinance Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
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
    config_results = None
    try:
        # Setup GitHub repository with empty dags folder
        access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        airflow_github_repo = os.getenv("AIRFLOW_REPO")

        # Convert full URL to owner/repo format if needed
        if "github.com" in airflow_github_repo:
            parts = airflow_github_repo.split("/")
            airflow_github_repo = f"{parts[-2]}/{parts[-1]}"

        g = Github(access_token)
        repo = g.get_repo(airflow_github_repo)

        try:
            # First, clear only the dags folder
            dags_contents = repo.get_contents("dags")
            for content in dags_contents:
                if content.name != ".gitkeep":
                    repo.delete_file(
                        path=content.path,
                        message="Clear dags folder",
                        sha=content.sha,
                        branch="main",
                    )

            # Ensure .gitkeep exists in dags folder
            try:
                repo.get_contents("dags/.gitkeep")
            except:
                repo.create_file(
                    path="dags/.gitkeep",
                    message="Add .gitkeep to dags folder",
                    content="",
                    branch="main",
                )
        except Exception as e:
            if "sha" not in str(e):
                raise e

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
        print(f"Found connections to stock_data db:", connections)

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

        # Set up the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        config_results = set_up_model_configs(Configs=Test_Configs.Configs)

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Check if the branch exists
        try:
            branch = repo.get_branch("feature/tesla_stock")
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Branch 'feature/tesla_stock' was created successfully"
        except Exception as e:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Branch 'feature/tesla_stock' was not created: {str(e)}"
            raise Exception(f"Branch 'feature/tesla_stock' was not created: {str(e)}")

        # Find and merge the PR
        pulls = repo.get_pulls(state="open")
        target_pr = None
        for pr in pulls:
            if pr.title == "Add Tesla Stock Data Pipeline":
                target_pr = pr
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = "PR 'Add Tesla Stock Data Pipeline' was created successfully"
                break

        if not target_pr:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "PR 'Add Tesla Stock Data Pipeline' not found"
            raise Exception("PR 'Add Tesla Stock Data Pipeline' not found")

        # Merge the PR
        merge_result = target_pr.merge(
            commit_title="Add Tesla Stock Data Pipeline", merge_method="squash"
        )

        if not merge_result.merged:
            raise Exception(f"Failed to merge PR: {merge_result.message}")

        # Use the airflow instance from the fixture to pull DAGs from GitHub
        # The fixture already has the Docker instance running
        airflow_instance = airflow_resource["airflow_instance"]
        if not airflow_instance.wait_for_airflow_to_be_ready(wait_time_in_minutes=6):
            raise Exception("Airflow instance did not redeploy successfully.")

        # Use the connection details from the fixture
        airflow_base_url = airflow_resource["base_url"]
        airflow_api_token = airflow_resource["api_token"]
        
        print(f"Connecting to Airflow at: {airflow_base_url}")
        print(f"Using API Token: {airflow_api_token}")

        # Wait for DAG to appear and trigger it
        max_retries = 5
        headers = airflow_resource["api_headers"]
        dag_id = "tesla_stock_dag"
        airflow_instance.verify_airflow_dag_exists(dag_id)
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_id)

        airflow_instance.verify_dag_id_ran(dag_id, dag_run_id)

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
                Configs=Test_Configs.Configs, custom_info=config_results
            )

            # Clean up GitHub - delete branch if it exists
            try:
                ref = repo.get_git_ref(f"heads/feature/tesla_stock")
                ref.delete()
            except Exception as e:
                print(f"Branch might not exist or other error: {e}")

            # Reset the repo to the original state
            dags_contents = repo.get_contents("dags")
            for content in dags_contents:
                if content.name != ".gitkeep":  # Keep the .gitkeep file if it exists
                    repo.delete_file(
                        path=content.path,
                        message="Clear dags folder",
                        sha=content.sha,
                        branch="main",
                    )

            # Ensure .gitkeep exists in dags folder
            try:
                repo.get_contents("dags/.gitkeep")
            except:
                repo.create_file(
                    path="dags/.gitkeep",
                    message="Add .gitkeep to dags folder",
                    content="",
                    branch="main",
                )

            # Clean up requirements.txt
            try:
                requirements_path = os.getenv("AIRFLOW_REQUIREMENTS_PATH", "Requirements/")
                requirements_file = repo.get_contents(f"{requirements_path}requirements.txt")
                repo.update_file(
                    path=requirements_file.path,
                    message="Reset requirements.txt to blank",
                    content="",
                    sha=requirements_file.sha,
                    branch="main",
                )
            except Exception as e:
                print(f"Error cleaning up requirements: {e}")

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
