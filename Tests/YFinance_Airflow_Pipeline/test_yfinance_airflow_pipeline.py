import os
import importlib
import pytest
import time
import requests
from github import Github
from requests.auth import HTTPBasicAuth
import psycopg2

from Environment.Airflow.Airflow import Airflow_Local
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
def test_yfinance_airflow_pipeline(request):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    airflow_local = Airflow_Local()

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

        # Set up the airflow folder with the correct configs

        # Setup Postgres database
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

        # Check and kill any existing connections
        postgres_cursor.execute(
            """
            SELECT pid, usename, datname 
            FROM pg_stat_activity 
            WHERE datname = 'stock_data'
            """
        )
        connections = postgres_cursor.fetchall()
        print(f"Found connections to stock_data db:", connections)

        postgres_cursor.execute(
            """
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = 'stock_data'
            """
        )
        print("Terminated all connections to stock_data db")

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

        config_results = set_up_model_configs(Configs=Test_Configs.Configs)


        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        input("Run the model")
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
            test_steps[1]["Result_Message"] = "PR was not created"
            raise Exception("PR was not created")

        # Merge the PR
        target_pr.merge(merge_method="merge")
        time.sleep(5)

        # Get the DAGs from GitHub
        airflow_local.Get_Airflow_Dags_From_Github()
        time.sleep(10)


        # Get Airflow base URL and credentials
        airflow_base_url = os.getenv("AIRFLOW_HOST")
        airflow_username = os.getenv("AIRFLOW_USERNAME")
        airflow_password = os.getenv("AIRFLOW_PASSWORD")

        # Wait for DAG to appear and trigger it
        max_retries = 5
        auth = HTTPBasicAuth(airflow_username, airflow_password)
        headers = {"Content-Type": "application/json", "Cache-Control": "no-cache"}

        for attempt in range(max_retries):
            # Check if DAG exists
            dag_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/tesla_stock_dag",
                auth=auth,
                headers=headers,
            )

            if dag_response.status_code != 200:
                if attempt == max_retries - 1:
                    raise Exception("DAG not found after max retries")
                time.sleep(10)
                continue

            # Unpause the DAG
            unpause_response = requests.patch(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/tesla_stock_dag",
                auth=auth,
                headers=headers,
                json={"is_paused": False},
            )

            if unpause_response.status_code != 200:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to unpause DAG: {unpause_response.text}")
                time.sleep(10)
                continue

            # Trigger the DAG
            trigger_response = requests.post(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/tesla_stock_dag/dagRuns",
                auth=auth,
                headers=headers,
                json={"conf": {}},
            )

            if trigger_response.status_code == 200:
                dag_run_id = trigger_response.json()["dag_run_id"]
                break
            else:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to trigger DAG: {trigger_response.text}")
                time.sleep(10)

        # Monitor the DAG run
        max_wait = 120
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/tesla_stock_dag/dagRuns/{dag_run_id}",
                auth=auth,
                headers=headers,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                if state == "success":
                    break
                elif state in ["failed", "error"]:
                    raise Exception(f"DAG failed with state: {state}")
            time.sleep(5)
        else:
            raise Exception("DAG run timed out")

        # SECTION 3: VERIFY THE OUTCOMES
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
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Successfully stored {row_count} days of Tesla stock data"

    finally:
        input("Waiting before cleanup")
        try:
            # Clean up Airflow DAG
            airflow_base_url = os.getenv("AIRFLOW_HOST")
            auth = HTTPBasicAuth(
                os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
            )
            headers = {"Content-Type": "application/json"}

            # Pause the DAG
            try:
                pause_response = requests.patch(
                    f"{airflow_base_url.rstrip('/')}/api/v1/dags/tesla_stock_dag",
                    auth=auth,
                    headers=headers,
                    json={"is_paused": True},
                )
            except:
                pass

            # Remove the configs
            remove_model_configs(
                Configs=Test_Configs.Configs, custom_info=config_results
            )

            # Clean up GitHub - delete branch
            try:
                ref = repo.get_git_ref(f"heads/feature/tesla_stock")
                ref.delete()
            except Exception as e:
                print(f"Branch might not exist or other error: {e}")

            # Reset repo state
            dags_contents = repo.get_contents("dags")
            for content in dags_contents:
                if content.name != ".gitkeep":
                    repo.delete_file(
                        path=content.path,
                        message="Clear dags folder",
                        sha=content.sha,
                        branch="main",
                    )

            # Ensure .gitkeep exists
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

            airflow_local.Cleanup_Airflow_Directories()

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

            # Database cleanup
            postgres_cursor.close()
            postgres_connection.close()
            print("Closed test connections")

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

            # Check and kill any remaining connections
            postgres_cursor.execute(
                """
                SELECT pid, usename, datname 
                FROM pg_stat_activity 
                WHERE datname = 'stock_data'
                """
            )
            connections = postgres_cursor.fetchall()
            print(f"Found connections to stock_data db during cleanup:", connections)

            postgres_cursor.execute(
                """
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = 'stock_data'
                """
            )
            print("Terminated all connections to stock_data db")

            # Now safe to drop
            postgres_cursor.execute("DROP DATABASE IF EXISTS stock_data")
            print("Dropped stock_data db in cleanup")

            # Close final connection
            postgres_cursor.close()
            postgres_connection.close()
            print("Database cleanup completed successfully")

        except Exception as e:
            print(f"Error during cleanup: {e}") 