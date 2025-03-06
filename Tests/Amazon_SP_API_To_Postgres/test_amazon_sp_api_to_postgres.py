import os
import importlib
import pytest
import time
from datetime import datetime, timedelta
import psycopg2
import requests
from github import Github
from requests.auth import HTTPBasicAuth

from Configs.ArdentConfig import Ardent_Client
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, remove_model_configs
from Environment.Airflow.Airflow import Airflow_Local

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.amazon_sp_api
@pytest.mark.pipeline
@pytest.mark.api_integration
def test_amazon_sp_api_to_postgres(request):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))

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
    config_results = None
    airflow_local = Airflow_Local()

    # SECTION 1: SETUP THE TEST
    try:
        # Setup GitHub repository with empty dags folder
        access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        airflow_github_repo = os.getenv("AIRFLOW_REPO")

        # Convert full URL to owner/repo format if needed
        if "github.com" in airflow_github_repo:
            parts = airflow_github_repo.split("/")
            airflow_github_repo = f"{parts[-2]}/{parts[-1]}"

        print("Using repo format:", airflow_github_repo)
        g = Github(access_token)
        repo = g.get_repo(airflow_github_repo)

        # Clean up dags folder
        try:
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
        except Exception as e:
            if "sha" not in str(e):
                raise e

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

        # Drop and recreate amazon_sales database
        postgres_cursor.execute(
            """
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = 'amazon_sales'
        """
        )
        postgres_cursor.execute("DROP DATABASE IF EXISTS amazon_sales")
        postgres_cursor.execute("CREATE DATABASE amazon_sales")

        # Close connection and reconnect to new database
        postgres_cursor.close()
        postgres_connection.close()

        postgres_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="amazon_sales",
            sslmode="require",
        )
        postgres_cursor = postgres_connection.cursor()

        # Configure model with necessary configs
        config_results = set_up_model_configs(Configs=Test_Configs.Configs)

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        input("Model has run we are now checking the results")
        # Check if the branch exists
        try:
            branch = repo.get_branch("feature/amazon_sp_api_pipeline")
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Branch 'feature/amazon_sp_api_pipeline' was created successfully"
        except Exception as e:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Branch 'feature/amazon_sp_api_pipeline' was not created: {str(e)}"
            raise Exception(f"Branch 'feature/amazon_sp_api_pipeline' was not created: {str(e)}")

        input("Branch exists we are now checking the PR")
        # Find and merge the PR
        pulls = repo.get_pulls(state="open")
        target_pr = None
        for pr in pulls:
            if pr.title == "Add Amazon SP-API to Postgres Pipeline":
                target_pr = pr
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = "PR 'Add Amazon SP-API to Postgres Pipeline' was created successfully"
                break

        if not target_pr:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "PR 'Add Amazon SP-API to Postgres Pipeline' not found"
            raise Exception("PR 'Add Amazon SP-API to Postgres Pipeline' not found")

        input("PR found we are now merging the PR")
        # Merge the PR
        merge_result = target_pr.merge(
            commit_title="Add Amazon SP-API to Postgres Pipeline", merge_method="squash"
        )

        if not merge_result.merged:
            raise Exception(f"Failed to merge PR: {merge_result.message}")

        # Get the DAGs from GitHub
        airflow_local.Get_Airflow_Dags_From_Github()

        # Wait for Airflow to detect changes
        time.sleep(30)

        # SECTION 3: VERIFY THE OUTCOMES
        # Verify DAG exists and runs
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
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/amazon_sp_api_to_postgres",
                auth=auth,
                headers=headers,
            )

            if dag_response.status_code != 200:
                if attempt == max_retries - 1:
                    # Check for import errors before giving up
                    print("DAG not found after max retries, checking for import errors...")
                    import_errors_response = requests.get(
                        f"{airflow_base_url.rstrip('/')}/api/v1/importErrors",
                        auth=auth,
                        headers=headers
                    )
                    
                    if import_errors_response.status_code == 200:
                        import_errors = import_errors_response.json()['import_errors']
                        dag_errors = [error for error in import_errors 
                                     if "amazon_sp_api_to_postgres.py" in error['filename']]
                        
                        if dag_errors:
                            error_message = f"DAG failed to load with import error: {dag_errors[0]['stack_trace']}"
                            print(error_message)
                            test_steps[2]["status"] = "failed"
                            test_steps[2]["Result_Message"] = error_message
                            raise Exception("DAG error which caused DAG to not load")
                    
                    raise Exception("DAG not found after max retries")
                time.sleep(10)
                continue

            # Unpause the DAG before triggering
            unpause_response = requests.patch(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/amazon_sp_api_to_postgres",
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
            response = requests.post(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/amazon_sp_api_to_postgres/dagRuns",
                auth=auth,
                headers=headers,
                json={"conf": {}},
            )

            if response.status_code == 200:
                dag_run_id = response.json()["dag_run_id"]
                break
            else:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to trigger DAG: {response.text}")
                time.sleep(10)

        # Monitor the DAG run
        max_wait = 300  # 5 minutes timeout
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/amazon_sp_api_to_postgres/dagRuns/{dag_run_id}",
                auth=auth,
                headers=headers,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                if state == "success":
                    break
                elif state in ["failed", "error"]:
                    raise Exception(f"DAG failed with state: {state}")

            time.sleep(10)
        else:
            raise Exception("DAG run timed out")

        # SECTION 3: VERIFY THE OUTCOMES
        # Verify database structure and data
        # Check tables exist
        postgres_cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        )
        tables = {row[0] for row in postgres_cursor.fetchall()}
        required_tables = {"orders", "order_items", "products", "inventory"}
        assert required_tables.issubset(
            tables
        ), f"Missing tables: {required_tables - tables}"

        # Check foreign key constraints
        postgres_cursor.execute(
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
        foreign_keys = postgres_cursor.fetchall()

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
            postgres_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = postgres_cursor.fetchone()[0]
            assert count > 0, f"No data found in table {table}"

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "DAG successfully transformed and stored Amazon SP-API data in Postgres"

    finally:
        try:
            print("Starting cleanup...")

            # Clean up Airflow DAG
            airflow_base_url = os.getenv("AIRFLOW_HOST")
            auth = HTTPBasicAuth(
                os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
            )
            headers = {"Content-Type": "application/json"}

            # First pause the DAG
            try:
                requests.patch(
                    f"{airflow_base_url.rstrip('/')}/api/v1/dags/amazon_sp_api_to_postgres",
                    auth=auth,
                    headers=headers,
                    json={"is_paused": True},
                )
                print("Paused the DAG")
            except Exception as e:
                print(f"Error pausing DAG: {e}")

            # Clean up Postgres database
            postgres_cursor.close()
            postgres_connection.close()

            # Reconnect to postgres database for cleanup
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

            # Drop the database
            postgres_cursor.execute(
                """
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = 'amazon_sales'
            """
            )
            postgres_cursor.execute("DROP DATABASE IF EXISTS amazon_sales")
            postgres_cursor.close()
            postgres_connection.close()

            # Remove model configs
            remove_model_configs(
                Configs=Test_Configs.Configs, custom_info=config_results
            )

            # Clean up GitHub - delete branch if it exists
            try:
                ref = repo.get_git_ref("heads/feature/amazon_sp_api_pipeline")
                ref.delete()
                print("Deleted feature branch")
            except Exception as e:
                print(f"Branch might not exist or other error: {e}")

            # Clean up GitHub - reset dags folder
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
            print("Cleaned dags folder")
            
            # Clean up Airflow
            airflow_local.Cleanup_Airflow_Directories()

            print("Cleanup completed successfully")

        except Exception as e:
            print(f"Error during cleanup: {e}")
