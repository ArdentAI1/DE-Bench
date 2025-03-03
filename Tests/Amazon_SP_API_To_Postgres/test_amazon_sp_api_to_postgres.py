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

        # Configure Ardent with Postgres connection
        postgres_result = Ardent_Client.set_config(
            config_type="postgres",
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            username=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="amazon_sales",
        )

        # Configure Ardent with Airflow connection
        airflow_result = Ardent_Client.set_config(
            config_type="airflow",
            host=os.getenv("AIRFLOW_HOST"),
            username=os.getenv("AIRFLOW_USERNAME"),
            password=os.getenv("AIRFLOW_PASSWORD"),
            github_token=os.getenv("AIRFLOW_GITHUB_TOKEN"),
            repo=os.getenv("AIRFLOW_REPO"),
            dag_path=os.getenv("AIRFLOW_DAG_PATH"),
        )

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Wait for DAG to be detected by Airflow
        time.sleep(30)

        # SECTION 3: VERIFY THE OUTCOMES
        # Verify DAG exists and runs
        airflow_base_url = os.getenv("AIRFLOW_HOST")
        auth = HTTPBasicAuth(
            os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
        )

        # Trigger DAG run
        response = requests.post(
            f"{airflow_base_url}/api/v1/dags/amazon_sp_api_to_postgres/dagRuns",
            auth=auth,
            json={"conf": {}},
        )
        assert response.status_code == 200, "Failed to trigger DAG run"

        dag_run_id = response.json()["dag_run_id"]

        # Wait for DAG completion (max 5 minutes)
        for _ in range(30):
            status_response = requests.get(
                f"{airflow_base_url}/api/v1/dags/amazon_sp_api_to_postgres/dagRuns/{dag_run_id}",
                auth=auth,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                if state == "success":
                    break
                elif state in ["failed", "error"]:
                    raise Exception(f"DAG failed with state: {state}")

            time.sleep(10)

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

    finally:
        try:
            print("Starting cleanup...")

            # Clean up Airflow DAG
            airflow_base_url = os.getenv("AIRFLOW_HOST")
            auth = HTTPBasicAuth(
                os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
            )

            # Delete all DAG runs
            requests.delete(
                f"{airflow_base_url}/api/v1/dags/amazon_sp_api_to_postgres/dagRuns",
                auth=auth,
            )

            # Delete the DAG itself
            requests.delete(
                f"{airflow_base_url}/api/v1/dags/amazon_sp_api_to_postgres", auth=auth
            )

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

            # Delete Ardent configs
            Ardent_Client.delete_config(
                config_id=postgres_result["specific_config"]["id"]
            )
            Ardent_Client.delete_config(
                config_id=airflow_result["specific_config"]["id"]
            )

            print("Cleanup completed successfully")

        except Exception as e:
            print(f"Error during cleanup: {e}")
