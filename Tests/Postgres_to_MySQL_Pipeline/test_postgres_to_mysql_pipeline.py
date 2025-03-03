import os
import importlib
import pytest
import time
from datetime import datetime, timedelta
import mysql.connector
from python_on_whales import DockerClient
import base64
import requests
from github import Github
import psycopg2
import httpx
from requests.auth import HTTPBasicAuth


from Configs.ArdentConfig import Ardent_Client
from model.Run_Model import run_model
from Configs.MySQLConfig import connection as mysql_connection

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.mysql
@pytest.mark.pipeline
@pytest.mark.database
def test_postgres_to_mysql_pipeline(request):
    input_dir = os.path.dirname(os.path.abspath(__file__))

    # Create a Docker client with the compose file configuration

    request.node.user_properties.append(("user_query", Test_Configs.User_Input))

    config_results = None

    # SECTION 1: SETUP THE TEST
    try:

        # Setup GitHub repository with empty dags folder
        access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        airflow_github_repo = os.getenv("AIRFLOW_REPO")

        # Convert full URL to owner/repo format if needed
        if "github.com" in airflow_github_repo:
            # Extract owner/repo from URL
            parts = airflow_github_repo.split("/")
            airflow_github_repo = f"{parts[-2]}/{parts[-1]}"

        print("Using repo format:", airflow_github_repo)
        g = Github(access_token)
        repo = g.get_repo(airflow_github_repo)
        main_branch = repo.get_branch("main")

        print("GitHub repository:", airflow_github_repo)

        try:
            # First, clear only the dags folder
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
            print("Cleaned dags folder. Press Enter to continue...")
            input()

        except Exception as e:
            if "sha" not in str(e):  # If error is not about folder already existing
                raise e

        # Setup Postgres database and sample data
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="postgres",
            sslmode="require",
        )
        postgres_cursor = connection.cursor()

        # First connect to postgres database
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
            WHERE datname = 'sales_db'
        """
        )
        connections = postgres_cursor.fetchall()
        print(f"Found connections to sales_db:", connections)

        postgres_cursor.execute(
            """
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = 'sales_db'
        """
        )
        print("Terminated all connections to sales_db")

        # Now safe to drop and recreate
        postgres_cursor.execute("DROP DATABASE IF EXISTS sales_db")
        print("Dropped existing sales_db if it existed")
        postgres_cursor.execute("CREATE DATABASE sales_db")
        print("Created new sales_db")

        # Close connection to postgres database
        postgres_cursor.close()
        postgres_connection.close()

        # Reconnect to the new database
        postgres_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="sales_db",
            sslmode="require",
        )
        postgres_cursor = postgres_connection.cursor()

        # Create test table
        postgres_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                sale_amount DECIMAL(10,2) NOT NULL,
                cost_amount DECIMAL(10,2) NOT NULL,
                transaction_date DATE NOT NULL
            )
        """
        )

        # Insert sample data
        postgres_cursor.execute(
            """
            INSERT INTO transactions 
            (user_id, product_id, sale_amount, cost_amount, transaction_date)
            VALUES 
            (1, 101, 100.00, 60.00, '2024-01-01'),
            (1, 102, 150.00, 90.00, '2024-01-01'),
            (2, 101, 200.00, 120.00, '2024-01-01')
        """
        )

        # Make sure to commit the transaction
        postgres_connection.commit()

        # Verify the insert
        postgres_cursor.execute("SELECT * FROM transactions")
        data = postgres_cursor.fetchall()

        # Setup MySQL database
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

        # Verify table structure
        mysql_cursor.execute("DESCRIBE daily_profits")
        structure = mysql_cursor.fetchall()

        # Configure Ardent with database connections
        postgres_result = Ardent_Client.set_config(
            config_type="postgreSQL",
            Hostname=os.getenv("POSTGRES_HOSTNAME"),
            Port=os.getenv("POSTGRES_PORT"),
            username=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            databases=[{"name": "sales_db"}],
        )

        mysql_result = Ardent_Client.set_config(
            config_type="mysql",
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            username=os.getenv("MYSQL_USERNAME"),
            password=os.getenv("MYSQL_PASSWORD"),
            databases=[{"name": "analytics_db"}],
        )

        airflow_result = Ardent_Client.set_config(
            config_type="airflow",
            github_token=os.getenv("AIRFLOW_GITHUB_TOKEN"),
            repo=os.getenv("AIRFLOW_REPO"),
            dag_path=os.getenv("AIRFLOW_DAG_PATH"),
            host=os.getenv("AIRFLOW_HOST"),
            username=os.getenv("AIRFLOW_USERNAME"),
            password=os.getenv("AIRFLOW_PASSWORD"),
        )

        input("waiting for input")

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        time.sleep(10)

        # we first need to merge the PR

        input("We are now merging the pr")
        # After model creates the PR, find and merge it
        pulls = repo.get_pulls(state="open")
        target_pr = None
        for pr in pulls:
            if (
                pr.title == "Merge_Sales_Profit_Pipeline"
            ):  # Look for PR by title instead of branch
                target_pr = pr
                break

        if not target_pr:
            raise Exception("PR 'Merge_Sales_Profit_Pipeline' not found")

        # Merge the PR
        merge_result = target_pr.merge(
            commit_title="Merge_Sales_Profit_Pipeline", merge_method="squash"
        )

        if not merge_result.merged:
            raise Exception(f"Failed to merge PR: {merge_result.message}")

        # After creating the DAG, wait a bit for Airflow to detect it
        time.sleep(100)  # Give Airflow time to scan for new DAGs

        # Trigger the DAG
        airflow_base_url = os.getenv("AIRFLOW_HOST")
        airflow_username = os.getenv("AIRFLOW_USERNAME")
        airflow_password = os.getenv("AIRFLOW_PASSWORD")

        # Wait for DAG to appear and trigger it
        max_retries = 5
        auth = HTTPBasicAuth(airflow_username, airflow_password)
        headers = {"Content-Type": "application/json", "Cache-Control": "no-cache"}

        for attempt in range(max_retries):
            # Check if DAG exists
            input("We are now checking if the DAG exists")
            dag_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline",
                auth=auth,
                headers=headers,
            )

            if dag_response.status_code != 200:
                if attempt == max_retries - 1:
                    raise Exception("DAG not found after max retries")
                print(f"DAG not found, attempt {attempt + 1} of {max_retries}")
                time.sleep(10)
                continue

            input("We are now unpausing the DAG")
            # Unpause the DAG before triggering
            unpause_response = requests.patch(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline",
                auth=auth,
                headers=headers,
                json={"is_paused": False},
            )

            if unpause_response.status_code != 200:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to unpause DAG: {unpause_response.text}")
                print(f"Failed to unpause DAG, attempt {attempt + 1} of {max_retries}")
                time.sleep(10)
                continue

            input("We are now triggering the DAG")
            # Trigger the DAG
            trigger_response = requests.post(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline/dagRuns",
                auth=auth,
                headers=headers,
                json={"conf": {}},
            )

            if trigger_response.status_code == 200:
                dag_run_id = trigger_response.json()["dag_run_id"]
                print("DAG triggered successfully")
                break
            else:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to trigger DAG: {trigger_response.text}")
                print(f"Failed to trigger DAG, attempt {attempt + 1} of {max_retries}")
                time.sleep(10)

        # Monitor the DAG run
        input("We are now monitoring the DAG run")
        max_wait = 300  # 5 minutes timeout
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline/dagRuns/{dag_run_id}",
                auth=auth,
                headers=headers,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                print(f"DAG state: {state}")
                if state == "success":
                    print("DAG completed successfully")
                    break
                elif state in ["failed", "error"]:
                    raise Exception(f"DAG failed with state: {state}")

            time.sleep(10)
        else:
            raise Exception("DAG run timed out")

        input("waiting for input 3")

        # SECTION 3: VERIFY THE OUTCOMES
        # Check if DAG file was created
        # dagbag = DagBag(os.getenv("AIRFLOW_DAGS_FOLDER"))
        # assert "sales_profit_pipeline" in dagbag.dags, "DAG not found in Airflow"

        input("We are now verifying the outcomes")
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

    finally:
        try:
            print("Starting cleanup...")

            # Clean up Airflow DAG
            airflow_base_url = os.getenv("AIRFLOW_HOST")
            auth = HTTPBasicAuth(
                os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
            )
            headers = {"Content-Type": "application/json"}

            # First delete all DAG runs
            requests.delete(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline/dagRuns",
                auth=auth,
                headers=headers,
            )

            # Then delete the DAG itself
            requests.delete(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/sales_profit_pipeline",
                auth=auth,
                headers=headers,
            )

            print("Cleaned up Airflow DAG")

            # Rest of your existing cleanup...
            # First close our connection to sales_db
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
                WHERE datname = 'sales_db'
            """
            )
            connections = postgres_cursor.fetchall()
            print(f"Found connections to sales_db during cleanup:", connections)

            postgres_cursor.execute(
                """
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = 'sales_db'
            """
            )
            print("Terminated all connections to sales_db")

            # Now safe to drop
            postgres_cursor.execute("DROP DATABASE IF EXISTS sales_db")
            print("Dropped sales_db in cleanup")

            # Close final connection
            postgres_cursor.close()
            postgres_connection.close()
            print("Cleanup completed successfully")

            # MySQL cleanup
            mysql_cursor.execute("DROP DATABASE IF EXISTS analytics_db")
            mysql_connection.commit()
            mysql_cursor.close()
            mysql_connection.close()
            print("MySQL cleanup completed")

            # Delete Ardent configs
            Ardent_Client.delete_config(
                config_id=postgres_result["specific_config"]["id"]
            )
            Ardent_Client.delete_config(config_id=mysql_result["specific_config"]["id"])
            Ardent_Client.delete_config(
                config_id=airflow_result["specific_config"]["id"]
            )

            # Clean up GitHub - delete branch if it exists
            try:
                ref = repo.get_git_ref(f"heads/feature/sales_profit_pipeline")
                ref.delete()
                print("Deleted feature branch")
            except Exception as e:
                print(f"Branch might not exist or other error: {e}")

            # reset the repo to the original state
            # First, clear only the dags folder
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
            print("Cleaned dags folder. Press Enter to continue...")
            input()

        except Exception as e:
            print(f"Error during cleanup: {e}")
