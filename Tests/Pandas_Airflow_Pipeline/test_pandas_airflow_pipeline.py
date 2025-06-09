import os
import importlib
import pytest
import time
import requests
from github import Github
from requests.auth import HTTPBasicAuth

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
def test_pandas_airflow_pipeline(request):
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
            "name": "Checking Dag Results",
            "description": "Checking if the DAG produces the expected pandas results",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None  # Initialize before try block
    try:
        # Setup GitHub repository with empty dags folder
        access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        airflow_github_repo = os.getenv("AIRFLOW_REPO")

        # Convert full URL to owner/repo format if needed
        if "github.com" in airflow_github_repo:
            # Extract owner/repo from URL
            parts = airflow_github_repo.split("/")
            airflow_github_repo = f"{parts[-2]}/{parts[-1]}"

        g = Github(access_token)
        repo = g.get_repo(airflow_github_repo)

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
        except Exception as e:
            if "sha" not in str(e):  # If error is not about folder already existing
                raise e

        
        # Set up the airflow folder with the correct configs
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
            branch = repo.get_branch("feature/pandas_dataframe")
            test_steps[0]["status"] = "passed"
            test_steps[0][
                "Result_Message"
            ] = "Branch 'feature/pandas_dataframe' was created successfully"
        except Exception as e:
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = f"Branch 'feature/pandas_dataframe' was not created: {str(e)}"
            raise Exception(
                f"Branch 'feature/pandas_dataframe' was not created: {str(e)}"
            )

        # Find and merge the PR
        pulls = repo.get_pulls(state="open")
        target_pr = None
        for pr in pulls:
            if pr.title == "Add Pandas DataFrame Processing DAG":  # Look for PR by title
                target_pr = pr
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = "PR 'Add Pandas DataFrame Processing DAG' was created successfully"
                break

        if not target_pr:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "PR was not created"
            raise Exception("PR was not created")

        # Merge the PR
        target_pr.merge(merge_method="merge")

        # Wait for the merge to complete
        time.sleep(5)

        # Get the DAGs from GitHub
        input("Checking container before we fetch")
        airflow_local.Get_Airflow_Dags_From_Github()

        # Wait for Airflow to detect the new DAG
        time.sleep(10)

        # Get Airflow base URL
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
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag",
                auth=auth,
                headers=headers,
            )

            if dag_response.status_code != 200:
                if attempt == max_retries - 1:
                    raise Exception("DAG not found after max retries")
                time.sleep(10)
                continue

            # Unpause the DAG before triggering
            unpause_response = requests.patch(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag",
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
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag/dagRuns",
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
        max_wait = 120  # 2 minutes timeout
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag/dagRuns/{dag_run_id}",
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
        # Get the task logs to verify pandas operations were successful

        # First, get task instance information
        task_instance_response = requests.get(
            f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag/dagRuns/{dag_run_id}/taskInstances/process_dataframe",
            auth=auth,
            headers=headers,
        )

        if task_instance_response.status_code != 200:
            raise Exception(
                f"Failed to retrieve task instance details: {task_instance_response.text}"
            )

        # Extract the try_number from the response
        task_instance_data = task_instance_response.json()
        try_number = task_instance_data.get(
            "try_number", 1
        )  # Default to 1 if not found

        # Now get the logs with the correct try number
        task_logs_response = requests.get(
            f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag/dagRuns/{dag_run_id}/taskInstances/process_dataframe/logs/{try_number}",
            auth=auth,
            headers=headers,
        )

        if task_logs_response.status_code != 200:
            raise Exception(f"Failed to retrieve task logs: {task_logs_response.text}")

        logs = task_logs_response.text
        
        # Check for pandas-specific output in the logs
        assert "Alice" in logs, "Expected 'Alice' in DataFrame output"
        assert "Bob" in logs, "Expected 'Bob' in DataFrame output"
        assert "Charlie" in logs, "Expected 'Charlie' in DataFrame output"
        assert "David" in logs, "Expected 'David' in DataFrame output"
        assert "Eve" in logs, "Expected 'Eve' in DataFrame output"
        assert "Mean value: 30.0" in logs, "Expected exact mean calculation output 'Mean value: 30.0'"
        
        test_steps[2]["status"] = "passed"
        test_steps[2][
            "Result_Message"
        ] = "DAG successfully used pandas to process data with the 5 specified names and calculated mean value of 30.0"

    finally:
        input("Waiting before cleanup")
        try:
            # Clean up Airflow DAG
            airflow_base_url = os.getenv("AIRFLOW_HOST")
            auth = HTTPBasicAuth(
                os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD")
            )
            headers = {"Content-Type": "application/json"}

            # First pause the DAG
            try:
                pause_response = requests.patch(
                    f"{airflow_base_url.rstrip('/')}/api/v1/dags/pandas_dataframe_dag",
                    auth=auth,
                    headers=headers,
                    json={"is_paused": True},
                )
            except:
                pass  # Ignore errors during cleanup

            # Remove the configs for the test
            remove_model_configs(
                Configs=Test_Configs.Configs, custom_info=config_results
            )

            # Clean up GitHub - delete branch if it exists
            try:
                ref = repo.get_git_ref(f"heads/feature/pandas_dataframe")
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

            # Clean up requirements.txt - reset to blank
            try:
                requirements_path = os.getenv("AIRFLOW_REQUIREMENTS_PATH", "Requirements/")
                requirements_file = repo.get_contents(f"{requirements_path}requirements.txt")
                
                # Set to empty content
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

        except Exception as e:
            print(f"Error during cleanup: {e}") 