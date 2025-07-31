import importlib
import os
import time

import pytest
import requests
from github import Github
from requests.auth import HTTPBasicAuth

from model.Configure_Model import remove_model_configs
from model.Configure_Model import set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.airflow
@pytest.mark.pipeline
def test_airflow_agent_simple_pipeline(request, airflow_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print(f"=== Starting Simple Airflow Pipeline Test ===")
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
            "name": "Checking Dag Results",
            "description": "Checking if the DAG produces the expected results",
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

        # set the airflow folder with the correct configs
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
            branch = repo.get_branch("feature/hello_world_dag")
            test_steps[0]["status"] = "passed"
            test_steps[0][
                "Result_Message"
            ] = "Branch 'feature/hello_world_dag' was created successfully"
        except Exception as e:
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = f"Branch 'feature/hello_world_dag' was not created: {str(e)}"
            raise Exception(
                f"Branch 'feature/hello_world_dag' was not created: {str(e)}"
            )

        # Find and merge the PR
        pulls = repo.get_pulls(state="open")
        target_pr = None
        for pr in pulls:
            if pr.title == "Add Hello World DAG":  # Look for PR by title
                target_pr = pr
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = "PR 'Add Hello World DAG' was created successfully"
                break

        if not target_pr:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "PR 'Add Hello World DAG' not found"
            raise Exception("PR 'Add Hello World DAG' not found")

        # Merge the PR
        merge_result = target_pr.merge(
            commit_title="Add Hello World DAG", merge_method="squash"
        )

        if not merge_result.merged:
            raise Exception(f"Failed to merge PR: {merge_result.message}")

        # Use the airflow instance from the fixture to pull DAGs from GitHub
        # The fixture already has the Docker instance running
        airflow_instance = airflow_resource["airflow_instance"]
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Use the connection details from the fixture
        airflow_base_url = airflow_resource["base_url"]
        airflow_api_token = airflow_resource["api_token"]
        
        print(f"Connecting to Airflow at: {airflow_base_url}")
        print(f"Using API Token: {airflow_api_token}")

        # Wait for DAG to appear and trigger it
        max_retries = 5
        headers = {"Content-Type": "application/json", "Cache-Control": "no-cache", "Authorization": f"Bearer {airflow_api_token}"}

        print(f"Waiting for DAG 'hello_world_dag' to appear in Airflow...")
        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1}/{max_retries}: Checking for DAG...")
            # Check if DAG exists
            dag_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag",
                headers=headers,
            )

            if dag_response.status_code != 200:
                print(f"DAG not found yet (status: {dag_response.status_code}), waiting...")
                if attempt == max_retries - 1:
                    raise Exception("DAG not found after max retries")
                time.sleep(10)
                continue

            print(f"DAG found! Unpausing DAG...")
            # Unpause the DAG before triggering
            unpause_response = requests.patch(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag",
                headers=headers,
                json={"is_paused": False},
            )

            if unpause_response.status_code != 200:
                print(f"Failed to unpause DAG: {unpause_response.text}")
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to unpause DAG: {unpause_response.text}")
                time.sleep(10)
                continue

            print(f"DAG unpaused successfully. Triggering DAG...")
            # Trigger the DAG
            trigger_response = requests.post(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns",
                headers=headers,
                json={"conf": {}},
            )

            if trigger_response.status_code == 200:
                dag_run_id = trigger_response.json()["dag_run_id"]
                print(f"DAG triggered successfully! Run ID: {dag_run_id}")
                break
            else:
                print(f"Failed to trigger DAG: {trigger_response.text}")
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to trigger DAG: {trigger_response.text}")
                time.sleep(10)

        # Monitor the DAG run
        max_wait = 120  # 2 minutes timeout
        start_time = time.time()
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns/{dag_run_id}",
                headers=headers,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                print(f"DAG run state: {state}")
                if state == "success":
                    print("DAG run completed successfully!")
                    break
                elif state in ["failed", "error"]:
                    raise Exception(f"DAG failed with state: {state}")

            time.sleep(5)
        else:
            raise Exception("DAG run timed out")

        # SECTION 3: VERIFY THE OUTCOMES
        # Get the task logs to verify "Hello World" was printed
        print("Retrieving task logs to verify 'Hello World' output...")

        # First, get task instance information
        task_instance_response = requests.get(
            f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns/{dag_run_id}/taskInstances/print_hello",
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

        print(f"Retrieving logs for task 'print_hello', try number: {try_number}")
        # Now get the logs with the correct try number
        task_logs_response = requests.get(
            f"{airflow_base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns/{dag_run_id}/taskInstances/print_hello/logs/{try_number}",
            headers=headers,
        )

        if task_logs_response.status_code != 200:
            raise Exception(f"Failed to retrieve task logs: {task_logs_response.text}")

        logs = task_logs_response.text
        print(f"Task logs retrieved. Log content length: {len(logs)} characters")
        print(f"Log content preview: {logs[:200]}...")
        
        assert "Hello World" in logs, "Expected 'Hello World' in task logs"
        print("✓ 'Hello World' found in task logs!")
        test_steps[2]["status"] = "passed"
        test_steps[2][
            "Result_Message"
        ] = "DAG produced the expected results of Hello World printed to the logs"


        input("Waiting here in test to inspect the airflow instance")



    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            remove_model_configs(
                Configs=Test_Configs.Configs, custom_info=config_results
            )

            # Clean up GitHub - delete branch if it exists
            try:
                ref = repo.get_git_ref(f"heads/feature/hello_world_dag")
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

        except Exception as e:
            print(f"Error during cleanup: {e}")
