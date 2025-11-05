# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a pandas DataFrame processing DAG.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"pandas_pipeline_test_{test_timestamp}_{test_uuid}",
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_pandas_pipeline_test_{test_timestamp}_{test_uuid}",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [airflow_fixture, github_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup and dynamically
    updates the task description with GitHub branch and PR information.
    """
    import os
    from extract_test_configs import create_config_from_fixtures

    # Get GitHub fixture to access manager for dynamic branch/PR creation
    github_fixture = next(
        (f for f in fixtures if f.get_resource_type() == "github_resource"), None
    )

    if not github_fixture:
        raise Exception(
            "GitHub fixture not found - required for branch and PR management"
        )

    # Get the GitHub manager from the fixture
    github_resource_data = getattr(github_fixture, "_resource_data", None)
    if not github_resource_data:
        raise Exception("GitHub resource data not available")

    github_manager = github_resource_data.get("github_manager")
    if not github_manager:
        raise Exception("GitHub manager not available")

    # Generate dynamic branch and PR names
    pr_title = f"Add Pandas DataFrame Processing DAG {test_timestamp}_{test_uuid}"
    branch_name = github_resource_data.get("resource_id")

    # Start with the original user input from Test_Configs
    task_description = Test_Configs.User_Input

    # Add merge step to user input
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders with dynamic values
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    print(f"🔧 Generated dynamic branch name: {branch_name}", flush=True)
    print(f"🔧 Generated dynamic PR title: {pr_title}", flush=True)

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created a pandas DataFrame processing DAG.

    Expected behavior:
    - DAG should be created with name "pandas_dataframe_dag"
    - DAG should have a task named "process_dataframe"
    - DAG should run successfully and process DataFrame with pandas
    - Task logs should contain specific DataFrame data and mean calculation

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create comprehensive test steps for validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to create Pandas DataFrame DAG",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Airflow DAG creation task...",
        },
        {
            "name": "Git Branch Creation",
            "description": "Verify that git branch was created with the correct name",
            "status": "running",
            "Result_Message": "Checking if git branch exists...",
        },
        {
            "name": "PR Creation and Merge",
            "description": "Verify that PR was created and merged successfully",
            "status": "running",
            "Result_Message": "Checking if PR was created and merged...",
        },
        {
            "name": "GitHub Action Completion",
            "description": "Verify that GitHub action completed successfully",
            "status": "running",
            "Result_Message": "Waiting for GitHub action to complete...",
        },
        {
            "name": "Airflow Redeployment",
            "description": "Verify that Airflow redeployed after GitHub action",
            "status": "running",
            "Result_Message": "Checking if Airflow redeployed successfully...",
        },
        {
            "name": "DAG Creation Validation",
            "description": "Verify that pandas_dataframe_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Pandas DataFrame DAG exists in Airflow...",
        },
        {
            "name": "DAG Task Validation",
            "description": "Verify that DAG has the process_dataframe task",
            "status": "running",
            "Result_Message": "Checking if DAG has the required process_dataframe task...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "DataFrame Creation Validation",
            "description": "Verify that DataFrame was created with correct data",
            "status": "running",
            "Result_Message": "Checking task logs for DataFrame creation...",
        },
        {
            "name": "Mean Calculation Validation",
            "description": "Verify that mean calculation is correct (30.0)",
            "status": "running",
            "Result_Message": "Validating mean calculation in task logs...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = (
                "❌ AI Agent task execution failed or returned no result"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = (
            "✅ AI Agent completed task execution successfully"
        )

        # Get fixtures for Airflow and GitHub
        airflow_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )
            if fixtures
            else None
        )
        github_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "github_resource"),
                None,
            )
            if fixtures
            else None
        )

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")
        if not github_fixture:
            raise Exception("GitHub fixture not found")

        # Get resource data
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not airflow_resource_data:
            raise Exception("Airflow resource data not available")

        github_resource_data = getattr(github_fixture, "_resource_data", None)
        if not github_resource_data:
            raise Exception("GitHub resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        base_url = airflow_resource_data["base_url"]
        github_manager = github_resource_data.get("github_manager")

        if not github_manager:
            raise Exception("GitHub manager not available")

        # Generate the same branch and PR names used in create_model_inputs
        pr_title = f"Add Pandas DataFrame Processing DAG {test_timestamp}_{test_uuid}"
        branch_name = github_resource_data.get("resource_id")

        # Step 2-6: GitHub and Airflow workflow
        print(f"🔍 Checking for branch: {branch_name}", flush=True)
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(
            branch_name, test_steps[1]
        )
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = (
            f"✅ Git branch '{branch_name}' created successfully"
        )

        # Capture agent's code snapshot for observability (after branch verification)
        print(
            f"📸 Capturing agent code snapshot from branch: {branch_name}", flush=True
        )
        print(
            f"🔍 DEBUG: About to call get_multiple_file_contents_from_branch",
            flush=True,
        )
        try:
            agent_code_snapshot = github_manager.get_multiple_file_contents_from_branch(
                branch_name=branch_name,
                paths_to_capture=[
                    "dags/",  # All DAG files created by the agent
                    "requirements.txt",  # Root requirements file
                    "Requirements/requirements.txt",  # Alternative requirements location
                ],
            )
            print(
                f"🔍 DEBUG: Successfully received agent_code_snapshot with type: {type(agent_code_snapshot)}, flush=True"
            )
            print(
                f"✅ Agent code snapshot captured: {agent_code_snapshot['summary']['total_files']} files "
                f"({agent_code_snapshot['summary']['total_size_bytes']} bytes, flush=True)"
            )

            # Store snapshot in base test metadata immediately (incremental capture)
            test_steps.append(
                {
                    "name": "Agent Code Snapshot Capture",
                    "description": "Capture exact code created by agent for debugging",
                    "status": "passed",
                    "Result_Message": f"✅ Captured {agent_code_snapshot['summary']['total_files']} files "
                    f"({agent_code_snapshot['summary']['total_size_bytes']} bytes) from branch {branch_name}",
                    "agent_code_snapshot": agent_code_snapshot,
                    "capture_timestamp": agent_code_snapshot["capture_timestamp"],
                    "branch_captured": branch_name,
                }
            )
            print(
                f"📋 Agent code snapshot added to test metadata for immediate availability",
                flush=True,
            )

        except Exception as e:
            print(f"⚠️ Failed to capture agent code snapshot: {e}", flush=True)
            agent_code_snapshot = None
            # Still add a test step to show the attempt
            test_steps.append(
                {
                    "name": "Agent Code Snapshot Capture",
                    "description": "Capture exact code created by agent for debugging",
                    "status": "failed",
                    "Result_Message": f"❌ Failed to capture code snapshot: {str(e)}",
                    "agent_code_snapshot": None,
                    "capture_error": str(e),
                }
            )

        # PR creation and merge
        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title,
            test_step=test_steps[2],
            commit_title=pr_title,
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
                "secretSuffix": airflow_resource_data["secret_suffix"],
            },
        )

        if not pr_exists:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "❌ Unable to find and merge PR"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = (
            f"✅ PR '{pr_title}' created and merged successfully"
        )

        # GitHub action completion with CI failure details
        action_status = github_manager.check_if_action_is_complete(
            pr_title=pr_title, return_details=True
        )

        if not action_status["completed"]:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = (
                f"❌ GitHub action timed out (status: {action_status['status']})"
            )
            test_steps[3]["action_status"] = action_status
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        elif not action_status["success"]:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = (
                f"❌ GitHub action failed (conclusion: {action_status['conclusion']})"
            )
            test_steps[3]["action_status"] = action_status
            # CI details are automatically included in action_status["ci_details"]
            if "ci_details" in action_status:
                print(
                    f"📋 CI details captured: {len(action_status['ci_details'].get('jobs', []), flush=True)} jobs analyzed"
                )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        else:
            test_steps[3]["status"] = "passed"
            test_steps[3]["Result_Message"] = "✅ GitHub action completed successfully"
            test_steps[3]["action_status"] = action_status
            # TESTING: Show CI details even for successful runs
            if "ci_details" in action_status:
                print(
                    f"📋 CI details captured for successful run: {len(action_status['ci_details'].get('jobs', []), flush=True)} jobs analyzed"
                )

        # Airflow redeployment
        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = (
                "❌ Airflow instance did not redeploy successfully"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = (
            "✅ Airflow redeployed successfully after GitHub action"
        )

        # DAG existence check
        dag_name = "pandas_dataframe_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}", flush=True)

        if airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' found in Airflow"
        else:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = (
                f"❌ DAG '{dag_name}' not found in Airflow"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # DAG task validation - check if it has the process_dataframe task
        try:
            dag_tasks = airflow_instance.get_dag_tasks(dag_name)
            task_ids = [task.get("task_id", "") for task in dag_tasks]

            if "process_dataframe" in task_ids:
                test_steps[6]["status"] = "passed"
                test_steps[6]["Result_Message"] = (
                    f"✅ Found 'process_dataframe' task in DAG (tasks: {', '.join(task_ids)})"
                )
            else:
                test_steps[6]["status"] = "failed"
                test_steps[6]["Result_Message"] = (
                    f"❌ Task 'process_dataframe' not found. Available tasks: {', '.join(task_ids)}"
                )
        except Exception as e:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = f"❌ Error checking DAG tasks: {str(e)}"

        # DAG execution
        print(f"🔍 Triggering DAG: {dag_name}", flush=True)
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)

        if not dag_run_id:
            test_steps[7]["status"] = "failed"
            test_steps[7]["Result_Message"] = "❌ Failed to trigger DAG"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Monitor the DAG run until completion
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = (
            f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"
        )

        # Capture comprehensive DAG information for debugging (source, import errors, task logs)
        print("📊 Capturing comprehensive DAG information for debugging...", flush=True)
        try:
            comprehensive_dag_info = airflow_instance.get_comprehensive_dag_info(
                dag_id=dag_name,
                dag_run_id=dag_run_id,
                github_manager=github_manager,
            )

            # Add agent code snapshot to comprehensive DAG info (captured earlier)
            if agent_code_snapshot:
                comprehensive_dag_info["agent_code_snapshot"] = agent_code_snapshot
                print(
                    f"📸 Agent code snapshot added to comprehensive DAG info: "
                    f"{agent_code_snapshot['summary']['total_files']} files, "
                    f"{agent_code_snapshot['summary']['total_size_bytes']} bytes",
                    flush=True,
                )
            else:
                print("⚠️ Agent code snapshot not available", flush=True)

            dag_source = comprehensive_dag_info.get("dag_source", {})
            import_errors = comprehensive_dag_info.get("import_errors", [])

            if dag_source.get("source_code"):
                print(
                    f"📄 DAG source code captured ({len(dag_source['source_code'])}, flush=True characters)"
                )
                print(
                    f"📄 Source code preview: {dag_source['source_code'][:200]}...",
                    flush=True,
                )
            else:
                print(
                    "⚠️ DAG source code not available from Airflow - check agent_code_snapshot for actual files",
                    flush=True,
                )

            if import_errors:
                print(f"❌ Found {len(import_errors)}, flush=True import errors")
                for error in import_errors:
                    print(
                        f"   - {error.get('filename', 'Unknown')}, flush=True: {error.get('stack_trace', 'No details')}"
                    )
            else:
                print("✅ No DAG import errors found", flush=True)

            # Attach to test metadata
            test_steps.append(
                {
                    "name": "DAG Information Capture",
                    "description": "Capture comprehensive DAG information for debugging",
                    "status": "passed",
                    "Result_Message": "✅ Comprehensive DAG information captured successfully",
                    "comprehensive_dag_info": comprehensive_dag_info,
                    "dag_source_code": dag_source.get("source_code"),
                    "dag_file_path": dag_source.get("file_path"),
                    "dag_import_errors": import_errors,
                    "task_logs_summary": {
                        task_id: {
                            "state": task_info.get("state"),
                            "duration": task_info.get("duration"),
                            "log_length": len(task_info.get("logs", "")),
                        }
                        for task_id, task_info in comprehensive_dag_info.get(
                            "task_logs", {}
                        ).items()
                    },
                }
            )

        except Exception as e:
            print(f"⚠️ Could not capture comprehensive DAG info: {e}", flush=True)
            test_steps.append(
                {
                    "name": "DAG Information Capture",
                    "description": "Capture comprehensive DAG information for debugging",
                    "status": "failed",
                    "Result_Message": f"❌ Failed to capture DAG information: {str(e)}",
                }
            )

        # Step 9-10: Task Log Validation
        print("🔍 Retrieving task logs to verify DataFrame processing...", flush=True)
        try:
            logs = airflow_instance.get_task_instance_logs(
                dag_id=dag_name, dag_run_id=dag_run_id, task_id="process_dataframe"
            )
            print(
                f"📝 Task logs retrieved. Log content length: {len(logs)}, flush=True characters"
            )
            print(f"📝 Log content preview: {logs[:300]}...", flush=True)

            # Step 9: Check for DataFrame creation with expected data
            expected_names = ["Alice", "Bob", "Charlie", "David", "Eve"]
            expected_values = ["10", "20", "30", "40", "50"]

            names_found = all(name in logs for name in expected_names)
            values_found = all(value in logs for value in expected_values)

            if names_found and values_found:
                test_steps[8]["status"] = "passed"
                test_steps[8]["Result_Message"] = (
                    "✅ DataFrame created with correct data: Alice-Eve with values 10-50"
                )
            else:
                missing_names = [name for name in expected_names if name not in logs]
                missing_values = [
                    value for value in expected_values if value not in logs
                ]
                test_steps[8]["status"] = "failed"
                test_steps[8]["Result_Message"] = (
                    f"❌ DataFrame validation failed. Missing names: {missing_names}, values: {missing_values}"
                )

            # Step 10: Check for mean calculation
            if "Mean value: 30.0" in logs:
                test_steps[9]["status"] = "passed"
                test_steps[9]["Result_Message"] = (
                    "✅ Mean calculation correct: 'Mean value: 30.0' found in logs"
                )
            else:
                test_steps[9]["status"] = "failed"
                test_steps[9]["Result_Message"] = (
                    "❌ Mean calculation not found or incorrect in logs"
                )

        except Exception as e:
            test_steps[8]["status"] = "failed"
            test_steps[8]["Result_Message"] = f"❌ Error retrieving task logs: {str(e)}"
            test_steps[9]["status"] = "failed"
            test_steps[9]["Result_Message"] = f"❌ Error retrieving task logs: {str(e)}"

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    passed_steps = sum([step["status"] == "passed" for step in test_steps])
    total_steps = len(test_steps)
    score = passed_steps / total_steps

    print(
        f"🎯 Validation completed: {passed_steps}/{total_steps} steps passed (Score: {score:.2f}, flush=True)"
    )

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
