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
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a simple Hello World DAG pipeline.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    resource_id = f"simple_pipeline_test_{test_timestamp}_{test_uuid}"
    custom_airflow_config = {
        "resource_id": resource_id,
        "airflow_provider": "aks",  # Use ECS deployment
        "container_image": os.getenv("AIRFLOW_CONTAINER_IMAGE"),
        "kubernetes_namespace": resource_id.replace("_", "-"),
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_{resource_id}",
        "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/empty-state.zip",
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
    pr_title = f"Add Hello World DAG {test_timestamp}_{test_uuid}"
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
    Validates that the AI agent successfully created a simple Hello World DAG.

    Expected behavior:
    - DAG should be created with name "hello_world_dag"
    - DAG should run successfully and print "Hello World" to logs
    - GitHub branch and PR should be created and merged

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
            "description": "AI Agent executes task to create Hello World DAG",
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
            "description": "Verify that hello_world_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Hello World DAG exists in Airflow...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "Hello World Output Validation",
            "description": "Verify that DAG prints 'Hello World' to task logs",
            "status": "running",
            "Result_Message": "Checking task logs for 'Hello World' output...",
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
        airflow_fixture = None
        github_fixture = None

        if fixtures:
            airflow_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )
            github_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "github_resource"),
                None,
            )

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")
        if not github_fixture:
            raise Exception("GitHub fixture not found")

        # Get Airflow instance from stored resource data (needed early for GitHub steps)
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not airflow_resource_data:
            raise Exception("Airflow resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        api_headers = airflow_resource_data["api_headers"]
        base_url = airflow_resource_data["base_url"]

        # Get GitHub manager for validation
        github_resource_data = getattr(github_fixture, "_resource_data", None)
        if not github_resource_data:
            raise Exception("GitHub resource data not available")

        github_manager = github_resource_data.get("github_manager")
        if not github_manager:
            raise Exception("GitHub manager not available")

        # Generate the same branch and PR names used in create_model_inputs
        pr_title = f"Add Hello World DAG {test_timestamp}_{test_uuid}"
        branch_name = github_resource_data.get("resource_id")

        # Step 2: Check if git branch was created
        print(f"🔍 Checking for branch: {branch_name}", flush=True)
        try:
            # Wait a bit for the model to create branch and PR
            import time

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

        except Exception as e:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = f"❌ Error checking git branch: {str(e)}"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

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

        if airflow_resource_data.get("k8s_namespace", None) is None:
            build_info = {
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
                "secretSuffix": airflow_resource_data["secret_suffix"],
            }
        else:
            build_info = {
                "acrRegistry": os.getenv("AZURE_ACR_NAME"),
                "acrRepository": airflow_resource_data["deployment_id"],
                "k8sNamespace": airflow_resource_data["k8s_namespace"],
                "k8sJobName": airflow_resource_data["resource_id"].replace("_", "-")[
                    :50
                ],
            }

        # Step 3: Check if PR was created and merge it
        print(f"🔍 Checking for PR: {pr_title}", flush=True)
        try:
            pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
                pr_title=pr_title,
                test_step=test_steps[2],
                commit_title=pr_title,
                merge_method="squash",
                build_info=build_info,
            )

            if not pr_exists:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "❌ Unable to find and merge PR"
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

            test_steps[2]["status"] = "passed"
            test_steps[2]["Result_Message"] = (
                f"✅ PR '{pr_title}' created and merged successfully"
            )

        except Exception as e:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = (
                f"❌ Error with PR creation/merge: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 4: Check GitHub action status with failure details
        print(f"🔍 Waiting for GitHub action to complete...", flush=True)
        try:
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
                # CI failure details are already included in action_status["ci_failure_details"]
                if "ci_failure_details" in action_status:
                    print(
                        f"📋 CI failure details captured: {len(action_status['ci_failure_details'].get('jobs', []), flush=True)} jobs analyzed"
                    )
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}
            else:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = (
                    "✅ GitHub action completed successfully"
                )
                test_steps[3]["action_status"] = action_status

        except Exception as e:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = (
                f"❌ Error checking GitHub action: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 5: Verify Airflow redeployment
        print(f"🔍 Verifying Airflow redeployment...", flush=True)
        try:
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

        except Exception as e:
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = (
                f"❌ Error verifying Airflow redeployment: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 6: Verify that hello_world_dag was created
        dag_name = "hello_world_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}", flush=True)

        try:
            # Use airflow_instance method to check if DAG exists
            if airflow_instance.verify_airflow_dag_exists(dag_name):
                test_steps[5]["status"] = "passed"
                test_steps[5]["Result_Message"] = (
                    f"✅ DAG '{dag_name}' found in Airflow"
                )
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5]["Result_Message"] = (
                    f"❌ DAG '{dag_name}' not found in Airflow"
                )
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        except Exception as e:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = (
                f"❌ Error checking DAG existence: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 7: Trigger DAG and wait for successful execution
        try:
            print(f"🔍 Triggering DAG: {dag_name}", flush=True)

            # Trigger the DAG
            dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)

            if not dag_run_id:
                test_steps[6]["status"] = "failed"
                test_steps[6]["Result_Message"] = "❌ Failed to trigger DAG"
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

            print(f"🔍 Monitoring DAG run {dag_run_id} for completion...", flush=True)

            # Monitor the DAG run until completion
            airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

            test_steps[6]["status"] = "passed"
            test_steps[6]["Result_Message"] = (
                f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"
            )

            # Capture comprehensive DAG information for debugging
            print(
                "📊 Capturing comprehensive DAG information for debugging...",
                flush=True,
            )
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

                # Log key information for debugging
                dag_source = comprehensive_dag_info.get("dag_source", {})
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

                import_errors = comprehensive_dag_info.get("import_errors", [])
                if import_errors:
                    print(f"❌ Found {len(import_errors)}, flush=True import errors")
                    for error in import_errors:
                        print(
                            f"   - {error.get('filename', 'Unknown')}, flush=True: {error.get('stack_trace', 'No details')}"
                        )
                else:
                    print("✅ No DAG import errors found", flush=True)

                # Add comprehensive DAG info to test steps for Braintrust logging
                test_steps.append(
                    {
                        "name": "DAG Information Capture",
                        "description": "Capture comprehensive DAG information for debugging",
                        "status": "passed",
                        "Result_Message": "✅ Comprehensive DAG information captured successfully",
                        "comprehensive_dag_info": comprehensive_dag_info,
                        # Also add key info at top level for easier access in Braintrust
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

            # Step 8: Verify Hello World output in task logs
            print(
                "🔍 Retrieving task logs to verify 'Hello World' output...", flush=True
            )
            try:
                logs = airflow_instance.get_task_instance_logs(
                    dag_id=dag_name, dag_run_id=dag_run_id, task_id="print_hello"
                )
                print(
                    f"📝 Task logs retrieved. Log content length: {len(logs)}, flush=True characters"
                )
                print(f"📝 Log content preview: {logs[:200]}...", flush=True)

                if "Hello World" in logs:
                    test_steps[7]["status"] = "passed"
                    test_steps[7]["Result_Message"] = (
                        "✅ 'Hello World' found in task logs - DAG executed correctly"
                    )
                else:
                    test_steps[7]["status"] = "failed"
                    test_steps[7]["Result_Message"] = (
                        "❌ 'Hello World' not found in task logs"
                    )

            except Exception as e:
                test_steps[7]["status"] = "failed"
                test_steps[7]["Result_Message"] = (
                    f"❌ Error retrieving task logs: {str(e)}"
                )

        except Exception as e:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = (
                f"❌ Error triggering/monitoring DAG: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

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
