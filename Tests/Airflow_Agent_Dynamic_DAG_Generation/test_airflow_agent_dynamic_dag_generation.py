# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import psycopg2
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This test validates dynamic DAG generation from database configuration.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # PostgreSQL for tenant configurations
    custom_postgres_config = {
        "resource_id": f"tenant_configs_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"tenant_config_db_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Snowflake for tenant data warehouses
    custom_snowflake_config = {
        "resource_id": f"tenant_dw_{test_timestamp}_{test_uuid}",
        "database": f"TENANT_DW_{test_timestamp}_{test_uuid}",
        "schema": f"PUBLIC_{test_timestamp}_{test_uuid}",
        "sql_file": None,
    }

    # Airflow for dynamic DAGs - configurable deployment provider
    resource_id = f"dynamic_dag_generation_{test_timestamp}_{test_uuid}"
    provider = os.getenv("AIRFLOW_PROVIDER", "modal")  # Default to Modal
    
    if provider == "modal":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "modal",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
        }
    elif provider == "aks":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "aks",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "kubernetes_namespace": resource_id.replace("_", "-"),
        }
    elif provider == "ecs":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "ecs",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "ecs_namespace": resource_id.replace("_", "-"),
        }
    elif provider == "astro":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "astro",
        }
    else:
        raise ValueError(
            f"Unknown AIRFLOW_PROVIDER: {provider}. "
            f"Supported: modal, aks, ecs, astro"
        )

    # GitHub
    custom_github_config = {
        "resource_id": f"test_airflow_{resource_id}",
        "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/empty-state.zip",
    }

    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [postgres_fixture, snowflake_fixture, airflow_fixture, github_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    """
    from extract_test_configs import create_config_from_fixtures

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

    pr_title = f"Add Dynamic DAG Generation System {test_timestamp}_{test_uuid}"
    branch_name = github_resource_data.get("resource_id")

    task_description = Test_Configs.User_Input
    task_description = github_manager.add_merge_step_to_user_input(task_description)
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    print(f"🔧 Generated dynamic branch name: {branch_name}", flush=True)
    print(f"🔧 Generated dynamic PR title: {pr_title}", flush=True)

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates dynamic DAG generation implementation.
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task",
            "status": "running",
            "Result_Message": "Checking agent execution...",
        },
        {
            "name": "Git Branch Creation",
            "description": "Verify branch created",
            "status": "running",
            "Result_Message": "Checking branch...",
        },
        {
            "name": "PR Creation and Merge",
            "description": "Verify PR merged",
            "status": "running",
            "Result_Message": "Checking PR...",
        },
        {
            "name": "GitHub Action Completion",
            "description": "Verify action completed",
            "status": "running",
            "Result_Message": "Checking action...",
        },
        {
            "name": "Airflow Redeployment",
            "description": "Verify Airflow redeployed",
            "status": "running",
            "Result_Message": "Checking Airflow...",
        },
        {
            "name": "Tenant Config Table",
            "description": "Verify tenant_pipeline_configs table",
            "status": "running",
            "Result_Message": "Checking config table...",
        },
        {
            "name": "Dynamic DAG Factory",
            "description": "Verify dynamic DAG code exists",
            "status": "running",
            "Result_Message": "Checking DAG factory...",
        },
        {
            "name": "Tenant DAGs Generated",
            "description": "Verify 4 tenant DAGs created",
            "status": "running",
            "Result_Message": "Checking generated DAGs...",
        },
        {
            "name": "DAG Execution",
            "description": "Verify tenant DAG runs",
            "status": "running",
            "Result_Message": "Testing DAG execution...",
        },
    ]

    try:
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent failed"
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ Agent completed"

        # Get fixtures
        postgres_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"),
                None,
            )
            if fixtures
            else None
        )
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

        if not all([postgres_fixture, airflow_fixture, github_fixture]):
            raise Exception("Required fixtures not found")

        postgres_resource_data = getattr(postgres_fixture, "_resource_data", None)
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        github_resource_data = getattr(github_fixture, "_resource_data", None)

        airflow_instance = airflow_resource_data["airflow_instance"]
        base_url = airflow_resource_data["base_url"]
        github_manager = github_resource_data.get("github_manager")

        if not github_manager:
            raise Exception("GitHub manager not available")

        pr_title = f"Add Dynamic DAG Generation System {test_timestamp}_{test_uuid}"
        branch_name = github_resource_data.get("resource_id")

        # GitHub workflow
        print(f"🔍 Checking for branch: {branch_name}", flush=True)
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(
            branch_name, test_steps[1]
        )
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
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

        # Determine build_info based on deployment mode
        provider = airflow_resource_data.get("provider")
        
        if provider == "modal":
            # Modal serverless deployment
            build_info = {
                "provider": "modal",
                "modalAppName": airflow_resource_data.get("modal_app_name"),
                "garRegistry": "us-central1-docker.pkg.dev",
                "garProject": "ardent-de-bench",
                "garRepository": "de-bench",
                "baseImage": airflow_resource_data.get("container_image"),
                "modalWorkspace": os.getenv("MODAL_WORKSPACE", "ardent"),
            }
        elif airflow_resource_data.get("k8s_namespace", None) is not None:
            # Kubernetes (AKS) deployment
            build_info = {
                "acrRegistry": os.getenv("AZURE_ACR_NAME"),
                "acrRepository": airflow_resource_data["deployment_id"],
                "k8sNamespace": airflow_resource_data["k8s_namespace"],
                "k8sJobName": airflow_resource_data["resource_id"].replace("_", "-")[
                    :50
                ],
            }
        elif airflow_resource_data.get("ecs_namespace", None) is not None:
            ecr_registry = (
                os.getenv("AWS_ECR_BASE")
                or os.getenv("AWS_ACCOUNT_ID", "").strip()
                + ".dkr.ecr."
                + os.getenv("AWS_REGION", "us-east-1")
                + ".amazonaws.com"
            )
            # ECS deployment
            build_info = {
                "ecrRegistry": ecr_registry,
                "ecrRepository": airflow_resource_data["deployment_id"],
                "ecsCluster": os.getenv("DE_BENCH_ECS_CLUSTER_NAME"),
                "ecsNamespace": airflow_resource_data["ecs_namespace"],
                "ecsServiceName": airflow_resource_data["ecs_namespace"] + "-service",
            }
        else:
            # Astro deployment
            build_info = {
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
                "secretSuffix": airflow_resource_data["secret_suffix"],
            }

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
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
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
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
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
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
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

        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = (
                "❌ Airflow instance did not redeploy successfully"
            )
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to earlier failure"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = (
            "✅ Airflow redeployed successfully after GitHub action"
        )

        # Check tenant config table in PostgreSQL
        postgres_db_name = postgres_resource_data["created_resources"][0]["name"]
        db_connection = postgres_fixture.get_connection(postgres_db_name)
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute("SELECT COUNT(*) FROM tenant_pipeline_configs")
            config_count = db_cursor.fetchone()[0]

            db_cursor.execute(
                "SELECT COUNT(*) FROM tenant_pipeline_configs WHERE enabled = TRUE"
            )
            enabled_count = db_cursor.fetchone()[0]

            if config_count >= 5 and enabled_count == 4:
                test_steps[5]["status"] = "passed"
                test_steps[5]["Result_Message"] = (
                    f"✅ tenant_pipeline_configs has {config_count} configs ({enabled_count} enabled)"
                )
            elif config_count >= 5:
                test_steps[5]["status"] = "partial"
                test_steps[5]["Result_Message"] = (
                    f"⚠️ Table has {config_count} configs but {enabled_count} enabled (expected 4)"
                )
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5]["Result_Message"] = (
                    f"❌ Only {config_count} tenant configs found"
                )

        finally:
            db_cursor.close()
            db_connection.close()

        # Check for dynamic DAG factory code
        test_steps[6]["status"] = "partial"
        test_steps[6]["Result_Message"] = (
            "⚠️ DAG factory code validation requires GitHub inspection"
        )

        # Check for generated tenant DAGs
        # Try to find DAGs matching pattern tenant_*_pipeline
        dag_list = []
        try:
            # This requires Airflow API to list all DAGs
            # We'll check if at least some DAGs exist
            test_steps[7]["status"] = "partial"
            test_steps[7]["Result_Message"] = (
                "⚠️ Tenant DAG count validation requires Airflow API inspection"
            )
        except Exception as e:
            test_steps[7]["status"] = "partial"
            test_steps[7]["Result_Message"] = f"⚠️ Cannot enumerate DAGs: {str(e)}"

        # Try to execute a tenant DAG
        tenant_dag_names = [
            "tenant_acme_corp_pipeline",
            "tenant_beta_inc_pipeline",
            "tenant_gamma_solutions_pipeline",
            "tenant_delta_systems_pipeline",
        ]

        executed_any = False
        executed_dag_name = None
        executed_dag_run_id = None

        for dag_name in tenant_dag_names:
            if airflow_instance.verify_airflow_dag_exists(dag_name):
                print(f"🔍 Triggering tenant DAG: {dag_name}", flush=True)
                dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
                if dag_run_id:
                    # Monitor the DAG run until completion
                    airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
                    executed_any = True
                    executed_dag_name = dag_name
                    executed_dag_run_id = dag_run_id
                    test_steps[8]["status"] = "passed"
                    test_steps[8]["Result_Message"] = (
                        f"✅ Successfully executed tenant DAG: {dag_name} (run_id: {dag_run_id})"
                    )
                    break

        if not executed_any:
            test_steps[8]["status"] = "partial"
            test_steps[8]["Result_Message"] = (
                "⚠️ Could not execute tenant DAGs (may not exist or use different naming)"
            )
        else:
            # Capture comprehensive DAG information for the executed tenant DAG
            print(
                f"📊 Capturing comprehensive DAG information for {executed_dag_name}...",
                flush=True,
            )
            try:
                comprehensive_dag_info = airflow_instance.get_comprehensive_dag_info(
                    dag_id=executed_dag_name,
                    dag_run_id=executed_dag_run_id,
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

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {"score": score, "metadata": {"test_steps": test_steps}}
