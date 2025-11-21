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
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]
PR_NAME = f"Add Advanced Data Engineering Pipeline {test_timestamp}_{test_uuid}"
BRANCH_NAME = f"test_airflow_advanced_data_pipeline_{test_timestamp}_{test_uuid}"
FEATURE_BRANCH_NAME = f"feature/{BRANCH_NAME}"


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create an advanced data engineering pipeline DAG.
    
    Supports four deployment providers:
    - "modal": Modal serverless (default for this test)
    - "astro": Astronomer Cloud
    - "aks": Azure Kubernetes Service
    - "ecs": AWS ECS Fargate
    
    Provider can be overridden via AIRFLOW_PROVIDER environment variable.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with configurable deployment provider
    provider = os.getenv("AIRFLOW_PROVIDER", "modal")  # Default to Modal
    
    # Provider-specific configurations
    if provider == "modal":
        custom_airflow_config = {
            "resource_id": BRANCH_NAME,
            "airflow_provider": "modal",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
        }
    elif provider == "aks":
        custom_airflow_config = {
            "resource_id": BRANCH_NAME,
            "airflow_provider": "aks",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "kubernetes_namespace": BRANCH_NAME.replace("_", "-"),
        }
    elif provider == "ecs":
        custom_airflow_config = {
            "resource_id": BRANCH_NAME,
            "airflow_provider": "ecs",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "ecs_namespace": BRANCH_NAME.replace("_", "-"),
        }
    elif provider == "astro":
        custom_airflow_config = {
            "resource_id": BRANCH_NAME,
            "airflow_provider": "astro",
        }
    else:
        raise ValueError(
            f"Unknown AIRFLOW_PROVIDER: {provider}. "
            f"Supported: modal, aks, ecs, astro"
        )

    # Initialize PostgreSQL fixture for the advanced pipeline data
    custom_postgres_config = {
        "resource_id": BRANCH_NAME,
        "databases": [
            {
                "name": f"advanced_pipeline_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": BRANCH_NAME,
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [airflow_fixture, postgres_fixture, github_fixture]


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

    # Start with the original user input from Test_Configs
    task_description = Test_Configs.User_Input

    # Add merge step to user input
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders with dynamic values
    task_description = task_description.replace("BRANCH_NAME", BRANCH_NAME)
    task_description = task_description.replace("PR_NAME", PR_NAME)

    print(f"🔧 Generated dynamic branch name: {BRANCH_NAME}", flush=True)
    print(f"🔧 Generated dynamic PR title: {PR_NAME}", flush=True)

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created an advanced data engineering pipeline DAG.

    Expected behavior:
    - DAG should be created with name "advanced_data_pipeline_dag"
    - DAG should run successfully and create multiple data pipeline tables
    - Data quality checks should be implemented
    - Business intelligence tables should be populated
    - Data lineage and monitoring should be in place

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
            "description": "AI Agent executes task to create Advanced Data Pipeline DAG",
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
            "description": "Verify that advanced_data_pipeline_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Advanced Data Pipeline DAG exists in Airflow...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "Data Cleansing and Validation",
            "description": "Verify data quality checks and validation logic",
            "status": "running",
            "Result_Message": "Checking data cleansing and validation tables...",
        },
        {
            "name": "Data Transformation Tables",
            "description": "Verify cleaned_orders and customer dimension tables",
            "status": "running",
            "Result_Message": "Validating data transformation tables...",
        },
        {
            "name": "Inventory Analysis",
            "description": "Verify inventory_facts and stock calculations",
            "status": "running",
            "Result_Message": "Checking inventory analysis tables...",
        },
        {
            "name": "Customer Analytics",
            "description": "Verify customer_sentiment and segmentation tables",
            "status": "running",
            "Result_Message": "Validating customer analytics tables...",
        },
        {
            "name": "Business Intelligence Tables",
            "description": "Verify sales_facts and performance metrics",
            "status": "running",
            "Result_Message": "Checking business intelligence tables...",
        },
        {
            "name": "Data Quality Monitoring",
            "description": "Verify data quality metrics and monitoring",
            "status": "running",
            "Result_Message": "Validating data quality monitoring...",
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

        # Get fixtures for Airflow, PostgreSQL, and GitHub
        airflow_fixture = None
        postgres_fixture = None
        github_fixture = None

        if fixtures:
            airflow_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )
            postgres_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"),
                None,
            )
            github_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "github_resource"),
                None,
            )

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")
        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")
        if not github_fixture:
            raise Exception("GitHub fixture not found")

        # Get Airflow instance from stored resource data (needed early for GitHub steps)
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not airflow_resource_data:
            raise Exception("Airflow resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        api_headers = airflow_resource_data["api_headers"]
        base_url = airflow_resource_data["base_url"]

        # Get PostgreSQL connection info
        postgres_resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not postgres_resource_data:
            raise Exception("PostgreSQL resource data not available")

        # Get GitHub manager for validation
        github_resource_data = getattr(github_fixture, "_resource_data", None)
        if not github_resource_data:
            raise Exception("GitHub resource data not available")

        github_manager = github_resource_data.get("github_manager")
        if not github_manager:
            raise Exception("GitHub manager not available")

        # Generate the same branch and PR names used in create_model_inputs
        branch_name = FEATURE_BRANCH_NAME

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

            # Capture agent's code snapshot for observability (after branch verification)
            print(
                f"📸 Capturing agent code snapshot from branch: {branch_name}",
                flush=True,
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

        except Exception as e:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = f"❌ Error checking git branch: {str(e)}"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

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

        # Step 3: Check if PR was created and merge it
        print(f"🔍 Checking for PR: {PR_NAME}", flush=True)
        try:
            pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
                pr_title=PR_NAME,
                test_step=test_steps[2],
                commit_title=PR_NAME,
                merge_method="squash",
                build_info=build_info,
            )

            if not pr_exists:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "❌ Unable to find and merge PR"
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

            test_steps[2]["status"] = "passed"
            test_steps[2]["Result_Message"] = (
                f"✅ PR '{PR_NAME}' created and merged successfully"
            )

        except Exception as e:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = (
                f"❌ Error with PR creation/merge: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 4: Check if GitHub action completed
        print(f"🔍 Waiting for GitHub action to complete...", flush=True)
        try:
            action_status = github_manager.check_if_action_is_complete(
                pr_title=PR_NAME, return_details=True
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

        # Step 6: Verify that advanced_data_pipeline_dag was created
        dag_name = "advanced_data_pipeline_dag"
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

        except Exception as e:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = (
                f"❌ Error triggering/monitoring DAG: {str(e)}"
            )
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

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

        # Step 8-13: PostgreSQL Database Validation
        try:
            # Get database connection details
            postgres_config = postgres_resource_data.get("databases", [{}])[0]
            database_name = postgres_config.get("name", "")

            # Use environment variables for connection (should be set by fixture)
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOSTNAME"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USERNAME"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=database_name,
                sslmode="require",
            )
            cur = conn.cursor()

            print(f"🔍 Connected to PostgreSQL database: {database_name}", flush=True)

            # Step 8: Check Data Cleansing and Validation
            try:
                # Check if cleaned_orders table exists and has data
                cur.execute("SELECT COUNT(*) FROM cleaned_orders")
                cleaned_orders_count = cur.fetchone()[0]

                if cleaned_orders_count > 0:
                    # Verify data quality flags were added
                    cur.execute(
                        """
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'cleaned_orders' 
                        AND (column_name LIKE '%quality%' OR column_name LIKE '%valid%')
                    """
                    )
                    quality_columns = cur.fetchall()

                    if len(quality_columns) > 0:
                        test_steps[7]["status"] = "passed"
                        test_steps[7]["Result_Message"] = (
                            f"✅ Data cleansing validated: {cleaned_orders_count} cleaned records with {len(quality_columns)} quality columns"
                        )
                    else:
                        test_steps[7]["status"] = "failed"
                        test_steps[7]["Result_Message"] = (
                            "❌ No data quality columns found in cleaned_orders"
                        )
                else:
                    test_steps[7]["status"] = "failed"
                    test_steps[7]["Result_Message"] = "❌ No cleaned orders data found"

            except psycopg2.Error as e:
                test_steps[7]["status"] = "failed"
                test_steps[7]["Result_Message"] = (
                    f"❌ Data cleansing validation error: {str(e)}"
                )

            # Step 9: Check Data Transformation Tables
            try:
                # Check customer_dim table
                cur.execute("SELECT COUNT(*) FROM customer_dim")
                customer_dim_count = cur.fetchone()[0]

                if customer_dim_count > 0:
                    test_steps[8]["status"] = "passed"
                    test_steps[8]["Result_Message"] = (
                        f"✅ Data transformation validated: {customer_dim_count} customer dimension records"
                    )
                else:
                    test_steps[8]["status"] = "failed"
                    test_steps[8]["Result_Message"] = (
                        "❌ No customer dimension data found"
                    )

            except psycopg2.Error as e:
                test_steps[8]["status"] = "failed"
                test_steps[8]["Result_Message"] = (
                    f"❌ Data transformation validation error: {str(e)}"
                )

            # Step 10: Check Inventory Analysis
            try:
                # Check inventory_fact table
                cur.execute("SELECT COUNT(*) FROM inventory_fact")
                inventory_count = cur.fetchone()[0]

                if inventory_count > 0:
                    test_steps[9]["status"] = "passed"
                    test_steps[9]["Result_Message"] = (
                        f"✅ Inventory analysis validated: {inventory_count} inventory fact records"
                    )
                else:
                    test_steps[9]["status"] = "failed"
                    test_steps[9]["Result_Message"] = "❌ No inventory fact data found"

            except psycopg2.Error as e:
                test_steps[9]["status"] = "failed"
                test_steps[9]["Result_Message"] = (
                    f"❌ Inventory analysis validation error: {str(e)}"
                )

            # Step 11: Check Customer Analytics
            try:
                # Check customer_sentiment table
                cur.execute("SELECT COUNT(*) FROM customer_sentiment")
                sentiment_count = cur.fetchone()[0]

                if sentiment_count > 0:
                    test_steps[10]["status"] = "passed"
                    test_steps[10]["Result_Message"] = (
                        f"✅ Customer analytics validated: {sentiment_count} sentiment records"
                    )
                else:
                    test_steps[10]["status"] = "failed"
                    test_steps[10]["Result_Message"] = (
                        "❌ No customer sentiment data found"
                    )

            except psycopg2.Error as e:
                test_steps[10]["status"] = "failed"
                test_steps[10]["Result_Message"] = (
                    f"❌ Customer analytics validation error: {str(e)}"
                )

            # Step 12: Check Business Intelligence Tables
            try:
                # Check sales_fact table
                cur.execute("SELECT COUNT(*) FROM sales_fact")
                sales_fact_count = cur.fetchone()[0]

                if sales_fact_count > 0:
                    test_steps[11]["status"] = "passed"
                    test_steps[11]["Result_Message"] = (
                        f"✅ Business intelligence validated: {sales_fact_count} sales fact records"
                    )
                else:
                    test_steps[11]["status"] = "failed"
                    test_steps[11]["Result_Message"] = "❌ No sales fact data found"

            except psycopg2.Error as e:
                test_steps[11]["status"] = "failed"
                test_steps[11]["Result_Message"] = (
                    f"❌ Business intelligence validation error: {str(e)}"
                )

            # Step 13: Check Data Quality Monitoring
            try:
                # Check data_quality_metrics table
                cur.execute("SELECT COUNT(*) FROM data_quality_metrics")
                dq_metrics_count = cur.fetchone()[0]

                if dq_metrics_count > 0:
                    test_steps[12]["status"] = "passed"
                    test_steps[12]["Result_Message"] = (
                        f"✅ Data quality monitoring validated: {dq_metrics_count} quality metric records"
                    )
                else:
                    test_steps[12]["status"] = "failed"
                    test_steps[12]["Result_Message"] = (
                        "❌ No data quality metrics found"
                    )

            except psycopg2.Error as e:
                test_steps[12]["status"] = "failed"
                test_steps[12]["Result_Message"] = (
                    f"❌ Data quality monitoring validation error: {str(e)}"
                )

            # Close database connection
            cur.close()
            conn.close()

        except Exception as e:
            # Mark all database-related steps as failed
            for i in range(7, 13):
                if test_steps[i]["status"] == "running":
                    test_steps[i]["status"] = "failed"
                    test_steps[i]["Result_Message"] = (
                        f"❌ Database validation error: {str(e)}"
                    )

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
