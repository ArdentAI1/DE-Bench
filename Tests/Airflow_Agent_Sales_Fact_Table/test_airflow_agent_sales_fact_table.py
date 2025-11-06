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


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a sales fact table DAG pipeline.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"sales_fact_table_test_{test_timestamp}_{test_uuid}",
        "runtime_version": "13.1.0",
        "scheduler_size": "small",
    }

    # Initialize PostgreSQL fixture for the sales data
    custom_postgres_config = {
        "resource_id": f"sales_fact_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"sales_data_{test_timestamp}_{test_uuid}",
                "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/schema.sql",
            }
        ],
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_sales_fact_table_test_{test_timestamp}_{test_uuid}",
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

    # Generate dynamic branch and PR names (same pattern as old pytest version)
    pr_title = f"Add Sales Fact Table Creation Pipeline {test_timestamp}_{test_uuid}"
    branch_name = github_resource_data.get("resource_id")

    # Start with the original user input from Test_Configs
    task_description = Test_Configs.User_Input

    # Add merge step to user input (from old pytest version)
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders with dynamic values
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    print(f"🔧 Generated dynamic branch name: {branch_name}", flush=True)
    print(f"🔧 Generated dynamic PR title: {pr_title}", flush=True)

    # Use the helper to automatically create config from all fixtures
    return {
        # Always do this
        **base_model_inputs,
        # Always do this
        "model_configs": create_config_from_fixtures(fixtures),
        # Now with dynamic branch and PR names
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created a sales fact table DAG.

    Expected behavior:
    - DAG should be created with name "sales_fact_creation_dag"
    - DAG should run successfully and create the sales_fact table
    - Table should have proper structure with foreign key constraints
    - Data should be populated with correct business logic

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
            "description": "AI Agent executes task to create Sales Fact Table DAG",
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
            "description": "Verify that sales_fact_creation_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Sales Fact Creation DAG exists in Airflow...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "Sales Fact Table Creation",
            "description": "Verify that sales_fact table was created with data",
            "status": "running",
            "Result_Message": "Checking if sales_fact table exists and has data...",
        },
        {
            "name": "Table Structure Validation",
            "description": "Verify that sales_fact table has expected columns",
            "status": "running",
            "Result_Message": "Validating table structure and columns...",
        },
        {
            "name": "Foreign Key Constraints",
            "description": "Verify that foreign key constraints are properly set up",
            "status": "running",
            "Result_Message": "Checking foreign key constraints...",
        },
        {
            "name": "Data Integrity Validation",
            "description": "Verify that foreign keys reference valid records",
            "status": "running",
            "Result_Message": "Validating data integrity and relationships...",
        },
        {
            "name": "Business Logic Validation",
            "description": "Verify that business logic calculations are correct",
            "status": "running",
            "Result_Message": "Checking business logic calculations...",
        },
    ]

    overall_success = False

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

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
        pr_title = (
            f"Add Sales Fact Table Creation Pipeline {test_timestamp}_{test_uuid}"
        )
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
            test_steps[1][
                "Result_Message"
            ] = f"✅ Git branch '{branch_name}' created successfully"

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

        # Step 3: Check if PR was created and merge it
        print(f"🔍 Checking for PR: {pr_title}", flush=True)
        try:
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
            test_steps[2][
                "Result_Message"
            ] = f"✅ PR '{pr_title}' created and merged successfully"

        except Exception as e:
            test_steps[2]["status"] = "failed"
            test_steps[2][
                "Result_Message"
            ] = f"❌ Error with PR creation/merge: {str(e)}"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 4: Check if GitHub action completed
        print(f"🔍 Waiting for GitHub action to complete...", flush=True)
        try:
            action_status = github_manager.check_if_action_is_complete(
                pr_title=pr_title, return_details=True
            )

            if not action_status["completed"]:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ GitHub action timed out (status: {action_status['status']})"
                test_steps[3]["action_status"] = action_status
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}
            elif not action_status["success"]:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ GitHub action failed (conclusion: {action_status['conclusion']})"
                test_steps[3]["action_status"] = action_status
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}
            else:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = "✅ GitHub action completed successfully"
                test_steps[3]["action_status"] = action_status

        except Exception as e:
            test_steps[3]["status"] = "failed"
            test_steps[3][
                "Result_Message"
            ] = f"❌ Error checking GitHub action: {str(e)}"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 5: Verify Airflow redeployment
        print(f"🔍 Verifying Airflow redeployment...", flush=True)
        try:
            if not airflow_instance.wait_for_airflow_to_be_ready():
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = "❌ Airflow instance did not redeploy successfully"
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

            test_steps[4]["status"] = "passed"
            test_steps[4][
                "Result_Message"
            ] = "✅ Airflow redeployed successfully after GitHub action"

        except Exception as e:
            test_steps[4]["status"] = "failed"
            test_steps[4][
                "Result_Message"
            ] = f"❌ Error verifying Airflow redeployment: {str(e)}"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 6: Verify that sales_fact_creation_dag was created
        dag_name = "sales_fact_creation_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}", flush=True)

        try:
            # Use airflow_instance method to check if DAG exists
            if airflow_instance.verify_airflow_dag_exists(dag_name):
                test_steps[5]["status"] = "passed"
                test_steps[5][
                    "Result_Message"
                ] = f"✅ DAG '{dag_name}' found in Airflow"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5][
                    "Result_Message"
                ] = f"❌ DAG '{dag_name}' not found in Airflow"
                return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        except Exception as e:
            test_steps[5]["status"] = "failed"
            test_steps[5][
                "Result_Message"
            ] = f"❌ Error checking DAG existence: {str(e)}"
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
            test_steps[6][
                "Result_Message"
            ] = f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"

        except Exception as e:
            test_steps[6]["status"] = "failed"
            test_steps[6][
                "Result_Message"
            ] = f"❌ Error triggering/monitoring DAG: {str(e)}"
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

        # Step 8-12: PostgreSQL Database Validation
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

            # Step 8: Check if sales_fact table exists and has data
            try:
                cur.execute("SELECT COUNT(*) FROM sales_fact")
                row_count = cur.fetchone()[0]

                if row_count > 0:
                    test_steps[7]["status"] = "passed"
                    test_steps[7][
                        "Result_Message"
                    ] = f"✅ Found {row_count} records in sales_fact table"
                else:
                    test_steps[7]["status"] = "failed"
                    test_steps[7][
                        "Result_Message"
                    ] = "❌ sales_fact table exists but has no data"

            except psycopg2.Error as e:
                test_steps[7]["status"] = "failed"
                test_steps[7][
                    "Result_Message"
                ] = f"❌ sales_fact table does not exist or is inaccessible: {str(e)}"

            # Step 9: Validate table structure
            try:
                cur.execute(
                    """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'sales_fact'
                    ORDER BY ordinal_position
                """
                )
                columns = cur.fetchall()

                if columns:
                    actual_columns = [col[0] for col in columns]
                    expected_columns = [
                        "sales_id",
                        "transaction_id",
                        "item_id",
                        "customer_id",
                        "quantity",
                        "unit_price",
                        "total_amount",
                        "sale_date",
                    ]

                    missing_columns = [
                        col for col in expected_columns if col not in actual_columns
                    ]

                    if not missing_columns:
                        test_steps[8]["status"] = "passed"
                        test_steps[8][
                            "Result_Message"
                        ] = f"✅ Table structure valid. Columns: {', '.join(actual_columns)}"
                    else:
                        test_steps[8]["status"] = "failed"
                        test_steps[8][
                            "Result_Message"
                        ] = f"❌ Missing expected columns: {', '.join(missing_columns)}"
                else:
                    test_steps[8]["status"] = "failed"
                    test_steps[8][
                        "Result_Message"
                    ] = "❌ Could not retrieve table structure"

            except Exception as e:
                test_steps[8]["status"] = "failed"
                test_steps[8][
                    "Result_Message"
                ] = f"❌ Error validating table structure: {str(e)}"

            # Step 10: Verify foreign key constraints
            try:
                cur.execute(
                    """
                    SELECT 
                        tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_name = 'sales_fact'
                    ORDER BY kcu.column_name
                """
                )
                foreign_keys = cur.fetchall()

                if len(foreign_keys) >= 3:
                    fk_columns = [fk[1] for fk in foreign_keys]
                    expected_fk_columns = ["transaction_id", "item_id", "customer_id"]
                    missing_fks = [
                        col for col in expected_fk_columns if col not in fk_columns
                    ]

                    if not missing_fks:
                        test_steps[9]["status"] = "passed"
                        test_steps[9][
                            "Result_Message"
                        ] = f"✅ Found {len(foreign_keys)} foreign key constraints: {', '.join(fk_columns)}"
                    else:
                        test_steps[9]["status"] = "failed"
                        test_steps[9][
                            "Result_Message"
                        ] = f"❌ Missing foreign key constraints for: {', '.join(missing_fks)}"
                else:
                    test_steps[9]["status"] = "failed"
                    test_steps[9][
                        "Result_Message"
                    ] = f"❌ Expected at least 3 foreign key constraints, found {len(foreign_keys)}"

            except Exception as e:
                test_steps[9]["status"] = "failed"
                test_steps[9][
                    "Result_Message"
                ] = f"❌ Error checking foreign key constraints: {str(e)}"

            # Step 11: Verify data integrity
            try:
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM sales_fact sf
                    LEFT JOIN transactions t ON sf.transaction_id = t.transaction_id
                    LEFT JOIN items i ON sf.item_id = i.item_id
                    LEFT JOIN customers c ON sf.customer_id = c.customer_id
                    WHERE t.transaction_id IS NULL OR i.item_id IS NULL OR c.customer_id IS NULL
                """
                )
                orphaned_records = cur.fetchone()[0]

                if orphaned_records == 0:
                    test_steps[10]["status"] = "passed"
                    test_steps[10][
                        "Result_Message"
                    ] = "✅ All foreign key references are valid"
                else:
                    test_steps[10]["status"] = "failed"
                    test_steps[10][
                        "Result_Message"
                    ] = f"❌ Found {orphaned_records} records with invalid foreign key references"

            except Exception as e:
                test_steps[10]["status"] = "failed"
                test_steps[10][
                    "Result_Message"
                ] = f"❌ Error validating data integrity: {str(e)}"

            # Step 12: Verify business logic
            try:
                # Check that total_amount = quantity * unit_price (allowing small rounding differences)
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM sales_fact 
                    WHERE ABS(total_amount - (quantity * unit_price)) > 0.01
                """
                )
                invalid_totals = cur.fetchone()[0]

                if invalid_totals == 0:
                    test_steps[11]["status"] = "passed"
                    test_steps[11][
                        "Result_Message"
                    ] = "✅ Business logic validation passed: total_amount = quantity * unit_price"
                else:
                    test_steps[11]["status"] = "failed"
                    test_steps[11][
                        "Result_Message"
                    ] = f"❌ Found {invalid_totals} records where total_amount doesn't equal quantity * unit_price"

            except Exception as e:
                test_steps[11]["status"] = "failed"
                test_steps[11][
                    "Result_Message"
                ] = f"❌ Error validating business logic: {str(e)}"

            # Close database connection
            cur.close()
            conn.close()

        except Exception as e:
            # Mark all database-related steps as failed
            for i in range(7, 12):
                if test_steps[i]["status"] == "running":
                    test_steps[i]["status"] = "failed"
                    test_steps[i][
                        "Result_Message"
                    ] = f"❌ Database validation error: {str(e)}"

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
