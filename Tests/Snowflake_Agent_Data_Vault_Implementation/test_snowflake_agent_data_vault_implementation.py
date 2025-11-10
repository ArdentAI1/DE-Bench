# Braintrust-only Airflow/Snowflake test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import snowflake.connector
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This test validates Data Vault 2.0 implementation in Snowflake.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize PostgreSQL fixture (source OLTP)
    custom_postgres_config = {
        "resource_id": f"data_vault_source_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"oltp_db_{test_timestamp}_{test_uuid}",
                "sql_file": "postgres_schema.sql",
            }
        ],
    }

    # Initialize Snowflake fixture (Data Vault target)
    custom_snowflake_config = {
        "resource_id": f"data_vault_target_{test_timestamp}_{test_uuid}",
        "database": f"DATA_VAULT_DB_{test_timestamp}_{test_uuid}",
        "schema": f"RAW_VAULT_{test_timestamp}_{test_uuid}",
        "sql_file": None,
    }

    # Initialize Airflow fixture
    custom_airflow_config = {
        "resource_id": f"data_vault_etl_{test_timestamp}_{test_uuid}",
    }

    # Initialize GitHub fixture
    custom_github_config = {
        "resource_id": f"test_airflow_data_vault_{test_timestamp}_{test_uuid}",
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

    # Get GitHub fixture for dynamic branch/PR names
    github_fixture = next(
        (f for f in fixtures if f.get_resource_type() == "github_resource"), None
    )

    if not github_fixture:
        raise Exception("GitHub fixture not found")

    github_resource_data = getattr(github_fixture, "_resource_data", None)
    github_manager = github_resource_data.get("github_manager")

    # Generate dynamic branch and PR names
    pr_title = f"Add Data Vault 2.0 ETL Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/data-vault-{test_timestamp}_{test_uuid}"

    task_description = Test_Configs.User_Input
    task_description = github_manager.add_merge_step_to_user_input(task_description)
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    print(f"🔧 Generated branch: {branch_name}", flush=True)
    print(f"🔧 Generated PR: {pr_title}", flush=True)

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates Data Vault 2.0 implementation.
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
            "description": "Verify GitHub action",
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
            "name": "DAG Creation",
            "description": "Verify DAG exists",
            "status": "running",
            "Result_Message": "Checking DAG...",
        },
        {
            "name": "Hub Tables Validation",
            "description": "Verify Hub tables created",
            "status": "running",
            "Result_Message": "Checking Hubs...",
        },
        {
            "name": "Link Tables Validation",
            "description": "Verify Link tables created",
            "status": "running",
            "Result_Message": "Checking Links...",
        },
        {
            "name": "Satellite Tables Validation",
            "description": "Verify Satellite tables",
            "status": "running",
            "Result_Message": "Checking Satellites...",
        },
        {
            "name": "DAG Execution",
            "description": "Verify DAG runs successfully",
            "status": "running",
            "Result_Message": "Running DAG...",
        },
    ]

    try:
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ Agent completed successfully"

        # Get fixtures
        airflow_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )
            if fixtures
            else None
        )
        snowflake_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "snowflake_resource"),
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

        if not all([airflow_fixture, snowflake_fixture, github_fixture]):
            raise Exception("Required fixtures not found")

        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        snowflake_resource_data = getattr(snowflake_fixture, "_resource_data", None)
        github_resource_data = getattr(github_fixture, "_resource_data", None)

        airflow_instance = airflow_resource_data["airflow_instance"]
        github_manager = github_resource_data.get("github_manager")
        database_name = snowflake_resource_data.get("database_name")
        schema_name = snowflake_resource_data.get("schema_name")

        pr_title = f"Add Data Vault 2.0 ETL Pipeline {test_timestamp}_{test_uuid}"
        branch_name = f"feature/data-vault-{test_timestamp}_{test_uuid}"

        # GitHub workflow (steps 2-5)
        time.sleep(10)
        branch_exists, test_steps[1] = github_manager.verify_branch_exists(
            branch_name, test_steps[1]
        )
        if not branch_exists:
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = f"✅ Branch '{branch_name}' created"

        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title,
            test_step=test_steps[2],
            commit_title=pr_title,
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
            },
        )
        if not pr_exists:
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"✅ PR merged"

        action_status = github_manager.check_if_action_is_complete(
            pr_title=pr_title, return_details=True
        )
        if not action_status["completed"] or not action_status["success"]:
            test_steps[3]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "✅ GitHub action completed"

        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = "✅ Airflow redeployed"

        # Check DAG
        dag_name = "data_vault_etl_pipeline"
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"❌ DAG '{dag_name}' not found"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' exists"

        # Connect to Snowflake to check Data Vault structures
        snowflake_conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=database_name,
            schema=schema_name,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        )
        snowflake_cur = snowflake_conn.cursor()

        try:
            # Check Hub tables
            snowflake_cur.execute(
                f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}"
            )
            all_tables = [row[1] for row in snowflake_cur.fetchall()]
            hub_tables = [t for t in all_tables if t.upper().startswith("HUB_")]

            if len(hub_tables) >= 2:
                test_steps[6]["status"] = "passed"
                test_steps[6][
                    "Result_Message"
                ] = f"✅ Found {len(hub_tables)} Hub tables: {hub_tables}"
            else:
                test_steps[6]["status"] = "partial"
                test_steps[6][
                    "Result_Message"
                ] = f"⚠️ Only {len(hub_tables)} Hub table(s) found"

            # Check Link tables
            link_tables = [t for t in all_tables if t.upper().startswith("LINK_")]
            if len(link_tables) >= 1:
                test_steps[7]["status"] = "passed"
                test_steps[7][
                    "Result_Message"
                ] = f"✅ Found {len(link_tables)} Link tables: {link_tables}"
            else:
                test_steps[7]["status"] = "partial"
                test_steps[7][
                    "Result_Message"
                ] = f"⚠️ Only {len(link_tables)} Link table(s) found"

            # Check Satellite tables
            sat_tables = [t for t in all_tables if t.upper().startswith("SAT_")]
            if len(sat_tables) >= 2:
                test_steps[8]["status"] = "passed"
                test_steps[8][
                    "Result_Message"
                ] = f"✅ Found {len(sat_tables)} Satellite tables: {sat_tables}"
            else:
                test_steps[8]["status"] = "partial"
                test_steps[8][
                    "Result_Message"
                ] = f"⚠️ Only {len(sat_tables)} Satellite table(s) found"

        finally:
            snowflake_cur.close()
            snowflake_conn.close()

        # Execute DAG
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if dag_run_id:
            try:
                airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
                test_steps[9]["status"] = "passed"
                test_steps[9]["Result_Message"] = f"✅ DAG executed successfully"
            except:
                test_steps[9]["status"] = "partial"
                test_steps[9][
                    "Result_Message"
                ] = "⚠️ DAG triggered but execution incomplete"
        else:
            test_steps[9]["status"] = "failed"
            test_steps[9]["Result_Message"] = "❌ Failed to trigger DAG"

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {"score": score, "metadata": {"test_steps": test_steps}}
