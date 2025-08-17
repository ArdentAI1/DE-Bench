import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2


from model.Configure_Model import remove_model_configs
from model.Configure_Model import set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.database
@pytest.mark.api_integration
@pytest.mark.three  # Difficulty 3 - involves API integration, DAG creation, PR management, and database validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"earthquake_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"earthquake_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_usgs_earthquake_to_postgresql(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "usgs_earthquake_dag"
    pr_title = "Add USGS Earthquake Data Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    airflow_deployment_name = airflow_resource["deployment_name"]
    
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
            "name": "Checking Database Results",
            "description": "Checking if the earthquake data was properly stored in PostgreSQL",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    try:
        # Get the actual database name from the fixture
        db_name = postgres_resource["created_resources"][0]["name"]
        print(f"Using PostgreSQL database: {db_name}")

        # Update the configs to use the fixture-created database
        Test_Configs.Configs["services"]["postgreSQL"]["databases"][0]["name"] = db_name

        # Set the airflow configuration with the correct values from fixtures
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        
        # Set up model configs using the configuration from Test_Configs
        config_results = set_up_model_configs(Configs=Test_Configs.Configs, custom_info={
            "publicKey": supabase_account_resource["publicKey"],
            "secretKey": supabase_account_resource["secretKey"],
        })

        # SECTION 2: RUN THE MODEL
        # Run the model which should create the earthquake data pipeline
        start_time = time.time()
        model_result = run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs, extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        model_runtime = end_time - start_time

        request.node.user_properties.append(("model_runtime", model_runtime))

        test_steps[0]["status"] = "completed"
        test_steps[0]["Result_Message"] = "DAG creation initiated"

        # SECTION 3: VALIDATE THE RESULTS
        
        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)  # Give the model time to create the branch and PR
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/earthquake_pipeline", test_steps[0])
        if not branch_exists:
            raise Exception(test_steps[0]["Result_Message"])

        pr_exists, test_steps[1] = github_manager.find_and_merge_pr(
            pr_title=pr_title, 
            test_step=test_steps[1], 
            commit_title=pr_title, 
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource["deployment_id"],
                "deploymentName": airflow_resource["deployment_name"],
            }
        )
        if not pr_exists:
            raise Exception("Unable to find and merge PR. Please check the PR title and commit title.")

        # Use the airflow instance from the fixture to pull DAGs from GitHub
        # The fixture already has the Docker instance running
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Use the connection details from the fixture
        airflow_base_url = airflow_resource["base_url"]
        airflow_api_token = airflow_resource["api_token"]
        
        print(f"Connecting to Airflow at: {airflow_base_url}")
        print(f"Using API Token: {airflow_api_token}")

        # Wait for DAG to appear and trigger it
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 3: VERIFY THE OUTCOMES
        print("Verifying database results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=postgres_resource["created_resources"][0]["name"],
            sslmode="require"
        )
        cursor = conn.cursor()

        # Check if any tables were created (agent should have created table(s) for earthquake data)
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """)
        tables = cursor.fetchall()
        
        if not tables:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No tables were created for earthquake data"
            raise AssertionError("No tables were created in PostgreSQL for earthquake data")

        # Find the main earthquake data table (could be named anything)
        earthquake_table = None
        max_row_count = 0
        
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]
            if row_count > max_row_count:
                max_row_count = row_count
                earthquake_table = table_name
        
        if not earthquake_table or max_row_count == 0:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No earthquake data was loaded into any table"
            raise AssertionError("No earthquake data was loaded into PostgreSQL")

        # Get table structure to understand what the agent created
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{earthquake_table}' 
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()
        
        if len(columns) < 3:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = f"Table {earthquake_table} has too few columns ({len(columns)})"
            raise AssertionError(f"Table {earthquake_table} should have more columns to represent earthquake data")

        # Get sample data for validation
        cursor.execute(f"SELECT * FROM {earthquake_table} LIMIT 5;")
        sample_data = cursor.fetchall()
        
        if len(sample_data) == 0:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No sample data found for validation"
            raise AssertionError("No sample earthquake data found for validation")

        # Basic validation - check that we have some non-null data
        non_null_count = 0
        for row in sample_data:
            for value in row:
                if value is not None:
                    non_null_count += 1
        
        if non_null_count < len(sample_data) * 2:  # At least 2 non-null values per row on average
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "Too many null values in earthquake data"
            raise AssertionError("Earthquake data appears to have too many missing values")

        test_steps[2]["status"] = "completed"
        test_steps[2]["Result_Message"] = f"Successfully loaded {max_row_count} earthquake records into table '{earthquake_table}' with {len(columns)} columns"

        cursor.close()
        conn.close()

        print("✅ USGS Earthquake to PostgreSQL pipeline test completed successfully!")
        print(f"📊 Agent created table '{earthquake_table}' with {len(columns)} columns")
        print(f"📊 Loaded {max_row_count} earthquake records into PostgreSQL")
        print(f"🕒 Total test runtime: {model_runtime:.2f} seconds")

    except Exception as e:
        # Mark remaining steps as failed
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        
        print(f"❌ Test failed: {e}")
        raise

    finally:
        # SECTION 4: CLEANUP
        if config_results:
            try:
                remove_model_configs(config_results)
                print("✅ Model configs cleaned up")
            except Exception as cleanup_error:
                print(f"⚠️ Error during model config cleanup: {cleanup_error}")
