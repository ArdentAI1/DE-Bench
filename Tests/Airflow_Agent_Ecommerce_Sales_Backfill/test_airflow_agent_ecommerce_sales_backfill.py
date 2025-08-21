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
@pytest.mark.four  # Difficulty 4 - involves backfill logic, date range processing, aggregations, and complex validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"sales_backfill_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"ecommerce_sales_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_ecommerce_sales_backfill(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "sales_backfill_dag"
    pr_title = "Add E-commerce Sales Backfill Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
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
            "name": "Checking Backfill Results",
            "description": "Checking if the sales backfill processed historical data correctly",
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

        # SECTION 3: VALIDATE THE RESULTS
        
        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/sales_backfill", test_steps[0])
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

        # Use the airflow instance from the fixture
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Wait for DAG to appear
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        # Test backfill functionality - trigger backfill for the date range
        print("Testing backfill functionality...")
        backfill_start_date = "2024-01-15"
        backfill_end_date = "2024-01-19"
        
        # Trigger backfill (this tests the backfill capability)
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG for backfill")

        # Monitor the DAG run
        print(f"Monitoring backfill DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 4: VERIFY BACKFILL RESULTS
        print("Verifying backfill results in database...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=postgres_resource["created_resources"][0]["name"],
            sslmode="require"
        )
        cursor = conn.cursor()

        # Check if daily sales metrics were created
        cursor.execute("SELECT COUNT(*) FROM daily_sales_metrics;")
        daily_metrics_count = cursor.fetchone()[0]
        
        if daily_metrics_count == 0:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No daily sales metrics were generated by backfill"
            raise AssertionError("Backfill did not generate daily sales metrics")

        # Check if category sales were calculated
        cursor.execute("SELECT COUNT(*) FROM category_sales_daily;")
        category_metrics_count = cursor.fetchone()[0]
        
        if category_metrics_count == 0:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No category sales metrics were generated"
            raise AssertionError("Backfill did not generate category sales metrics")

        # Check if customer segment metrics were calculated
        cursor.execute("SELECT COUNT(*) FROM customer_segment_daily;")
        segment_metrics_count = cursor.fetchone()[0]

        # Validate data quality - check sample metrics
        cursor.execute("""
            SELECT metric_date, total_orders, total_revenue, avg_order_value, unique_customers 
            FROM daily_sales_metrics 
            ORDER BY metric_date 
            LIMIT 5;
        """)
        sample_metrics = cursor.fetchall()
        
        if len(sample_metrics) == 0:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "No sample metrics found for validation"
            raise AssertionError("No daily sales metrics found for validation")

        # Validate that metrics make sense
        for metric_date, total_orders, total_revenue, avg_order_value, unique_customers in sample_metrics:
            if total_orders <= 0 or total_revenue <= 0:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Invalid metrics for {metric_date}: orders={total_orders}, revenue={total_revenue}"
                raise AssertionError(f"Invalid sales metrics calculated for {metric_date}")

        test_steps[2]["status"] = "completed"
        test_steps[2]["Result_Message"] = f"Backfill successful: {daily_metrics_count} daily metrics, {category_metrics_count} category metrics, {segment_metrics_count} segment metrics"

        cursor.close()
        conn.close()

        print("✅ E-commerce Sales Backfill test completed successfully!")
        print(f"📊 Generated {daily_metrics_count} daily metrics")
        print(f"📊 Generated {category_metrics_count} category metrics")
        print(f"📊 Generated {segment_metrics_count} segment metrics")
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
        # SECTION 5: CLEANUP
        if config_results:
            try:
                remove_model_configs(config_results)
                print("✅ Model configs cleaned up")
            except Exception as cleanup_error:
                print(f"⚠️ Error during model config cleanup: {cleanup_error}")
