import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2
from datetime import datetime, timedelta

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
@pytest.mark.four  # Difficulty 4 - involves complex data engineering, multiple transformations, and advanced validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"advanced_pipeline_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"advanced_pipeline_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_advanced_data_pipeline(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "advanced_data_pipeline_dag"
    pr_title = "Add Advanced Data Engineering Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting Advanced Data Engineering Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
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
            "name": "Checking Data Cleansing and Validation",
            "description": "Verifying data quality checks and validation logic",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Data Transformation Tables",
            "description": "Verifying cleaned_orders and customer dimension tables",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Inventory Analysis",
            "description": "Verifying inventory_facts and stock calculations",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Customer Analytics",
            "description": "Verifying customer_sentiment and segmentation tables",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Business Intelligence Tables",
            "description": "Verifying sales_facts and performance metrics",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Data Quality Monitoring",
            "description": "Verifying data quality metrics and monitoring",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    # SECTION 1: SETUP THE TEST
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = postgres_resource["created_resources"][0]["name"]

    config_results = None  # Initialize before try block
    try:
        # The dags folder is already set up by the fixture
        # The PostgreSQL database is already set up by the postgres_resource fixture

        # Get the actual database name from the fixture
        print(f"Using PostgreSQL database: {created_db_name}")

        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]

        # set the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
        test_configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        test_configs["services"]["airflow"]["username"] = airflow_resource["username"]
        test_configs["services"]["airflow"]["password"] = airflow_resource["password"]
        test_configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        config_results = set_up_model_configs(
            Configs=test_configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        print("Running model to create DAG and PR...")
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=test_configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        print(f"Model execution completed. Result: {model_result}")
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)  # Give the model time to create the branch and PR
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/advanced-data-pipeline", test_steps[0])
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
        print("Verifying advanced data pipeline results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # STEP 3: Check Data Cleansing and Validation
        print("Verifying data cleansing and validation...")
        
        # Check if cleaned_orders table exists and has data
        cur.execute("SELECT COUNT(*) FROM cleaned_orders")
        cleaned_orders_count = cur.fetchone()[0]
        assert cleaned_orders_count > 0, "No cleaned orders data found"
        
        # Verify data quality flags were added
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'cleaned_orders' 
            AND column_name LIKE '%quality%' OR column_name LIKE '%valid%'
        """)
        quality_columns = cur.fetchall()
        assert len(quality_columns) > 0, "No data quality columns found in cleaned_orders"
        
        # Verify invalid records were filtered out
        cur.execute("SELECT COUNT(*) FROM raw_orders WHERE customer_email = 'invalid-email' OR quantity < 0")
        invalid_raw_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cleaned_orders WHERE customer_email = 'invalid-email' OR quantity < 0")
        invalid_cleaned_count = cur.fetchone()[0]
        assert invalid_cleaned_count == 0, "Invalid records were not properly filtered"
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Data cleansing validated: {cleaned_orders_count} cleaned records, {invalid_raw_count - invalid_cleaned_count} invalid records filtered"
        
        # STEP 4: Check Data Transformation Tables
        print("Verifying data transformation tables...")
        
        # Check customer dimension table
        cur.execute("SELECT COUNT(*) FROM customer_dimension")
        customer_dim_count = cur.fetchone()[0]
        assert customer_dim_count > 0, "Customer dimension table not created or empty"
        
        # Verify customer deduplication
        cur.execute("SELECT COUNT(DISTINCT customer_email) FROM cleaned_orders")
        unique_customers_raw = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customer_dimension")
        unique_customers_dim = cur.fetchone()[0]
        assert unique_customers_dim <= unique_customers_raw, "Customer deduplication not working properly"
        
        # Check profit margin calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM cleaned_orders 
            WHERE profit_margin IS NOT NULL AND profit_margin >= 0
        """)
        profit_margin_count = cur.fetchone()[0]
        assert profit_margin_count > 0, "Profit margin calculations not found"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = f"Data transformation validated: {customer_dim_count} customers, {profit_margin_count} records with profit margins"
        
        # STEP 5: Check Inventory Analysis
        print("Verifying inventory analysis...")
        
        # Check inventory_facts table
        cur.execute("SELECT COUNT(*) FROM inventory_facts")
        inventory_facts_count = cur.fetchone()[0]
        assert inventory_facts_count > 0, "Inventory facts table not created or empty"
        
        # Verify stock level calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM inventory_facts 
            WHERE current_stock_level IS NOT NULL AND current_stock_level >= 0
        """)
        stock_level_count = cur.fetchone()[0]
        assert stock_level_count > 0, "Stock level calculations not found"
        
        # Check inventory turnover calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM inventory_facts 
            WHERE inventory_turnover_rate IS NOT NULL
        """)
        turnover_count = cur.fetchone()[0]
        assert turnover_count > 0, "Inventory turnover calculations not found"
        
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = f"Inventory analysis validated: {inventory_facts_count} inventory records, {stock_level_count} stock levels calculated"
        
        # STEP 6: Check Customer Analytics
        print("Verifying customer analytics...")
        
        # Check customer_sentiment table
        cur.execute("SELECT COUNT(*) FROM customer_sentiment")
        sentiment_count = cur.fetchone()[0]
        assert sentiment_count > 0, "Customer sentiment table not created or empty"
        
        # Verify sentiment score calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM customer_sentiment 
            WHERE avg_sentiment_score IS NOT NULL AND avg_sentiment_score BETWEEN 0 AND 1
        """)
        sentiment_score_count = cur.fetchone()[0]
        assert sentiment_score_count > 0, "Sentiment score calculations not found"
        
        # Check customer segmentation
        cur.execute("""
            SELECT COUNT(*) 
            FROM customer_behavior_analysis 
            WHERE customer_segment IS NOT NULL
        """)
        segmentation_count = cur.fetchone()[0]
        assert segmentation_count > 0, "Customer segmentation not found"
        
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"Customer analytics validated: {sentiment_count} sentiment records, {segmentation_count} customer segments"
        
        # STEP 7: Check Business Intelligence Tables
        print("Verifying business intelligence tables...")
        
        # Check sales_facts table
        cur.execute("SELECT COUNT(*) FROM sales_facts")
        sales_facts_count = cur.fetchone()[0]
        assert sales_facts_count > 0, "Sales facts table not created or empty"
        
        # Check product_performance table
        cur.execute("SELECT COUNT(*) FROM product_performance")
        product_perf_count = cur.fetchone()[0]
        assert product_perf_count > 0, "Product performance table not created or empty"
        
        # Check daily_sales_summary
        cur.execute("SELECT COUNT(*) FROM daily_sales_summary")
        daily_summary_count = cur.fetchone()[0]
        assert daily_summary_count > 0, "Daily sales summary not created or empty"
        
        # Verify KPI calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM daily_sales_summary 
            WHERE total_revenue IS NOT NULL AND total_orders IS NOT NULL
        """)
        kpi_count = cur.fetchone()[0]
        assert kpi_count > 0, "KPI calculations not found"
        
        test_steps[6]["status"] = "passed"
        test_steps[6]["Result_Message"] = f"Business intelligence validated: {sales_facts_count} sales facts, {product_perf_count} product performance records"
        
        # STEP 8: Check Data Quality Monitoring
        print("Verifying data quality monitoring...")
        
        # Check data_quality_metrics table
        cur.execute("SELECT COUNT(*) FROM data_quality_metrics")
        dq_metrics_count = cur.fetchone()[0]
        assert dq_metrics_count > 0, "Data quality metrics table not created or empty"
        
        # Verify quality metrics
        cur.execute("""
            SELECT COUNT(*) 
            FROM data_quality_metrics 
            WHERE completeness_score IS NOT NULL AND accuracy_score IS NOT NULL
        """)
        quality_metrics_count = cur.fetchone()[0]
        assert quality_metrics_count > 0, "Data quality metrics not calculated"
        
        # Check audit logging
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name LIKE '%audit%' OR table_name LIKE '%log%'
        """)
        audit_tables_count = cur.fetchone()[0]
        assert audit_tables_count > 0, "No audit logging tables found"
        
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = f"Data quality monitoring validated: {dq_metrics_count} quality metrics, {audit_tables_count} audit tables"
        
        # Final comprehensive validation
        print("Performing final comprehensive validation...")
        
        # Verify data lineage and relationships
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_facts sf
            JOIN cleaned_orders co ON sf.order_id = co.order_id
            JOIN customer_dimension cd ON co.customer_email = cd.customer_email
            JOIN product_catalog pc ON co.product_sku = pc.product_sku
        """)
        lineage_count = cur.fetchone()[0]
        assert lineage_count > 0, "Data lineage relationships not properly established"
        
        # Verify business logic consistency
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_facts 
            WHERE total_amount != (quantity * unit_price)
        """)
        inconsistent_totals = cur.fetchone()[0]
        assert inconsistent_totals == 0, "Business logic inconsistencies found in sales calculations"
        
        print("✓ Successfully validated advanced data pipeline with comprehensive data engineering transformations")
        print(f"✓ All {len(test_steps)} validation steps passed")
        
        # Close database connection
        cur.close()
        conn.close()

    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/advanced-data-pipeline")

        except Exception as e:
            print(f"Error during cleanup: {e}")
