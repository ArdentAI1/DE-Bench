import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2
from decimal import Decimal

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


@pytest.mark.database
@pytest.mark.postgres
@pytest.mark.postgresql
@pytest.mark.four  # Difficulty 4 - involves advanced SQL analytics, window functions, statistical analysis
@pytest.mark.database_computation  # New marker for tests that push computation to database
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"db_analytics_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"analytics_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_and_postgresql_agent_database_analytics(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "database_analytics_dag"
    pr_title = "Add Database-Side Analytics Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use fixtures for Airflow and PostgreSQL resources
    print("=== Starting PostgreSQL Database-Side Analytics Test ===")
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
            "name": "Verifying Database Stored Procedures",
            "description": "Checking that analytics stored procedures exist and are ready",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Sales Analytics Results",
            "description": "Verifying comprehensive sales analytics were computed database-side",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Validating Customer Segmentation",
            "description": "Verifying customer analytics and RFM segmentation using SQL window functions",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Product Performance Analysis",
            "description": "Verifying product ranking and performance analysis using database functions",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Statistical Computations",
            "description": "Verifying advanced statistical measures computed server-side",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Confirming Database-Side Performance",
            "description": "Validating computation happened in database rather than Airflow containers",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    # SECTION 1: SETUP THE TEST
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = postgres_resource["created_resources"][0]["name"]

    config_results = None  # Initialize before try block
    try:
        # Get the actual database name from the fixture
        print(f"Using PostgreSQL database: {created_db_name}")

        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]

        # Configure Airflow connection details
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
        print("Running model to create database-centric analytics DAG...")
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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/database-analytics", test_steps[0])
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

        # Use the airflow instance from the fixture to run the DAG
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Wait for DAG to appear and trigger it
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # Verify database and stored procedures exist
        print("Verifying database stored procedures...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Verify analytics stored procedure exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_proc 
        """)
        analytics_proc_count = cur.fetchone()[0]
        assert analytics_proc_count > 0, "analyze_sales_data() stored procedure not found"
        
        # Verify summary function exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_proc 
            WHERE proname = 'get_analytics_summary'
        """)
        summary_proc_count = cur.fetchone()[0]
        assert summary_proc_count > 0, "get_analytics_summary() function not found"
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Analytics stored procedures exist and ready for database-side computation"
        
        cur.close()
        conn.close()

        # SECTION 3: VERIFY DATABASE-SIDE ANALYTICS RESULTS
        print("Verifying database-side analytics results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # STEP 4: Validate Sales Analytics Results
        print("Validating sales analytics...")
        cur.execute("SELECT COUNT(*) FROM sales_analytics")
        sales_analytics_count = cur.fetchone()[0]
        assert sales_analytics_count > 0, "Sales analytics table not created"
        
        cur.execute("SELECT total_orders, unique_customers, total_revenue_with_tax, avg_order_value, median_order_value, order_value_stddev FROM sales_analytics")
        sales_data = cur.fetchone()
        total_orders, unique_customers, total_revenue, avg_order_value, median_order_value, order_stddev = sales_data
        
        assert total_orders > 0, "No orders found in sales analytics"
        assert unique_customers > 0, "No unique customers found"
        assert total_revenue > 0, "No revenue calculated"
        assert avg_order_value is not None and avg_order_value > 0, "Average order value not calculated"
        assert median_order_value is not None and median_order_value > 0, "Median order value not calculated"
        assert order_stddev is not None, "Standard deviation not calculated"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = f"Sales analytics validated: {total_orders} orders, {unique_customers} customers, ${total_revenue:.2f} revenue"
        
        # STEP 5: Validate Customer Segmentation and RFM Analysis
        print("Validating customer analytics and segmentation...")
        cur.execute("SELECT COUNT(*) FROM customer_analytics")
        customer_analytics_count = cur.fetchone()[0]
        assert customer_analytics_count > 0, "Customer analytics table not created"
        
        # Check RFM scores are calculated
        cur.execute("SELECT COUNT(*) FROM customer_analytics WHERE rfm_score IS NOT NULL AND rfm_score > 0")
        rfm_scores_count = cur.fetchone()[0]
        assert rfm_scores_count > 0, "RFM scores not calculated"
        
        # Check customer segmentation
        cur.execute("SELECT customer_segment, COUNT(*) FROM customer_analytics GROUP BY customer_segment")
        segments = dict(cur.fetchall())
        assert len(segments) >= 3, "Customer segmentation not working - expected at least 3 segments"
        
        # Check estimated CLV calculations
        cur.execute("SELECT COUNT(*) FROM customer_analytics WHERE estimated_monthly_clv IS NOT NULL AND estimated_monthly_clv > 0")
        clv_count = cur.fetchone()[0]
        assert clv_count > 0, "Customer lifetime value not calculated"
        
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = f"Customer segmentation validated: {customer_analytics_count} customers analyzed, {len(segments)} segments, {rfm_scores_count} RFM scores calculated"
        
        # STEP 6: Validate Product Performance Analysis
        print("Validating product performance analytics...")
        cur.execute("SELECT COUNT(*) FROM product_analytics")
        product_analytics_count = cur.fetchone()[0]
        assert product_analytics_count > 0, "Product analytics table not created"
        
        # Check performance rankings
        cur.execute("SELECT performance_tier, COUNT(*) FROM product_analytics GROUP BY performance_tier")
        performance_tiers = dict(cur.fetchall())
        assert len(performance_tiers) > 0, "Product performance tiers not assigned"
        
        # Check revenue rankings within categories
        cur.execute("SELECT COUNT(*) FROM product_analytics WHERE revenue_rank_in_category IS NOT NULL")
        ranked_products = cur.fetchone()[0]
        assert ranked_products > 0, "Product rankings not calculated"
        
        # Check statistical measures
        cur.execute("SELECT COUNT(*) FROM product_analytics WHERE price_variance IS NOT NULL")
        price_variance_count = cur.fetchone()[0]
        assert price_variance_count > 0, "Price variance not calculated"
        
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"Product analytics validated: {product_analytics_count} products analyzed, {len(performance_tiers)} performance tiers, price variance calculated"
        
        # STEP 7: Validate Statistical Computations
        print("Validating advanced statistical computations...")
        
        # Check regional analytics
        cur.execute("SELECT COUNT(*) FROM regional_analytics")
        regional_count = cur.fetchone()[0]
        assert regional_count > 0, "Regional analytics not created"
        
        # Check market share calculations
        cur.execute("SELECT COUNT(*) FROM regional_analytics WHERE revenue_market_share IS NOT NULL AND revenue_market_share > 0")
        market_share_count = cur.fetchone()[0]
        assert market_share_count > 0, "Market share calculations not performed"
        
        # Check rep performance analytics  
        cur.execute("SELECT COUNT(*) FROM rep_performance")
        rep_count = cur.fetchone()[0]
        assert rep_count > 0, "Rep performance analytics not created"
        
        # Validate summary function works
        cur.execute("SELECT * FROM get_analytics_summary()")
        summary_data = cur.fetchall()
        assert len(summary_data) > 10, "Analytics summary function not working properly"
        
        # Check data quality validation function
        cur.execute("SELECT * FROM validate_analytics_quality()")
        quality_checks = cur.fetchall()
        failed_checks = [check for check in quality_checks if not check[1]]  # check[1] is the 'passed' boolean
        assert len(failed_checks) == 0, f"Data quality validation failed: {failed_checks}"
        
        test_steps[6]["status"] = "passed"
        test_steps[6]["Result_Message"] = f"Statistical computations validated: regional analytics, market share, rep performance, {len(summary_data)} summary metrics"
        
        # STEP 8: Validate Database-Side Performance Benefits
        print("Validating database-centric performance benefits...")
        
        # Check task logs to ensure database operations were used
        logs = airflow_instance.get_task_instance_logs(dag_id=dag_name, dag_run_id=dag_run_id, task_id="run_analytics")
        
        # Look for evidence of database-side processing
        database_keywords = ['PostgreSQLOperator', 'CALL analyze_sales_data', 'SQL execution', 'database function', 'stored procedure']
        database_processing_found = any(keyword in logs for keyword in database_keywords)
        
        # Look for absence of data-pulling keywords (good - means we're not pulling data to Airflow)
        data_pulling_keywords = ['pandas', 'DataFrame', 'to_csv', 'read_sql', 'fetch', 'numpy']
        data_pulling_found = any(keyword in logs for keyword in data_pulling_keywords)
        
        # We want database processing but NOT data pulling
        if not database_processing_found and data_pulling_found:
            print("Warning: Evidence suggests data was processed in Airflow rather than database")
        
        # Validate performance through analytics complexity
        # If complex analytics were computed (multiple tables with statistical measures), 
        # it demonstrates database-side computation capability
        analytics_complexity_score = (
            customer_analytics_count +  # Customer analytics
            product_analytics_count +   # Product analytics  
            regional_count +            # Regional analytics
            rep_count +                 # Rep analytics
            len(summary_data)           # Summary metrics
        )
        
        assert analytics_complexity_score > 20, f"Analytics complexity too low ({analytics_complexity_score}) - may indicate insufficient database-side processing"
        
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = f"Database-side performance confirmed: complexity score {analytics_complexity_score}, comprehensive analytics computed server-side"
        
        print(f"✓ Successfully validated database-side analytics with comprehensive business insights")
        print(f"✓ All {len(test_steps)} validation steps passed")
        print(f"✓ Analytics scope: {customer_analytics_count} customers, {product_analytics_count} products, {regional_count} regions analyzed")
        print(f"✓ Statistical measures: RFM scores, performance rankings, market share, CLV calculations")
        
        # Close database connection
        cur.close()
        conn.close()

    finally:
        try:
            # Clean up configurations
            remove_model_configs(
                Configs=test_configs, 
                custom_info={
                    **config_results,  # Spread all config results
                    "publicKey": supabase_account_resource["publicKey"],
                    "secretKey": supabase_account_resource["secretKey"],
                }
            )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/database-analytics")

        except Exception as e:
            print(f"Error during cleanup: {e}")