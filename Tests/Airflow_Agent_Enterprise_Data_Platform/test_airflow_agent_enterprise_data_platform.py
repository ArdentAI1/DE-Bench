import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2
import json
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
@pytest.mark.five  # Difficulty 5 - Enterprise-level complexity with ML, real-time processing, and advanced analytics
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"enterprise_platform_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"enterprise_platform_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_enterprise_data_platform(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "enterprise_data_platform_dag"
    pr_title = "Add Enterprise Data Platform with Advanced Analytics"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting Enterprise Data Platform Test ===")
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
            "name": "Checking Customer 360 Integration",
            "description": "Verifying unified customer profiles and 360-degree view",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Multi-Source Transaction Processing",
            "description": "Verifying unified transaction processing across systems",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Inventory Optimization Platform",
            "description": "Verifying inventory optimization and demand forecasting",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Marketing Attribution and ROI",
            "description": "Verifying marketing attribution and ROI calculations",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Product Analytics and Performance",
            "description": "Verifying product analytics and recommendation systems",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Operational Intelligence",
            "description": "Verifying operational dashboards and optimization",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Advanced Analytics and ML",
            "description": "Verifying machine learning models and predictive analytics",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Data Governance and Compliance",
            "description": "Verifying data governance, lineage, and compliance",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Real-Time Processing",
            "description": "Verifying real-time streaming and event processing",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking Enterprise Architecture",
            "description": "Verifying enterprise data architecture and scalability",
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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/enterprise-data-platform", test_steps[0])
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
        print("Verifying enterprise data platform results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # STEP 3: Check Customer 360 Integration
        print("Verifying Customer 360 Integration...")
        
        # Check unified customer profile table
        cur.execute("SELECT COUNT(*) FROM customer_360_profile")
        customer_360_count = cur.fetchone()[0]
        assert customer_360_count > 0, "Customer 360 profile table not created or empty"
        
        # Verify data quality scoring
        cur.execute("""
            SELECT COUNT(*) 
            FROM customer_360_profile 
            WHERE data_quality_score IS NOT NULL AND data_quality_score BETWEEN 0 AND 100
        """)
        quality_score_count = cur.fetchone()[0]
        assert quality_score_count > 0, "Data quality scoring not implemented"
        
        # Check customer journey mapping
        cur.execute("SELECT COUNT(*) FROM customer_journey_events")
        journey_events_count = cur.fetchone()[0]
        assert journey_events_count > 0, "Customer journey mapping not implemented"
        
        # Verify customer lifetime value calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM customer_360_profile 
            WHERE customer_lifetime_value IS NOT NULL AND customer_lifetime_value > 0
        """)
        clv_count = cur.fetchone()[0]
        assert clv_count > 0, "Customer lifetime value calculations not implemented"
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Customer 360 validated: {customer_360_count} profiles, {quality_score_count} quality scores, {clv_count} CLV calculations"
        
        # STEP 4: Check Multi-Source Transaction Processing
        print("Verifying Multi-Source Transaction Processing...")
        
        # Check unified transactions table
        cur.execute("SELECT COUNT(*) FROM unified_transactions")
        unified_txns_count = cur.fetchone()[0]
        assert unified_txns_count > 0, "Unified transactions table not created or empty"
        
        # Verify currency conversion
        cur.execute("""
            SELECT COUNT(*) 
            FROM unified_transactions 
            WHERE converted_amount_usd IS NOT NULL
        """)
        currency_conversion_count = cur.fetchone()[0]
        assert currency_conversion_count > 0, "Currency conversion not implemented"
        
        # Check transaction reconciliation
        cur.execute("SELECT COUNT(*) FROM transaction_reconciliation")
        reconciliation_count = cur.fetchone()[0]
        assert reconciliation_count > 0, "Transaction reconciliation not implemented"
        
        # Verify fraud detection
        cur.execute("""
            SELECT COUNT(*) 
            FROM unified_transactions 
            WHERE fraud_score IS NOT NULL
        """)
        fraud_detection_count = cur.fetchone()[0]
        assert fraud_detection_count > 0, "Fraud detection not implemented"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = f"Transaction processing validated: {unified_txns_count} transactions, {currency_conversion_count} conversions, {fraud_detection_count} fraud scores"
        
        # STEP 5: Check Inventory Optimization Platform
        print("Verifying Inventory Optimization Platform...")
        
        # Check unified inventory table
        cur.execute("SELECT COUNT(*) FROM unified_inventory")
        unified_inv_count = cur.fetchone()[0]
        assert unified_inv_count > 0, "Unified inventory table not created or empty"
        
        # Verify demand forecasting
        cur.execute("SELECT COUNT(*) FROM demand_forecast")
        demand_forecast_count = cur.fetchone()[0]
        assert demand_forecast_count > 0, "Demand forecasting not implemented"
        
        # Check inventory optimization
        cur.execute("""
            SELECT COUNT(*) 
            FROM inventory_optimization 
            WHERE reorder_point IS NOT NULL AND optimal_stock_level IS NOT NULL
        """)
        optimization_count = cur.fetchone()[0]
        assert optimization_count > 0, "Inventory optimization not implemented"
        
        # Verify multi-location fulfillment
        cur.execute("SELECT COUNT(*) FROM fulfillment_optimization")
        fulfillment_count = cur.fetchone()[0]
        assert fulfillment_count > 0, "Fulfillment optimization not implemented"
        
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = f"Inventory optimization validated: {unified_inv_count} inventory records, {demand_forecast_count} forecasts, {optimization_count} optimizations"
        
        # STEP 6: Check Marketing Attribution and ROI
        print("Verifying Marketing Attribution and ROI...")
        
        # Check marketing attribution table
        cur.execute("SELECT COUNT(*) FROM marketing_attribution")
        attribution_count = cur.fetchone()[0]
        assert attribution_count > 0, "Marketing attribution table not created or empty"
        
        # Verify multi-touch attribution
        cur.execute("""
            SELECT COUNT(*) 
            FROM marketing_attribution 
            WHERE attribution_model IS NOT NULL AND touchpoint_weight IS NOT NULL
        """)
        multi_touch_count = cur.fetchone()[0]
        assert multi_touch_count > 0, "Multi-touch attribution not implemented"
        
        # Check ROI calculations
        cur.execute("""
            SELECT COUNT(*) 
            FROM marketing_roi 
            WHERE cac IS NOT NULL AND ltv IS NOT NULL AND roi_percentage IS NOT NULL
        """)
        roi_count = cur.fetchone()[0]
        assert roi_count > 0, "Marketing ROI calculations not implemented"
        
        # Verify cohort analysis
        cur.execute("SELECT COUNT(*) FROM customer_cohorts")
        cohort_count = cur.fetchone()[0]
        assert cohort_count > 0, "Cohort analysis not implemented"
        
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"Marketing attribution validated: {attribution_count} attributions, {multi_touch_count} multi-touch, {roi_count} ROI calculations"
        
        # STEP 7: Check Product Analytics and Performance
        print("Verifying Product Analytics and Performance...")
        
        # Check product performance table
        cur.execute("SELECT COUNT(*) FROM product_performance_analytics")
        product_perf_count = cur.fetchone()[0]
        assert product_perf_count > 0, "Product performance analytics table not created or empty"
        
        # Verify product scoring
        cur.execute("""
            SELECT COUNT(*) 
            FROM product_performance_analytics 
            WHERE performance_score IS NOT NULL AND performance_score BETWEEN 0 AND 100
        """)
        scoring_count = cur.fetchone()[0]
        assert scoring_count > 0, "Product performance scoring not implemented"
        
        # Check recommendation engine
        cur.execute("SELECT COUNT(*) FROM product_recommendations")
        recommendations_count = cur.fetchone()[0]
        assert recommendations_count > 0, "Product recommendation engine not implemented"
        
        # Verify sentiment analysis
        cur.execute("""
            SELECT COUNT(*) 
            FROM product_sentiment_analysis 
            WHERE sentiment_score IS NOT NULL AND sentiment_score BETWEEN -1 AND 1
        """)
        sentiment_count = cur.fetchone()[0]
        assert sentiment_count > 0, "Product sentiment analysis not implemented"
        
        test_steps[6]["status"] = "passed"
        test_steps[6]["Result_Message"] = f"Product analytics validated: {product_perf_count} performance records, {scoring_count} scores, {recommendations_count} recommendations"
        
        # STEP 8: Check Operational Intelligence
        print("Verifying Operational Intelligence...")
        
        # Check operational dashboards
        cur.execute("SELECT COUNT(*) FROM operational_kpis")
        operational_kpis_count = cur.fetchone()[0]
        assert operational_kpis_count > 0, "Operational KPIs table not created or empty"
        
        # Verify supply chain optimization
        cur.execute("SELECT COUNT(*) FROM supply_chain_optimization")
        supply_chain_count = cur.fetchone()[0]
        assert supply_chain_count > 0, "Supply chain optimization not implemented"
        
        # Check shipping optimization
        cur.execute("""
            SELECT COUNT(*) 
            FROM shipping_optimization 
            WHERE optimal_carrier IS NOT NULL AND cost_savings IS NOT NULL
        """)
        shipping_count = cur.fetchone()[0]
        assert shipping_count > 0, "Shipping optimization not implemented"
        
        # Verify support analytics
        cur.execute("SELECT COUNT(*) FROM support_analytics")
        support_count = cur.fetchone()[0]
        assert support_count > 0, "Support analytics not implemented"
        
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = f"Operational intelligence validated: {operational_kpis_count} KPIs, {supply_chain_count} optimizations, {shipping_count} shipping optimizations"
        
        # STEP 9: Check Advanced Analytics and ML
        print("Verifying Advanced Analytics and ML...")
        
        # Check churn prediction
        cur.execute("SELECT COUNT(*) FROM customer_churn_prediction")
        churn_prediction_count = cur.fetchone()[0]
        assert churn_prediction_count > 0, "Customer churn prediction not implemented"
        
        # Verify sales forecasting
        cur.execute("""
            SELECT COUNT(*) 
            FROM sales_forecast 
            WHERE predicted_sales IS NOT NULL AND confidence_interval IS NOT NULL
        """)
        sales_forecast_count = cur.fetchone()[0]
        assert sales_forecast_count > 0, "Sales forecasting not implemented"
        
        # Check recommendation systems
        cur.execute("SELECT COUNT(*) FROM cross_sell_recommendations")
        cross_sell_count = cur.fetchone()[0]
        assert cross_sell_count > 0, "Cross-sell recommendations not implemented"
        
        # Verify anomaly detection
        cur.execute("""
            SELECT COUNT(*) 
            FROM anomaly_detection 
            WHERE anomaly_score IS NOT NULL AND is_anomaly IS NOT NULL
        """)
        anomaly_count = cur.fetchone()[0]
        assert anomaly_count > 0, "Anomaly detection not implemented"
        
        test_steps[8]["status"] = "passed"
        test_steps[8]["Result_Message"] = f"Advanced analytics validated: {churn_prediction_count} churn predictions, {sales_forecast_count} forecasts, {anomaly_count} anomalies"
        
        # STEP 10: Check Data Governance and Compliance
        print("Verifying Data Governance and Compliance...")
        
        # Check data lineage
        cur.execute("SELECT COUNT(*) FROM data_lineage")
        lineage_count = cur.fetchone()[0]
        assert lineage_count > 0, "Data lineage tracking not implemented"
        
        # Verify data quality monitoring
        cur.execute("""
            SELECT COUNT(*) 
            FROM data_quality_monitoring 
            WHERE quality_score IS NOT NULL AND alert_threshold IS NOT NULL
        """)
        quality_monitoring_count = cur.fetchone()[0]
        assert quality_monitoring_count > 0, "Data quality monitoring not implemented"
        
        # Check audit logging
        cur.execute("SELECT COUNT(*) FROM audit_log")
        audit_count = cur.fetchone()[0]
        assert audit_count > 0, "Audit logging not implemented"
        
        # Verify GDPR compliance
        cur.execute("SELECT COUNT(*) FROM gdpr_compliance")
        gdpr_count = cur.fetchone()[0]
        assert gdpr_count > 0, "GDPR compliance framework not implemented"
        
        test_steps[9]["status"] = "passed"
        test_steps[9]["Result_Message"] = f"Data governance validated: {lineage_count} lineage records, {quality_monitoring_count} quality monitors, {audit_count} audit logs"
        
        # STEP 11: Check Real-Time Processing
        print("Verifying Real-Time Processing...")
        
        # Check real-time analytics
        cur.execute("SELECT COUNT(*) FROM real_time_analytics")
        realtime_count = cur.fetchone()[0]
        assert realtime_count > 0, "Real-time analytics not implemented"
        
        # Verify streaming processing
        cur.execute("SELECT COUNT(*) FROM streaming_events")
        streaming_count = cur.fetchone()[0]
        assert streaming_count > 0, "Streaming event processing not implemented"
        
        # Check real-time alerts
        cur.execute("""
            SELECT COUNT(*) 
            FROM real_time_alerts 
            WHERE alert_type IS NOT NULL AND severity_level IS NOT NULL
        """)
        alerts_count = cur.fetchone()[0]
        assert alerts_count > 0, "Real-time alerting not implemented"
        
        # Verify event-driven architecture
        cur.execute("SELECT COUNT(*) FROM event_processing")
        event_count = cur.fetchone()[0]
        assert event_count > 0, "Event-driven processing not implemented"
        
        test_steps[10]["status"] = "passed"
        test_steps[10]["Result_Message"] = f"Real-time processing validated: {realtime_count} analytics, {streaming_count} events, {alerts_count} alerts"
        
        # STEP 12: Check Enterprise Architecture
        print("Verifying Enterprise Architecture...")
        
        # Check data lake structure
        cur.execute("SELECT COUNT(*) FROM data_lake_metadata")
        data_lake_count = cur.fetchone()[0]
        assert data_lake_count > 0, "Data lake architecture not implemented"
        
        # Verify data marts
        cur.execute("SELECT COUNT(*) FROM data_marts")
        data_marts_count = cur.fetchone()[0]
        assert data_marts_count > 0, "Data marts not implemented"
        
        # Check data catalog
        cur.execute("""
            SELECT COUNT(*) 
            FROM data_catalog 
            WHERE table_name IS NOT NULL AND data_owner IS NOT NULL
        """)
        catalog_count = cur.fetchone()[0]
        assert catalog_count > 0, "Data catalog not implemented"
        
        # Verify performance monitoring
        cur.execute("""
            SELECT COUNT(*) 
            FROM performance_monitoring 
            WHERE execution_time IS NOT NULL AND resource_usage IS NOT NULL
        """)
        performance_count = cur.fetchone()[0]
        assert performance_count > 0, "Performance monitoring not implemented"
        
        test_steps[11]["status"] = "passed"
        test_steps[11]["Result_Message"] = f"Enterprise architecture validated: {data_lake_count} lake records, {data_marts_count} marts, {catalog_count} catalog entries"
        
        # Final comprehensive validation
        print("Performing final comprehensive enterprise validation...")
        
        # Verify cross-system data integration
        cur.execute("""
            SELECT COUNT(*) 
            FROM customer_360_profile c360
            JOIN unified_transactions ut ON c360.customer_id = ut.customer_id
            JOIN unified_inventory ui ON ut.product_sku = ui.product_sku
            JOIN marketing_attribution ma ON c360.customer_id = ma.customer_id
        """)
        integration_count = cur.fetchone()[0]
        assert integration_count > 0, "Cross-system data integration not properly established"
        
        # Verify business logic consistency across systems
        cur.execute("""
            SELECT COUNT(*) 
            FROM unified_transactions 
            WHERE total_amount != (quantity * unit_price + tax_amount - discount_amount)
        """)
        inconsistent_calculations = cur.fetchone()[0]
        assert inconsistent_calculations == 0, "Business logic inconsistencies found across systems"
        
        # Verify data quality across all systems
        cur.execute("""
            SELECT COUNT(*) 
            FROM data_quality_monitoring 
            WHERE quality_score < 80
        """)
        low_quality_count = cur.fetchone()[0]
        print(f"Found {low_quality_count} records with quality score below 80%")
        
        print("✓ Successfully validated enterprise data platform with comprehensive data engineering and analytics")
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
            github_manager.delete_branch("feature/enterprise-data-platform")

        except Exception as e:
            print(f"Error during cleanup: {e}")
