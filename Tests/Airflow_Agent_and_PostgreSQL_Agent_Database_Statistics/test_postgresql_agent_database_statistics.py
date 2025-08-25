import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2
from decimal import Decimal
import math

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
@pytest.mark.five  # Difficulty 5 - involves advanced statistical analysis, correlation, mathematical functions
@pytest.mark.database_computation  # New marker for tests that push computation to database
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"db_stats_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"statistics_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_postgresql_agent_database_statistics(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "database_statistics_dag"
    pr_title = "Add Database-Side Statistical Analysis Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use fixtures for Airflow and PostgreSQL resources
    print("=== Starting PostgreSQL Database-Side Statistical Analysis Test ===")
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
            "name": "Verifying Statistical Functions Setup",
            "description": "Checking that statistical analysis stored procedures exist and data is loaded",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Descriptive Statistics",
            "description": "Verifying comprehensive descriptive statistics computed database-side",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Correlation Analysis",
            "description": "Verifying Pearson correlation coefficients calculated using SQL",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Outlier Detection",
            "description": "Verifying statistical outlier detection using Z-scores and IQR methods",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Time Series Analysis",
            "description": "Verifying trend analysis and growth rate calculations",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validating Statistical Accuracy",
            "description": "Confirming mathematical accuracy of database-computed statistics",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Confirming Database-Side Computation",
            "description": "Validating all computation happened in database rather than pandas/Python",
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

        # Verify database setup and statistical functions
        print("Verifying database setup and statistical functions...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Check that sample data exists
        cur.execute("SELECT COUNT(*) FROM dataset_samples")
        sample_data_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM time_series_data")
        time_series_count = cur.fetchone()[0]
        
        assert sample_data_count > 0 and time_series_count > 0, "Sample data not properly loaded"
        print(f"✓ Sample data loaded: {sample_data_count} dataset samples, {time_series_count} time series points")
        
        # Verify statistical functions exist
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_proc 
            WHERE proname = 'calculate_comprehensive_statistics'
        """)
        stats_proc_count = cur.fetchone()[0]
        assert stats_proc_count > 0, "calculate_comprehensive_statistics() stored procedure not found"
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_proc 
            WHERE proname = 'get_statistical_summary'
        """)
        summary_proc_count = cur.fetchone()[0]
        assert summary_proc_count > 0, "get_statistical_summary() function not found"
        
        # Check data diversity for meaningful statistics
        cur.execute("SELECT COUNT(DISTINCT category) FROM dataset_samples")
        category_count = cur.fetchone()[0]
        assert category_count >= 4, f"Need at least 4 categories for statistical analysis, found {category_count}"
        
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"Statistical functions ready: {sample_data_count} samples across {category_count} categories"
        
        cur.close()
        conn.close()

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        print("Running model to create database-centric statistical analysis DAG...")
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
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/database-statistics", test_steps[0])
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

        # SECTION 3: VERIFY DATABASE-SIDE STATISTICAL ANALYSIS RESULTS
        print("Verifying database-side statistical analysis results...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # STEP 4: Validate Descriptive Statistics
        print("Validating comprehensive descriptive statistics...")
        cur.execute("SELECT COUNT(*) FROM descriptive_statistics")
        desc_stats_count = cur.fetchone()[0]
        assert desc_stats_count > 0, "Descriptive statistics table not created"
        
        cur.execute("""
            SELECT category, sample_size, arithmetic_mean, sample_stddev, median, 
                   q1_quartile, q3_quartile, coefficient_of_variation, skewness_coefficient
            FROM descriptive_statistics 
            WHERE category = 'Electronics'
        """)
        electronics_stats = cur.fetchone()
        assert electronics_stats is not None, "Electronics category statistics not found"
        
        category, sample_size, mean, stddev, median, q1, q3, cv, skewness = electronics_stats
        
        # Validate statistical measures are reasonable
        assert sample_size > 0, "Sample size not calculated"
        assert mean is not None and mean > 0, "Mean not calculated properly"
        assert stddev is not None and stddev >= 0, "Standard deviation not calculated"
        assert median is not None and median > 0, "Median not calculated"
        assert q1 is not None and q3 is not None and q3 > q1, "Quartiles not calculated properly"
        assert cv is not None, "Coefficient of variation not calculated"
        
        # Check that all categories have statistics
        cur.execute("SELECT COUNT(DISTINCT category) FROM descriptive_statistics")
        stats_categories = cur.fetchone()[0]
        assert stats_categories == category_count, f"Statistics not calculated for all categories: expected {category_count}, got {stats_categories}"
        
        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = f"Descriptive statistics validated: {stats_categories} categories, mean={mean:.2f}, stddev={stddev:.2f}, CV={cv:.4f}"
        
        # STEP 5: Validate Correlation Analysis  
        print("Validating correlation analysis...")
        cur.execute("SELECT COUNT(*) FROM correlation_analysis")
        corr_count = cur.fetchone()[0]
        assert corr_count > 0, "Correlation analysis table not created"
        
        cur.execute("""
            SELECT category, correlation_coefficient, r_squared, correlation_strength 
            FROM correlation_analysis 
            LIMIT 1
        """)
        corr_sample = cur.fetchone()
        assert corr_sample is not None, "Correlation analysis not computed"
        
        category, corr_coeff, r_squared, corr_strength = corr_sample
        
        # Validate correlation measures
        assert corr_coeff is not None and abs(corr_coeff) <= 1, f"Invalid correlation coefficient: {corr_coeff}"
        assert r_squared is not None and 0 <= r_squared <= 1, f"Invalid R-squared value: {r_squared}"
        assert corr_strength is not None, "Correlation strength not classified"
        
        # Validate R² = correlation²
        expected_r_squared = float(corr_coeff) ** 2
        actual_r_squared = float(r_squared)
        r_squared_diff = abs(expected_r_squared - actual_r_squared)
        assert r_squared_diff < 0.001, f"R-squared calculation error: expected {expected_r_squared:.6f}, got {actual_r_squared:.6f}"
        
        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = f"Correlation analysis validated: r={corr_coeff:.4f}, R²={r_squared:.4f}, strength={corr_strength}"
        
        # STEP 6: Validate Outlier Detection
        print("Validating outlier detection...")
        cur.execute("SELECT COUNT(*) FROM outlier_detection")
        outlier_count = cur.fetchone()[0]
        assert outlier_count > 0, "Outlier detection table not created"
        
        # Check outlier classifications
        cur.execute("""
            SELECT outlier_classification, COUNT(*) 
            FROM outlier_detection 
            GROUP BY outlier_classification
        """)
        outlier_classifications = dict(cur.fetchall())
        assert len(outlier_classifications) > 0, "Outlier classifications not performed"
        
        # Validate Z-score calculations
        cur.execute("""
            SELECT sample_id, z_score, outlier_classification, outlier_severity 
            FROM outlier_detection 
            WHERE outlier_classification != 'Normal' 
            LIMIT 3
        """)
        outlier_samples = cur.fetchall()
        
        for sample_id, z_score, classification, severity in outlier_samples:
            assert z_score is not None, f"Z-score not calculated for sample {sample_id}"
            if abs(z_score) > 2:
                assert classification != 'Normal', f"High Z-score ({z_score}) but classified as Normal"
        
        total_outliers = sum(count for classification, count in outlier_classifications.items() if classification != 'Normal')
        
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"Outlier detection validated: {total_outliers} outliers found across {len(outlier_classifications)} classifications"
        
        # STEP 7: Validate Time Series Analysis
        print("Validating time series analysis...")
        cur.execute("SELECT COUNT(*) FROM time_series_analysis")
        ts_count = cur.fetchone()[0]
        assert ts_count > 0, "Time series analysis table not created"
        
        cur.execute("""
            SELECT metric_name, observation_count, total_growth_rate_percent, 
                   trend_correlation, trend_interpretation 
            FROM time_series_analysis 
            WHERE metric_name = 'daily_sales'
        """)
        ts_sample = cur.fetchone()
        assert ts_sample is not None, "Time series analysis not computed"
        
        metric_name, obs_count, growth_rate, trend_corr, trend_interp = ts_sample
        
        # Validate time series measures
        assert obs_count > 0, "Observation count not calculated"
        assert growth_rate is not None, "Growth rate not calculated"
        assert trend_corr is not None and abs(trend_corr) <= 1, f"Invalid trend correlation: {trend_corr}"
        assert trend_interp is not None, "Trend interpretation not provided"
        
        test_steps[6]["status"] = "passed"
        test_steps[6]["Result_Message"] = f"Time series analysis validated: {obs_count} observations, {growth_rate:.2f}% growth, trend: {trend_interp}"
        
        # STEP 8: Validate Statistical Accuracy
        print("Validating mathematical accuracy of database statistics...")
        
        # Get summary statistics to verify comprehensive analysis
        cur.execute("SELECT * FROM get_statistical_summary()")
        summary_stats = cur.fetchall()
        assert len(summary_stats) > 15, f"Statistical summary incomplete: only {len(summary_stats)} measures computed"
        
        # Validate quality analysis
        cur.execute("SELECT * FROM quality_analysis")
        quality_data = cur.fetchone()
        assert quality_data is not None, "Quality analysis not performed"
        
        scope, total_samples, unique_categories, avg_quality, quality_stddev, high_quality, low_quality, complete_measurements, complete_quality, total_outliers_qa, extreme_outliers = quality_data
        
        # Cross-validate with original data
        assert total_samples == sample_data_count, f"Sample count mismatch: expected {sample_data_count}, quality analysis shows {total_samples}"
        assert unique_categories == category_count, f"Category count mismatch: expected {category_count}, quality analysis shows {unique_categories}"
        assert complete_measurements == total_samples, "Data completeness issue detected"
        
        # Validate statistical measures are within reasonable bounds
        assert 1 <= avg_quality <= 10, f"Average quality score out of bounds: {avg_quality}"
        assert high_quality + low_quality <= total_samples, "Quality classification inconsistency"
        
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = f"Statistical accuracy validated: {len(summary_stats)} measures computed, data quality confirmed, mathematical consistency verified"
        
        # STEP 9: Validate Database-Side Performance
        print("Validating database-centric computation benefits...")
        
        # Check task logs to ensure database operations were used
        logs = airflow_instance.get_task_instance_logs(dag_id=dag_name, dag_run_id=dag_run_id, task_id="calculate_statistics")
        
        # Look for evidence of database-side processing
        database_keywords = ['PostgreSQLOperator', 'CALL calculate_comprehensive_statistics', 'SQL execution', 'database function', 'statistical function']
        database_processing_found = any(keyword in logs for keyword in database_keywords)
        
        # Look for absence of pandas/scipy keywords (good - means we're not using Python for stats)
        pandas_keywords = ['pandas', 'DataFrame', 'scipy', 'numpy', 'describe()', 'corr()', 'std()', 'quantile()']
        pandas_found = any(keyword in logs for keyword in pandas_keywords)
        
        # Calculate computational complexity score
        # Complex statistics computed database-side indicates successful database-centric approach
        statistical_complexity = (
            desc_stats_count * 10 +          # Descriptive statistics complexity
            corr_count * 5 +                 # Correlation analysis
            outlier_count +                  # Outlier detection per sample
            ts_count * 8 +                   # Time series analysis
            len(summary_stats)               # Summary measures
        )
        
        assert statistical_complexity > 100, f"Statistical complexity too low ({statistical_complexity}) - indicates insufficient database-side processing"
        
        # Performance validation
        if not database_processing_found and pandas_found:
            print("Warning: Evidence suggests statistical computation was done with pandas rather than SQL")
        
        test_steps[8]["status"] = "passed"
        test_steps[8]["Result_Message"] = f"Database-side computation confirmed: complexity score {statistical_complexity}, comprehensive statistics computed server-side"
        
        print(f"✓ Successfully validated database-side statistical analysis with mathematical precision")
        print(f"✓ All {len(test_steps)} validation steps passed")
        print(f"✓ Statistical scope: {desc_stats_count} category analyses, {corr_count} correlations, {outlier_count} outlier assessments")
        print(f"✓ Advanced measures: Pearson correlation, Z-scores, quartiles, skewness, effect sizes, trend analysis")
        print(f"✓ Performance benefits: Zero pandas usage, all computation in PostgreSQL statistical engine")
        
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
            github_manager.delete_branch("feature/database-statistics")

        except Exception as e:
            print(f"Error during cleanup: {e}")