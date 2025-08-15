import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that implements a comprehensive data engineering pipeline with the following requirements:

1. DATA CLEANSING AND VALIDATION:
   - Read data from raw_orders table and perform data quality checks
   - Validate email addresses using regex patterns
   - Convert order_date from string to proper DATE format, handling invalid dates
   - Remove records with negative quantities or zero prices
   - Standardize customer names (remove extra spaces, proper case)
   - Flag records with missing or invalid product_sku references

2. DATA TRANSFORMATION:
   - Create a cleaned_orders table with validated and standardized data
   - Join with product_catalog to enrich order data with product details
   - Calculate order totals and profit margins
   - Add data quality flags and validation status
   - Create customer dimension table with deduplicated customer information

3. INVENTORY ANALYSIS:
   - Process raw_inventory data to calculate current stock levels
   - Create inventory_facts table with daily stock positions
   - Calculate inventory turnover rates and days of inventory
   - Identify slow-moving and fast-moving products
   - Track inventory value and cost of goods sold

4. CUSTOMER ANALYTICS:
   - Process raw_customer_feedback to create customer_sentiment table
   - Calculate average ratings and sentiment scores by product/category
   - Create customer_lifetime_value calculations
   - Generate customer segmentation based on purchase behavior
   - Track customer satisfaction trends over time

5. BUSINESS INTELLIGENCE TABLES:
   - Create sales_facts table with comprehensive sales metrics
   - Build product_performance table with sales, inventory, and feedback metrics
   - Generate daily_sales_summary with aggregated KPIs
   - Create customer_behavior_analysis table
   - Build data_quality_metrics table to track data quality over time

6. DATA QUALITY MONITORING:
   - Implement data quality checks and alerting
   - Track data completeness, accuracy, and consistency metrics
   - Create data lineage tracking
   - Generate data quality reports

7. PIPELINE REQUIREMENTS:
   - Use multiple tasks with proper dependencies
   - Implement error handling and retry logic
   - Add data validation checkpoints
   - Create audit logging for all transformations
   - Ensure idempotency for all operations
   - Run daily at 2 AM
   - Name the DAG 'advanced_data_pipeline_dag'
   - Create it in a branch called 'feature/advanced-data-pipeline'
   - Name the PR 'Add Advanced Data Engineering Pipeline'
   - Use pandas for data processing and psycopg2 for database connections
   - Implement proper logging and monitoring
   - Add data quality alerts for critical issues
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "advanced_pipeline"}],  # Will be updated with actual database name from fixture
        }
    }
}
