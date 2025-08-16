import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Creates a new table called cleaned_orders from the raw_orders table by validating the data and data types
2. Joins the cleaned_orders table with the product_catalog table to get the product details and sales KPIs
3. Builds a customer_dim with deduplicated customer records from the cleaned_orders table.
4. Process raw_inventory into daily inventory_facts with stock positions and inventory KPIs like inventory turnover, days of inventory, slow/fast movers, and COGS.
5. Transforms raw_customer_feedback into customer_sentiment with ratings and sentiment by product/category.
6. Compute CLV and segment customers by purchase behavior.
7. Creates sales_facts, product_performance, daily_sales_summary, customer_behavior_analysis, and data_quality_metrics tables.
8. Implement DQ checks, completeness/accuracy/consistency metrics, lineage capture, reports, and critical alerts.
9. Add audit logging, idempotency, retries, and validation checkpoints across tasks.
10. Define multiple tasks with explicit dependencies.
11. Enable operational logging and monitoring.
12. Run daily at midnight.
13. Name the DAG 'advanced_data_pipeline_dag'.
14. Create it in branch 'feature/advanced-data-pipeline'.
15. Name the PR 'Add Advanced Data Engineering Pipeline'.
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
