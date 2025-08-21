import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Creates cleaned_orders, customer_dim, inventory_fact, customer_sentiment, sales_fact, product_performance, daily_sales_summary, customer_behavior_analysis, and data_quality_metrics tables
2. Implements DQ checks (completeness, accuracy, consistency), lineage capture, reports, and critical alerts
3. Adds audit logging, idempotency, retries, and validation checkpoints across tasks
4. Defines multiple tasks for each table creation
5. Runs daily at midnight
6. Names the DAG 'advanced_data_pipeline_dag'
7. Creates it in branch 'feature/advanced-data-pipeline'
8. Names the PR 'Add Advanced Data Engineering Pipeline'
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
