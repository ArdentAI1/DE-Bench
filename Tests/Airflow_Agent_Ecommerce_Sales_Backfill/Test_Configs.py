import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG for backfilling e-commerce sales data that was missed due to a system outage.

Our daily sales aggregation pipeline failed from January 15-19, 2024, and we need to backfill the missing metrics.

Requirements:
1. Process historical sales data for the date range January 15-19, 2024
2. The DAG should be idempotent (safe to run multiple times)
3. Support backfill command: `airflow dags backfill sales_backfill_dag -s 2024-01-15 -e 2024-01-19`
4. Name the DAG 'sales_backfill_dag'
5. Create it in a branch called 'feature/sales_backfill'
6. Name the PR 'Add E-commerce Sales Backfill Pipeline'

You need to:
- Analyze the existing database schema and determine what data needs to be processed
- Figure out what metrics should be calculated and where they should be stored
- Create a robust backfill pipeline that can handle the specified date range

The agent should demonstrate the ability to work with complex data relationships and create a production-ready backfill pipeline.
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
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "ecommerce_sales"}],
        }
    }
}
