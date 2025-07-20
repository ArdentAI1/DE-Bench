import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Uses yfinance to fetch Tesla (TSLA) stock data for the last 10 days
2. Uses pandas to store this data in a PostgreSQL database
3. The data should be stored in a table called 'tesla_stock'
4. The table should include columns: date, open, high, low, close, volume
5. Runs daily at midnight
6. Has a single task named 'fetch_tesla_data'
7. Name the DAG 'tesla_stock_dag'
8. Create it in a branch called 'feature/tesla_stock'
9. Name the PR 'Add Tesla Stock Data Pipeline'
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
            "databases": [{"name": "stock_data"}],  # Changed from airflow to stock_data
        }
    }
} 