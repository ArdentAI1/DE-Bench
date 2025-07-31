import os

User_Input = """
Create an Airflow DAG that:
1. Extracts sales data from Postgres database 'sales_db', table 'transactions'
2. Calculates daily profit for each user
3. Stores results in MySQL database 'analytics_db', table 'daily_profits'
4. runs daily at 12:00 AM UTC
5. Name it sales_profit_pipeline
6. Name the branch feature/sales_profit_pipeline
7. Call the PR Merge_Sales_Profit_Pipeline
8. Use these DAG settings:
    - retries: 1
    - retry_delay: 20 seconds
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "host": os.getenv("AIRFLOW_HOST"),
            "username": os.getenv("AIRFLOW_USERNAME"),
            "password": os.getenv("AIRFLOW_PASSWORD"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "sales_db"}],
        },
        "mysql": {
            "host": os.getenv("MYSQL_HOST"),
            "port": os.getenv("MYSQL_PORT"),
            "username": os.getenv("MYSQL_USERNAME"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "databases": [{"name": "analytics_db"}],
        },
    }
}
