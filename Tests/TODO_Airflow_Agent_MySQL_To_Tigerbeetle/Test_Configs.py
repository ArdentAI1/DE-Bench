import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Accesses the MySQL database to retrieve access tokens for Plaid and Finch for company 123
2. Uses these access tokens to fetch transaction data from both services
3. Transforms and stores this data in the TigerBeetle database as transactions
4. Runs daily at 1:00 AM UTC
5. Name the DAG 'mysql_to_tigerbeetle'
6. Create it in a branch called 'feature/mysql_to_tigerbeetle'
7. Name the PR 'Add MySQL to TigerBeetle Pipeline'
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
        "mysql": {
            "host": os.getenv("MYSQL_HOST"),
            "port": os.getenv("MYSQL_PORT"),
            "username": os.getenv("MYSQL_USERNAME"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "databases": [{"name": "Access_Tokens"}]
        },
        "tigerbeetle": {
            "cluster_id": "0",
            "replica_addresses": ["127.0.0.1:3001"]
        }
    }
}
