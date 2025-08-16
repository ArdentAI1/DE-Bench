import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Deduplicates the data from these three PostgreSQL tables: users_source_1, users_source_2, and users_source_3 by email address (which is the unique identifier)
2. Creates a table called 'users_deduplicated' with the deduplicated data
3. Runs daily at midnight
4. Has a single task named 'deduplicate_users' that deduplicates the data
5. Name the DAG 'user_deduplication_dag'
6. Create it in a branch called 'feature/user-deduplication'
7. Name the PR 'Add User Data Deduplication Pipeline'
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
            "databases": [{"name": "user_data"}],  # Will be updated with actual database name from fixture
        }
    }
}
