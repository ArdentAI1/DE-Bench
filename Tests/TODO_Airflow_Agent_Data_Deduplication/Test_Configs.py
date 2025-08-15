import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Reads data from three PostgreSQL tables: users_source_1, users_source_2, and users_source_3
2. Deduplicates the data based on email address (which is the unique identifier)
3. Creates a new table called 'users_deduplicated' with the deduplicated data
4. The deduplicated table should include all columns from the source tables: id, email, first_name, last_name, company, department, role, source
5. For duplicate email addresses, merge the data from all sources, keeping non-null values
6. Runs daily at midnight
7. Has a single task named 'deduplicate_users'
8. Name the DAG 'user_deduplication_dag'
9. Create it in a branch called 'feature/user-deduplication'
10. Name the PR 'Add User Data Deduplication Pipeline'
11. Use pandas for data processing and psycopg2 for database connections
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
