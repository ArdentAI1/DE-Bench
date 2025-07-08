import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create a simple Airflow DAG that:
1. Prints "Hello World" to the logs
2. Runs daily at midnight
3. Has a single task named 'print_hello'
4. Name the DAG 'hello_world_dag'
5. Create it in a branch called 'feature/hello-world'
6. Name the PR 'Add Hello World DAG'
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST"),
            "username": os.getenv("AIRFLOW_USERNAME"),
            "password": os.getenv("AIRFLOW_PASSWORD"),
        }
    }
} 