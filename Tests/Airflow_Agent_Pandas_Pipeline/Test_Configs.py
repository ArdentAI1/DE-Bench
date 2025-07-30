import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Uses pandas to create a simple DataFrame with columns 'name' and 'value'
2. Adds exactly 5 rows of sample data to the DataFrame with these values:
   - Alice, 10
   - Bob, 20
   - Charlie, 30
   - David, 40
   - Eve, 50
3. Calculates the mean of the 'value' column (which should be 30)
4. Prints the entire DataFrame and the exact text "Mean value: 30.0" to the logs
5. Runs daily at midnight
6. Has a single task named 'process_dataframe'
7. Name the DAG 'pandas_dataframe_dag'
8. Create it in a branch called 'feature/pandas_dataframe'
9. Name the PR 'Add Pandas DataFrame Processing DAG'
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