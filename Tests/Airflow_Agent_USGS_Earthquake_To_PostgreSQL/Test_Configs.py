import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that pulls earthquake data from the USGS API and stores it in a PostgreSQL database.

API Endpoint: https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=10&orderby=time

Requirements:
1. Given the API endpoint above we want to create a DAG that pulls data from the API and stores it in our postgres database. We want data from the lats week.
2. The DAG should run daily at 6 AM UTC
3. Include proper error handling and retries for API failures
5. Name the DAG 'usgs_earthquake_dag'
6. Create it in a branch called 'feature/earthquake_pipeline'
7. Name the PR 'Add USGS Earthquake Data Pipeline'

You need to:
- Properly evaluate what is required to perform this task, perform any operations to satisfy the requirements

The agent should demonstrate the ability to work with an unfamiliar API and make intelligent decisions about data modeling and pipeline design.
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
            "databases": [{"name": "earthquake_data"}],
        }
    }
}
