import os
from dotenv import load_dotenv

load_dotenv()

"""
Configuration for PostgreSQL Database-Side Deduplication Test

This test verifies that AI agents can create database-side deduplication logic
using SQL stored procedures instead of processing data in Airflow containers.
The computation happens entirely within the PostgreSQL database for optimal performance.
"""

# Task description for the AI agent
User_Input = """
Create an Airflow DAG that:
1. Creates a stored procedure called 'deduplicate_users' that deduplicates users into a single user table called 'deduplicated_users'
2. Runs the stored procedure daily at midnight
3. Has a single task named 'deduplicate_users'
4. Name the DAG 'user_deduplication_dag'
5. Create it in a branch called 'feature/database-user-deduplication'
6. Name the PR 'Add Database-Side User Deduplication Pipeline'
"""

# Configuration for database and services
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