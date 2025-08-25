"""
Configuration for PostgreSQL Database-Side Analytics Test

This test verifies that AI agents can create comprehensive analytics processing
entirely within PostgreSQL using stored procedures, statistical functions, and
advanced SQL features instead of processing data in Airflow containers.
"""
import os
from dotenv import load_dotenv

load_dotenv()


# Task description for the AI agent
User_Input = """
Create an Airflow DAG that:
1. Creates a stored procedure that performs a comprehensive analytics analysis of a database, all created database-side:
   - customer_segmentation
   - product_performance
   - regional_market_share
   - rep_performance
   - statistical_sales_trends
   - customer_lifetime_value
2. Runs the stored procedure daily at midnight
3. Has a single task named 'analytics_task'
4. Name the DAG 'database_analytics_dag'
5. Create it in a branch called 'feature/database-analytics'
6. Name the PR 'Add Database-Side Analytics Pipeline'
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
            "databases": [{"name": "sales_data"}],  # Will be updated with actual database name from fixture
        }
    }
}