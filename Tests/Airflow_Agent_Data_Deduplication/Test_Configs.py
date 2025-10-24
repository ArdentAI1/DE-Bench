import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Deduplicate users into a single user table called 'deduplicated_users'
3. Runs daily at midnight
4. Has a single task named 'deduplicate_users'
5. Name the DAG 'user_deduplication_dag'
6. Create a new feature branch called 'feature/BRANCH_NAME'
7. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
