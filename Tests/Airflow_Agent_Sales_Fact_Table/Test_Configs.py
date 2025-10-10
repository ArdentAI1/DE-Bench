import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Creates a sales fact table called 'sales_fact' 
2. The sales_fact table uses foreign keys to the related tables, using `_id` suffix to the table.
3. Runs daily at midnight
4. Has a single task named 'create_sales_fact_table'
5. Name the DAG 'sales_fact_creation_dag'
6. Create a new feature branch called 'feature/BRANCH_NAME'
7. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_config function
