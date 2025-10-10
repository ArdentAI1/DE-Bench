import os

User_Input = """
Transform the workflow_definition JSON to extract:
- workflow_id, workflow_name, dag_id, description
- node_count (count of nodes in the JSON)
- created_by, customer_id, department

Create an Airflow DAG that:
1. Extracts workflow data from PostgreSQL database 'workflow_db', table 'workflows'
2. Transforms the JSON workflow definitions into analytics format
3. Loads the results into Snowflake database 'analytics_db', table 'workflow_analytics'
4. Runs daily at 3:00 AM UTC
5. Name it workflow_analytics_etl
6. Create a new feature branch called 'feature/BRANCH_NAME'
7. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
