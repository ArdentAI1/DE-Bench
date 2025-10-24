import os

User_Input = """
Create a simple Airflow DAG that:
1. Prints "Hello World" to the logs
2. Runs daily at midnight
3. Has a single task named 'print_hello'
4. Name the DAG 'hello_world_dag'
5. Create a new feature branch called 'feature/BRANCH_NAME'
6. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
