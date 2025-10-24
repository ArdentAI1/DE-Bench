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
8. Create a new feature branch called 'feature/BRANCH_NAME'
9. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
