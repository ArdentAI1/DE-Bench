import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Uses yfinance to fetch Tesla (TSLA) stock data for the last 10 days
2. Uses pandas to store this data in a PostgreSQL database
3. The data should be stored in a table called 'tesla_stock'
4. The table should include columns: date, open, high, low, close, volume
5. Runs daily at midnight
6. Has a single task named 'fetch_tesla_data'
7. Name the DAG 'tesla_stock_dag'
8. Create a new feature branch called 'feature/BRANCH_NAME'
9. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
