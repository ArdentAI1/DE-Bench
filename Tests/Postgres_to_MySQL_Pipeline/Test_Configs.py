User_Input = """
Create an Airflow DAG that:
1. Extracts sales data from Postgres database 'sales_db', table 'transactions'
2. Calculates daily profit for each user
3. Stores results in MySQL database 'analytics_db', table 'daily_profits'
4. runs daily at 12:00 AM UTC
5. Name it sales_profit_pipeline
6. Name the branch feature/sales_profit_pipeline
7. Call the PR Merge_Sales_Profit_Pipeline
8. Use these DAG settings:
    - retries: 1
    - retry_delay: 20 seconds
"""


Configs = """
Airflow Configuration:
dag_id: sales_profit_pipeline
schedule_interval: @daily
start_date: 2024-01-01
"""
