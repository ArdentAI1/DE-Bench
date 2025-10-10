import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Creates cleaned_orders, customer_dim, inventory_fact, customer_sentiment, sales_fact, product_performance, daily_sales_summary, customer_behavior_analysis, and data_quality_metrics tables
2. Implements DQ checks (completeness, accuracy, consistency), lineage capture, reports, and critical alerts
3. Adds audit logging, idempotency, retries, and validation checkpoints across tasks
4. Defines multiple tasks for each table creation
5. Runs daily at midnight
6. Names the DAG 'advanced_data_pipeline_dag'
7. Create a new feature branch called 'feature/BRANCH_NAME'
8. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
