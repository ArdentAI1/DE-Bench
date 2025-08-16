import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Reads data from three PostgreSQL tables: transactions, items, and customers
2. Creates a sales fact table called 'sales_fact' with the following structure:
   - sales_id (SERIAL PRIMARY KEY)
   - transaction_id (FOREIGN KEY to transactions.transaction_id)
   - item_id (FOREIGN KEY to items.item_id)
   - customer_id (FOREIGN KEY to customers.customer_id)
   - quantity (INTEGER)
   - unit_price (DECIMAL)
   - total_amount (DECIMAL)
   - sale_date (DATE)
3. Runs daily at midnight
4. Has a single task named 'create_sales_fact_table'
5. Name the DAG 'sales_fact_creation_dag'
6. Create it in a branch called 'feature/sales-fact-table'
7. Name the PR 'Add Sales Fact Table Creation Pipeline'
"""

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
