import os

User_Input = """
Create an Airflow DAG that:
1. Connects to Amazon SP-API to fetch order and inventory data
2. Transforms the data into properly structured tables in Postgres
3. Creates the following tables with appropriate relationships:
   - orders (order_id, order_date, status, total_amount)
   - order_items (item_id, order_id, product_id, quantity, price)
   - products (product_id, sku, title, price)
   - inventory (product_id, quantity, last_updated)
4. Ensures proper indexing and foreign key relationships
5. Runs daily to keep data synchronized
6. Name the DAG 'amazon_sp_api_to_postgres'
7. Create it in a branch called 'feature/amazon_sp_api_pipeline'
8. Name the PR 'Add Amazon SP-API to Postgres Pipeline'

The DAG should handle incremental updates and maintain data consistency.
"""

Configs = {
    "services": {
        "airflow": {
            "host": os.getenv("AIRFLOW_HOST"),
            "username": os.getenv("AIRFLOW_USERNAME"),
            "password": os.getenv("AIRFLOW_PASSWORD"),
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "amazon_sales"}],
        },
    }
}
