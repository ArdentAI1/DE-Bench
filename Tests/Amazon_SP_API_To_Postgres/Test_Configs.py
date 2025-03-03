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

The DAG should handle incremental updates and maintain data consistency.
"""

Configs = """

Airflow Configuration:
dag_id: amazon_sp_api_to_postgres
schedule_interval: @daily
start_date: 2024-03-01
"""
