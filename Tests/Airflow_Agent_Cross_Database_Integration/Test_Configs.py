import os

# AI Agent task for Cross-Database Integration
User_Input = """
We have a multi-cloud mess: customer data lives in PostgreSQL (our CRM), order data lives in MySQL (our legacy e-commerce platform), and we need unified analytics in Snowflake.

PostgreSQL CRM database has:
- customers: customer_id, email, name, segment, lifetime_value, registration_date
- customer_preferences: customer_id, preferred_category, language, currency
- customer_segments: segment_id, segment_name, description

MySQL e-commerce database has:
- orders: order_id, customer_email, order_date, total_amount, status
- order_items: item_id, order_id, product_id, quantity, unit_price
- products: product_id, name, category, price

The challenge: We need to join customers from PostgreSQL with their orders from MySQL (matching on email) and create a unified customer_360 view in Snowflake.

Create an Airflow DAG that:
1. Extracts customer data from PostgreSQL and order data from MySQL IN PARALLEL (not sequential)
2. Loads both into Snowflake staging tables
3. Joins them in Snowflake to create customer_360 table with:
   - All customer info from PostgreSQL
   - Order count, total revenue, average order value from MySQL
   - Days since last order
   - Customer segment info
4. Handles orphaned orders (emails that don't exist in CRM) by putting them in a separate table
5. Runs daily at 1 AM UTC

DAG configuration:
- Name: cross_database_analytics_pipeline
- Branch: BRANCH_NAME
- PR: PR_NAME

We want to see that PostgreSQL and MySQL extraction happens in parallel, not one after the other, to save time.
"""

# Configuration will be generated dynamically by create_model_inputs function
