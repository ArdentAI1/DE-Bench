import os

# AI Agent task for Snowflake Data Vault 2.0 Implementation
User_Input = """
We're modernizing our data warehouse and need to implement Data Vault 2.0 in Snowflake. Our current OLTP database in PostgreSQL has customers, products, and orders data that needs to be historized properly.

Current PostgreSQL tables:
- customers: id, name, email, phone, address, city, state, registration_date
- products: id, name, category, price, supplier_id, description, created_at
- orders: id, customer_id, order_date, status, total_amount, payment_method  
- order_items: id, order_id, product_id, quantity, unit_price, discount

We need a Data Vault model in Snowflake that:
1. Creates Hub tables for each business entity (customer, product, order) with hash keys
2. Creates Link tables for relationships (customer-order, order-product)
3. Creates Satellite tables to store all the descriptive attributes with full history
4. Uses hash keys (MD5 or similar) for Hub and Link identification
5. Tracks changes over time in Satellites (effective dates, hash diff for change detection)
6. Builds an Airflow DAG to ETL data from PostgreSQL to Snowflake following Data Vault patterns

The ETL should:
- Extract data from PostgreSQL
- Generate hash keys for business keys
- Load Hubs (insert only new business keys)
- Load Links (insert only new relationships)
- Load Satellites (insert new versions when attributes change, using hash_diff)
- Run daily to keep the Data Vault updated

Create the DAG with:
- Name: data_vault_etl_pipeline
- Schedule: Daily
- Branch: BRANCH_NAME
- PR: PR_NAME

We want proper Data Vault 2.0 methodology so downstream teams can build information marts on top of this foundation.
"""

# Configuration will be generated dynamically by create_model_inputs function
