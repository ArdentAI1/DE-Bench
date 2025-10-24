import os

# AI Agent task for PostgreSQL Logical Replication
User_Input = """
We're expanding globally and need to replicate our US database to EU and Asia regions for lower latency. Our application has these tables: users, orders, and products.

Current setup: Single PostgreSQL database in US East handling ~1000 transactions per minute.

Requirements:
1. Set up logical replication so EU and Asia regions can have read-only copies
2. We need to replicate these specific tables: users, orders, products (not all tables)
3. Replication lag should be under 5 seconds for acceptable user experience
4. Changes in the US database should automatically flow to other regions

What we need:
- Configure the primary (publisher) database to send changes
- Set up at least one replica (subscriber) to receive changes
- Make sure new data inserted in primary shows up in subscriber
- Monitor replication to ensure it's working and not falling behind
- Handle any conflicts if they occur

The goal is to have a working replication setup that we can test by inserting data in the primary and seeing it appear in the subscriber within a few seconds.

Tables structure:
- users: id, email, name, region, created_at
- orders: id, user_id, order_date, total_amount, status
- products: id, name, price, inventory, last_updated
"""

# Configuration will be generated dynamically by create_model_inputs function
