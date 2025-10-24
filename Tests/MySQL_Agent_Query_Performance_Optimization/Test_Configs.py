import os

# AI Agent task for MySQL Query Performance Optimization
User_Input = """
Our e-commerce MySQL database is getting really slow during peak hours. Queries that used to take milliseconds are now taking 5-30 seconds, and customers are complaining about timeouts on the checkout page.

The database has these tables:
- customers (100k rows) 
- orders (500k rows)
- order_items (2M rows)
- products (50k rows)

We need you to diagnose and fix the performance issues.

The most problematic queries are:
1. Customer order history - joins all 4 tables and takes 10+ seconds
2. Product search by category and price range - really slow table scans
3. Daily sales reports - GROUP BY queries timing out
4. Finding customers who haven't ordered recently - complex subqueries

Please:
1. Figure out what's causing the slow queries (use EXPLAIN if needed)
2. Add appropriate indexes where they're missing - especially on foreign keys
3. Optimize the worst performing queries
4. If it helps, create summary tables for expensive aggregations

We want queries to complete in under 1 second during normal traffic.
"""

# Configuration will be generated dynamically by create_config function
