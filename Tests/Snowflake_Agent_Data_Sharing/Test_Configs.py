import os

# AI Agent task for Snowflake Data Sharing
User_Input = """
We're a B2B analytics SaaS and need to share data with our enterprise customers using Snowflake Data Sharing. Each customer should only see their own analytics data, and we want to use Snowflake's zero-copy sharing instead of exporting CSV files.

We have these tables in our analytics database that customers need access to:
- campaign_performance: campaign_id, customer_id, date, impressions, clicks, conversions, spend
- customer_segments: customer_id, segment_name, ltv_score, churn_risk, last_updated
- product_usage_metrics: customer_id, feature_name, usage_count, last_used, date

The problem: These tables contain data for ALL customers mixed together. We need row-level security so Customer A can't see Customer B's data.

Requirements:
1. Create secure views that automatically filter by the logged-in customer (using CURRENT_USER() or session context)
2. Set up a Snowflake Share object that includes only these secure views (not the base tables)
3. Show that when data is queried through the share, customers only see their own data
4. Make sure sensitive columns (like precise LTV scores) are masked or aggregated
5. Set up some basic monitoring to track which customers are accessing what data

The goal is zero-copy data sharing where customers can run analytics in their own Snowflake accounts without us having to export and transfer data.

Can you set this up in our Snowflake provider account and show how it would work for a customer consuming the share?
"""

# Configuration will be generated dynamically by create_model_inputs function
