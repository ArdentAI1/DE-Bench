import os

# AI Agent task for Snowflake External Tables with S3
User_Input = """
We have a data lake on S3 with CSV and Parquet files (around 100GB per day), but loading everything into Snowflake is getting expensive. Our data analysts need to query this data without importing it all.

The S3 structure looks like:
- s3://bucket/events/year=2024/month=11/day=15/*.parquet (user event data)
- s3://bucket/products/snapshot=2024-11-15/*.csv (product catalog snapshots)
- s3://bucket/transactions/2024/11/*.csv.gz (transaction logs)

Problems we're facing:
1. Files have inconsistent schemas - columns get added/removed over time
2. We need to query by date but don't want to scan all files
3. Some CSV files are missing headers or have extra columns

Can you set up Snowflake external tables so we can query this S3 data directly?

Requirements:
1. Create external stages pointing to these S3 paths
2. Set up external tables that can handle schema changes gracefully
3. Make sure date-based filtering is efficient (partition pruning)
4. Handle missing or extra columns without breaking queries
5. Show that we can query the data without loading it into Snowflake first

The goal is to save on storage costs while still being able to run analytics queries on the S3 data.
"""

# Configuration will be generated dynamically by create_model_inputs function
