import os

# AI Agent task for Airflow Sensor & Conditional Branching
User_Input = """
We have a financial data pipeline that needs to wait for external files and database conditions before processing, and the processing path changes based on file size.

Current situation:
- Vendor drops transaction files to S3 daily (sometimes late)
- We have an upstream ETL job that populates a 'data_ready' status in our PostgreSQL database
- Processing differs: small files (< 10k rows) go through fast processing, large files need distributed processing
- Sometimes files don't arrive and we need to handle that gracefully

Can you create an Airflow DAG with this flow:

1. Wait for the vendor file to arrive in S3 (timeout after 2 hours if no file)
2. Wait for the upstream ETL to complete (check database for status='completed' in etl_jobs table)
3. Count the rows in the file
4. Branch based on file size:
   - If < 10k rows: run simple_processing task
   - If >= 10k rows: run distributed_processing task
5. After processing, update the completion status in the database

The DAG should:
- Be named: event_driven_financial_pipeline
- Handle timeouts gracefully (send alerts but don't fail the whole DAG)
- Branch correctly based on runtime conditions
- Wait for both file AND database conditions before proceeding
- Create branch: BRANCH_NAME
- Create PR: PR_NAME

Table structure (already exists):
- etl_jobs: job_name, status, updated_at
"""

# Configuration will be generated dynamically by create_model_inputs function
