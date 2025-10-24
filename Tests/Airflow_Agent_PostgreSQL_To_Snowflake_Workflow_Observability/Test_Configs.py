import os

User_Input = """
Transform the execution data to extract:
- workflow_run_id, step_id, workflow_name, organization_id
- start_time, end_time, step_duration_seconds, run_duration_seconds
- status (success/failed), error_message
- customer_id, department, workflow_version
- step_type, step_category
- resource_usage (cpu, memory if available)
- Use these DAG settings:
    - retries: 3
    - retry_delay: 10 minutes

The goal is to enable:
- Reliability measurement (p99 latency, fail rates)
- Billing analysis based on execution metrics
- Long-term retention of workflow execution events

Create an Airflow DAG that:
1. Extracts workflow execution data from PostgreSQL database 'workflow_db', tables 'workflow_runs' and 'workflow_step_runs'
2. Transforms the execution data to compute observability metrics:
   - Calculate run duration and step latency
   - Flag failed runs vs succeeded runs
   - Enrich with organization and workflow metadata
3. Loads the results into Snowflake database 'observability_db', table 'workflow_step_events'
4. Runs every hour at minute 0
5. Name it workflow_observability_etl
6. Create a new feature branch called 'feature/BRANCH_NAME'
7. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
