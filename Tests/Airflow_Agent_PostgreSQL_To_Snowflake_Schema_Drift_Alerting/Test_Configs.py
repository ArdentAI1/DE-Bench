import os

User_Input = """
Create an Airflow DAG that:
1. Extracts schema data from PostgreSQL database 'workflow_db', tables 'step_definitions' and 'workflow_step_runs'
2. Transforms the data to detect schema drift by comparing actual runtime I/O schemas vs expected registry schemas:
   - Compare actual runtime input/output schemas from workflow_step_runs
   - Compare against expected schemas from step_definitions registry
   - If schemas match → no action needed
   - If drift detected → flag the drift and generate schema diff
3. Loads the results into Snowflake database 'drift_db', table 'schema_drift_events'
4. Runs every 30 minutes
5. Name it schema_drift_alerting_etl
7. Use these DAG settings:
    - retries: 2
    - retry_delay: 5 minutes
8. Create a new feature branch called 'feature/BRANCH_NAME'
9. Create a pull request named 'PR_NAME'

Transform the schema data to extract:
- step_id, workflow_name, organization_id, customer_id
- expected_input_schema, expected_output_schema (from step_definitions)
- actual_input_schema, actual_output_schema (from workflow_step_runs)
- drift_detected (boolean flag)
- schema_diff (JSON containing differences)
- drift_type (input_drift, output_drift, or both)
- severity_level (low, medium, high)
- detection_timestamp

The goal is to enable:
- LLM-powered debugging that can auto-patch or alert operations
- Proactive detection of schema changes that could break workflows
- Automated alerting for schema drift events
"""

# Configuration will be generated dynamically by create_model_inputs function
