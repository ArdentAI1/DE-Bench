import os

User_Input = """
Create an Airflow DAG that:
1. Extracts human-in-loop events from PostgreSQL database 'workflow_db', table 'interventions'
2. Transforms the data to categorize interventions by type:
   - validation_error: Data validation failures requiring human review
   - step_error: Workflow step execution errors requiring intervention
   - external_api_fail: External API failures requiring manual retry or alternative approach
3. Loads the categorized data into PostgreSQL database 'workflow_db', table 'ops_queue' for live UI dashboard
4. Sends Slack/webhook notifications for high-priority interventions using SLACK_APP_URL from environment variables
5. Runs every 5 minutes for real-time ops monitoring
6. Name it hil_ops_dashboard_etl
7. Create a new feature branch called 'feature/BRANCH_NAME'
8. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
