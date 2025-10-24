import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
You need to:
- Properly evaluate what is required to perform this task, perform any operations to satisfy the requirements

The agent should demonstrate the ability to work with an unfamiliar API and make intelligent decisions about data modeling and pipeline design.

Create an Airflow DAG that pulls earthquake data from the USGS API and stores it in a PostgreSQL database.

API Endpoint: https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=10&orderby=time

Requirements:
1. Given the API endpoint above we want to create a DAG that pulls data from the API and stores it in our postgres database. We want data from the lats week.
2. The DAG should run daily at 6 AM UTC
3. Include proper error handling and retries for API failures
5. Name the DAG 'usgs_earthquake_dag'
6. Create a new feature branch called 'feature/BRANCH_NAME'
7. Create a pull request named 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
