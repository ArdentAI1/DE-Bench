# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve. It was designed to test Ardent's Agents

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo into wherever you want. Ideally a tests folder

2. Set Environment variables

  You will have to set a ton of environment variables for the tests to work. This provides the neccesary information for the tests to set up the right environments as well as provide the agent enough information to make solving the problem possible.


## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials:

<pre><code># AWS Credentials
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"           # AWS access key ID
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"   # AWS secret access key

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"         # MongoDB connection string

# Ardent
ARDENT_PUBLIC_KEY="YOUR_ARDENT_PUBLIC_KEY"           # Ardent public key
ARDENT_SECRET_KEY="YOUR_ARDENT_SECRET_KEY"           # Ardent secret key
ARDENT_BASE_URL="YOUR_ARDENT_BASE_URL"               # Ardent base URL

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"                         # MySQL host address
MYSQL_PORT=3306                                      # MySQL port (typically 3306)
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"                 # MySQL username
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"                 # MySQL password

# Supabase
SUPABASE_PROJECT_URL="YOUR_SUPABASE_PROJECT_URL"     # Supabase project URL
SUPABASE_API_KEY="YOUR_SUPABASE_API_KEY"             # Supabase API key

# Postgres
POSTGRES_HOSTNAME="YOUR_POSTGRES_HOSTNAME"           # Postgres hostname
POSTGRES_PORT=5432                                   # Postgres port (typically 5432)
POSTGRES_USERNAME="YOUR_POSTGRES_USERNAME"           # Postgres username
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"           # Postgres password

# Snowflake
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT"           # Snowflake account name
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USER"                 # Snowflake username
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"         # Snowflake password
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"       # Snowflake warehouse name

# Azure SQL
AZURE_SQL_SERVER="YOUR_AZURE_SQL_SERVER"             # Azure SQL server address
AZURE_SQL_USERNAME="YOUR_AZURE_SQL_USERNAME"         # Azure SQL username
AZURE_SQL_PASSWORD="YOUR_AZURE_SQL_PASSWORD"         # Azure SQL password
AZURE_SQL_VERSION=18                                 # Azure SQL version

# Airflow
AIRFLOW_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"             # GitHub token with full repo access
AIRFLOW_REPO="YOUR_AIRFLOW_REPO_URL"                 # Repo URL (e.g., https://github.com/YOUR_ORG/YOUR_REPO)
AIRFLOW_DAG_PATH="dags/"                             # Path to the DAG folder
AIRFLOW_HOST="http://localhost:8888"                 # Airflow host URL
AIRFLOW_USERNAME="airflow"                           # Airflow username
AIRFLOW_PASSWORD="airflow"                           # Airflow password
AIRFLOW_UID=501                                      # User ID for Airflow
AIRFLOW_GID=0                                        # Group ID for Airflow
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"           # Docker image for Airflow
_AIRFLOW_WWW_USER_USERNAME="airflow"                 # Airflow web UI username
_AIRFLOW_WWW_USER_PASSWORD="airflow"                 # Airflow web UI password
AIRFLOW__CORE__LOAD_EXAMPLES=false                   # Whether to load example DAGs

# Databricks
DATABRICKS_SERVER_HOSTNAME="YOUR_DATABRICKS_SERVER_HOSTNAME"   # Databricks server hostname
DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"               # Databricks HTTP path
DATABRICKS_ACCESS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"         # Databricks access token
DATABRICKS_JOBS_WORKSPACE_URL="YOUR_DATABRICKS_WORKSPACE_URL"  # Databricks workspace URL
DATABRICKS_JOBS_ACCESS_TOKEN="YOUR_DATABRICKS_JOBS_ACCESS_TOKEN"  # Databricks jobs access token
DATABRICKS_JOBS_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"               # GitHub token for Databricks
DATABRICKS_JOBS_REPO="YOUR_DATABRICKS_REPO_URL"                # GitHub repo URL for Databricks
DATABRICKS_JOBS_REPO_PATH="YOUR_DATABRICKS_REPO_PATH"          # Repo path in Databricks

# Finch
FINCH_ACCESS_TOKEN="YOUR_FINCH_ACCESS_TOKEN"                   # Finch access token

# Benchmark
BENCHMARK_ROOT="FULL_PATH_TO_BENCHMARK_FOLDER"                 # Full path of the folder you clone the repo into
MODEL_PATH="PATH_TO_YOUR_MODEL"                                # Path to your model
</code></pre>

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model

4. Install requirements.txt with pip install -r requirements.txt

5. Configure your tools and permissions:

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

6. Use pytest to run. Pytest to run all or pytest -m "category" to run all tests of a specific category. Pytest supports and and or operators too. Something like pytest -m "one and two" will work.

7. A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the necessary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind.

Environment Variables Template:







Notes:

-Tigerbeetle must be set up with VOPR for testing.
-Mongo must have permisions to create and drop collections and databases
-Airflow must be set up with git sync enabled to the repo you provide
-make sure your mySQL password and username are up to date. AWS sets defaults to rotate once a week...
-Postgres must have the postgres db in it to function (i mean u shouldn't have deleted this anyway)
