# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve. It was designed to test Ardent's Agents

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo into wherever you want. Ideally a tests folder

2. Set Environment variables

  You will have to set a ton of environment variables for the tests to work. This provides the neccesary information for the tests to set up the right environments as well as provide the agent enough information to make solving the problem possible.

  a. Set BENCHMARK_ROOT to the full path of the folder you clone the repo into
  b. Set MODEL_PATH to the path to your model
  c. Specific Providers
    1. AWS
      a. ACCESS_KEY_ID_AWS = AWS access key ID
      b. SECRET_ACCESS_KEY_AWS = AWS secret access key
    2. MongoDB
      a. MONGODB_URI = MongoDB connection string
    3. Ardent
      a. ARDENT_PUBLIC_KEY = Ardent public key
      b. ARDENT_SECRET_KEY = Ardent secret key
      c. ARDENT_BASE_URL = Ardent base URL
    4. MySQL
      a. MYSQL_HOST = MySQL host address
      b. MYSQL_PORT = MySQL port (typically 3306)
      c. MYSQL_USERNAME = MySQL username
      d. MYSQL_PASSWORD = MySQL password
    5. Supabase
      a. SUPABASE_PROJECT_URL = Supabase project URL
      b. SUPABASE_API_KEY = Supabase API key
    6. Postgres
      a. POSTGRES_HOSTNAME = Postgres hostname
      b. POSTGRES_PORT = Postgres port (typically 5432)
      c. POSTGRES_USERNAME = Postgres username
      d. POSTGRES_PASSWORD = Postgres password
    7. Snowflake
      a. SNOWFLAKE_ACCOUNT = Snowflake account name
      b. SNOWFLAKE_USER = Snowflake username
      c. SNOWFLAKE_PASSWORD = Snowflake password
      d. SNOWFLAKE_WAREHOUSE = Snowflake warehouse name
    8. Azure SQL
      a. AZURE_SQL_SERVER = Azure SQL server address
      b. AZURE_SQL_USERNAME = Azure SQL username
      c. AZURE_SQL_PASSWORD = Azure SQL password
      d. AZURE_SQL_VERSION = Azure SQL version
    9. Airflow - boots through docker compose for fast booting
      a. AIRFLOW_GITHUB_TOKEN = github token with full repo access
      b. AIRFLOW_REPO = repo URL (e.g., https://github.com/YOUR_ORG/YOUR_REPO)
      c. AIRFLOW_DAG_PATH = path to the dag folder (e.g., dags/)
      d. AIRFLOW_HOST = http://localhost:8888
      e. AIRFLOW_USERNAME = airflow
      f. AIRFLOW_PASSWORD = airflow
    10. Databricks
      a. DATABRICKS_SERVER_HOSTNAME = Databricks server hostname
      b. DATABRICKS_HTTP_PATH = Databricks HTTP path
      c. DATABRICKS_ACCESS_TOKEN = Databricks access token
      d. DATABRICKS_JOBS_WORKSPACE_URL = Databricks workspace URL
      e. DATABRICKS_JOBS_ACCESS_TOKEN = Databricks jobs access token
      f. DATABRICKS_JOBS_GITHUB_TOKEN = GitHub token for Databricks
      g. DATABRICKS_JOBS_REPO = GitHub repo URL for Databricks
      h. DATABRICKS_JOBS_REPO_PATH = Repo path in Databricks


## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials:

<pre><code># AWS Credentials
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"

# Ardent
ARDENT_PUBLIC_KEY="YOUR_ARDENT_PUBLIC_KEY"
ARDENT_SECRET_KEY="YOUR_ARDENT_SECRET_KEY"
ARDENT_BASE_URL="YOUR_ARDENT_BASE_URL"

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"
MYSQL_PORT=3306
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"

# Supabase
SUPABASE_PROJECT_URL="YOUR_SUPABASE_PROJECT_URL"
SUPABASE_API_KEY="YOUR_SUPABASE_API_KEY"

# Postgres
POSTGRES_HOSTNAME="YOUR_POSTGRES_HOSTNAME"
POSTGRES_PORT=5432
POSTGRES_USERNAME="YOUR_POSTGRES_USERNAME"
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"

# Snowflake
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT"
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USER"
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"

# Azure SQL
AZURE_SQL_SERVER="YOUR_AZURE_SQL_SERVER"
AZURE_SQL_USERNAME="YOUR_AZURE_SQL_USERNAME"
AZURE_SQL_PASSWORD="YOUR_AZURE_SQL_PASSWORD"
AZURE_SQL_VERSION=18

# Airflow
AIRFLOW_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"
AIRFLOW_REPO="YOUR_AIRFLOW_REPO_URL"
AIRFLOW_DAG_PATH="dags/"
AIRFLOW_HOST="http://localhost:8888"
AIRFLOW_USERNAME="airflow"
AIRFLOW_PASSWORD="airflow"
AIRFLOW_UID=501
AIRFLOW_GID=0
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"
_AIRFLOW_WWW_USER_USERNAME="airflow"
_AIRFLOW_WWW_USER_PASSWORD="airflow"
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Databricks
DATABRICKS_SERVER_HOSTNAME="YOUR_DATABRICKS_SERVER_HOSTNAME"
DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"
DATABRICKS_ACCESS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"
DATABRICKS_JOBS_WORKSPACE_URL="YOUR_DATABRICKS_WORKSPACE_URL"
DATABRICKS_JOBS_ACCESS_TOKEN="YOUR_DATABRICKS_JOBS_ACCESS_TOKEN"
DATABRICKS_JOBS_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"
DATABRICKS_JOBS_REPO="YOUR_DATABRICKS_REPO_URL"
DATABRICKS_JOBS_REPO_PATH="YOUR_DATABRICKS_REPO_PATH"

# Finch
FINCH_ACCESS_TOKEN="YOUR_FINCH_ACCESS_TOKEN"

# Benchmark
BENCHMARK_ROOT="FULL_PATH_TO_BENCHMARK_FOLDER"
MODEL_PATH="PATH_TO_YOUR_MODEL"</code></pre>

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
