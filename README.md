# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve. It was designed to test Ardent's Agents

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo into wherever you want. Ideally a tests folder

2. Set Environment variables

  You will have to set a ton of environment variables for the tests to work. This provides the neccesary information for the tests to set up the right environments as well as provide the agent enough information to make solving the problem possible.


## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials. If there is an actual value there already do not change it:

<pre><code>

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"         # MongoDB connection string

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"                         # MySQL host address
MYSQL_PORT="YOUR MYSQL PORT"                         # MySQL port (typically 3306)
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"                 # MySQL username
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"                 # MySQL password

# Postgres
POSTGRES_HOSTNAME="YOUR_POSTGRES_HOSTNAME"           # Postgres hostname
POSTGRES_PORT="YOUR POSTGRES PORT"                   # Postgres port (typically 5432)
POSTGRES_USERNAME="YOUR_POSTGRES_USERNAME"           # Postgres username
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"           # Postgres password

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

# Benchmark
BENCHMARK_ROOT="FULL_PATH_TO_BENCHMARK_FOLDER"       # Full path of the folder you clone the repo into
MODEL_PATH="PATH_TO_YOUR_MODEL"                      # Path to your model
</code></pre>

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model

4. Install Requirements: `pip install -r requirements.txt`

5. Configure your tools and permissions:

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

6. Use pytest to run. 
`pytest -sv -k "keyword"` -- run tests by keyword
`pytest` -- will run all tests
Pytest supports `and` & `or` operators too. Something like `pytest -m "one and two"` will work.

7. A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the necessary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind.

Environment Variables Template:







Notes:

-Tigerbeetle must be set up with VOPR for testing.
-Mongo must have permisions to create and drop collections and databases
-Airflow must be set up with git sync enabled to the repo you provide
-make sure your mySQL password and username are up to date. AWS sets defaults to rotate once a week...
-Postgres must have the postgres db in it to function (i mean u shouldn't have deleted this anyway)
