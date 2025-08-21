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
# AWS Credentials
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"
MYSQL_PORT=3306
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"

# Supabase
SUPABASE_PROJECT_URL="YOUR_SUPABASE_PROJECT_URL"
SUPABASE_API_KEY="YOUR_SUPABASE_API_KEY"
SUPABASE_URL="YOUR_SUPABASE_URL"
SUPABASE_SERVICE_ROLE_KEY="YOUR_SUPABASE_SERVICE_ROLE_KEY"
SUPABASE_JWT_SECRET="YOUR_SUPABASE_JWT_SECRET"

# PostgreSQL
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

# Airflow Configuration
AIRFLOW_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"
AIRFLOW_REPO="YOUR_AIRFLOW_REPO_URL"
AIRFLOW_DAG_PATH="dags/"
AIRFLOW_REQUIREMENTS_PATH="Requirements/"
AIRFLOW_HOST="http://localhost:8888"
AIRFLOW_USERNAME="airflow"
AIRFLOW_PASSWORD="airflow"
AIRFLOW_UID=501
AIRFLOW_GID=0
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"
_AIRFLOW_WWW_USER_USERNAME="airflow"
_AIRFLOW_WWW_USER_PASSWORD="airflow"
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Databricks Configuration
DATABRICKS_HOST="YOUR_DATABRICKS_HOST"
DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"
DATABRICKS_CLUSTER_ID="YOUR_DATABRICKS_CLUSTER_ID"
DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"
DATABRICKS_JOBS_WORKSPACE_URL="YOUR_DATABRICKS_WORKSPACE_URL"
DATABRICKS_JOBS_ACCESS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"
DATABRICKS_JOBS_GITHUB_TOKEN="YOUR_DATABRICKS_GITHUB_TOKEN"
DATABRICKS_JOBS_REPO="YOUR_DATABRICKS_REPO_URL"
DATABRICKS_JOBS_REPO_PATH="YOUR_DATABRICKS_REPO_PATH"

# Finch API
FINCH_ACCESS_TOKEN="YOUR_FINCH_ACCESS_TOKEN"

# Astronomer Cloud Configuration
ASTRO_WORKSPACE_ID="YOUR_ASTRO_WORKSPACE_ID"
ASTRO_ACCESS_TOKEN="YOUR_ASTRO_ACCESS_TOKEN"
ASTRO_CLOUD_PROVIDER="aws"
ASTRO_REGION="us-east-1"
</code></pre>

### Custom Variables for Your Setup

Below are custom variables for your specific setup. We set up the Ardent configs as an example:

<pre><code>
# Ardent AI Configuration (Example Custom Setup)
ARDENT_PUBLIC_KEY="YOUR_ARDENT_PUBLIC_KEY"
ARDENT_SECRET_KEY="YOUR_ARDENT_SECRET_KEY"
ARDENT_BASE_URL="http://localhost:8000"
</code></pre>

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model

4. Install Dependencies:
   - **UV (Recommended)**: `uv sync` (or `pip install uv && uv sync` if UV is not installed)
   - **Legacy pip**: `pip install -r requirements.txt` (still supported for backward compatibility)

5. Set up and run the Docker Compose environment:

```bash
# Build and start the containers
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build
```

6. Run tests from inside the container:

```bash
# Enter the container
docker-compose exec de-bench bash

# Run tests with UV (recommended)
uv run pytest -n auto -sv             # Run with default settings (parallel)
uv run pytest -sv -k "keyword"        # Run tests by keyword
uv run pytest -m "postgres"           # Run tests by marker

# Legacy pytest (fallback)
pytest -n auto -sv                    # Run with default settings (parallel)
pytest -sv -k "keyword"               # Run tests by keyword
pytest -m "postgres"                  # Run tests by marker
pytest                                # Run all tests without parallelization (not recommended)
```

Pytest supports `and` & `or` operators too. Something like `pytest -m "one and two"` will work.

**⚠️ Important: Graceful Test Interruption**

The test suite now handles **Ctrl+C** gracefully! When you interrupt tests with Ctrl+C:
- ✅ **All resources are cleaned up** (databases, containers, cloud resources)
- ✅ **No orphaned resources** are left behind
- ✅ **Fixture teardown runs** automatically
- ✅ **Temp files are removed**

You can safely interrupt long-running tests without worrying about cleanup! DO NOT SPAM CONTROL C though

7. Configure your tools and permissions:

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

8. A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the necessary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind.



Notes:

-Tigerbeetle must be set up with VOPR for testing.
-Mongo must have permisions to create and drop collections and databases
-Airflow must be set up with git sync enabled to the repo you provide
-make sure your mySQL password and username are up to date. AWS sets defaults to rotate once a week...
-Postgres must have the postgres db in it to function (i mean u shouldn't have deleted this anyway)

## Common Errors

### **Astronomer Token Expired**
```
subprocess.CalledProcessError: Command '['astro', 'login', '--token-login', 'eyJhbGciOiJSUzI1NiIs...']' returned non-zero exit status 1.
```
**Solution:** Your `ASTRO_ACCESS_TOKEN` has expired. Generate a new token from your Astronomer account and update your `.env` file.


### **Database Connection Errors**
```
psycopg2.OperationalError: could not connect to server
mysql.connector.errors.DatabaseError: Can't connect to MySQL server
```
**Solution:** Check your database credentials in the `.env` file. For AWS RDS, credentials may rotate weekly - update them as needed.

## Dependency Management Migration

This project has been migrated from pip + requirements.txt to UV for faster and more reliable dependency management:

- **UV Benefits**: 10-100x faster installation, universal lockfile, better dependency resolution
- **New Files**: `pyproject.toml` (project config), `uv.lock` (lockfile), `.venv/` (virtual environment)
- **Backward Compatibility**: The `requirements.txt` file is maintained for legacy systems and fixture compatibility
- **Docker**: Updated to use UV for container builds