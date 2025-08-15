# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve. It was designed to test Ardent's Agents

There is a README within each test folder to explain the problem and the tests

## Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd DE-Bench
   ```

2. **Set up environment variables:**
   Create a `.env` file in the root directory (see Environment Variables Template below)

3. **Build and run with Docker Compose:**
   ```bash
   # Build the container
   docker-compose build

   # Start the container (interactive mode)
   docker-compose run --rm de-bench bash

   # Or run tests directly
   docker-compose run --rm de-bench pytest -sv
   ```

4. **Docker Features:**
   - **Automatic Network Translation**: The container automatically converts `localhost` and `127.0.0.1` URLs in your environment variables to `host.docker.internal` for proper Docker networking
   - **Isolated Environment**: All dependencies (PostgreSQL client, Git, Astro CLI) are pre-installed
   - **Volume Mounting**: Your local code changes are reflected immediately in the container
   - **Host Access**: Can connect to services running on your host machine (databases, Airflow, etc.)


## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials. If there is an actual value there already do not change it:

**Note for Docker users:** 
- You can use `localhost` or `127.0.0.1` in your environment variables - the Docker container will automatically convert these to `host.docker.internal` for proper networking.
- **Important**: If you update any environment variables in your `.env` file, you need to restart the Docker container/compose for the changes to take effect.

<pre><code>

# AWS
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"           # AWS access key ID
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"   # AWS secret access key

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"         # MongoDB connection string

# Ardent AI
ARDENT_PUBLIC_KEY="YOUR_ARDENT_PUBLIC_KEY"           # Ardent AI public key
ARDENT_SECRET_KEY="YOUR_ARDENT_SECRET_KEY"           # Ardent AI secret key
ARDENT_BASE_URL="http://localhost:8000"              # Ardent AI base URL

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"                         # MySQL host address
MYSQL_PORT=3306                                      # MySQL port (typically 3306)
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"                 # MySQL username
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"                 # MySQL password

# Supabase
SUPABASE_PROJECT_URL="YOUR_SUPABASE_PROJECT_URL"     # Supabase project URL
SUPABASE_API_KEY="YOUR_SUPABASE_API_KEY"             # Supabase API key
SUPABASE_URL="YOUR_SUPABASE_URL"                     # Supabase URL
SUPABASE_SERVICE_ROLE_KEY="YOUR_SUPABASE_SERVICE_ROLE_KEY"  # Supabase service role key
SUPABASE_JWT_SECRET="YOUR_SUPABASE_JWT_SECRET"       # Supabase JWT secret

# PostgreSQL
POSTGRES_HOSTNAME="YOUR_POSTGRES_HOSTNAME"           # PostgreSQL hostname
POSTGRES_PORT=5432                                   # PostgreSQL port (typically 5432)
POSTGRES_USERNAME="YOUR_POSTGRES_USERNAME"           # PostgreSQL username
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"           # PostgreSQL password

# Snowflake
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT"           # Snowflake account identifier
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USER"                 # Snowflake username
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"         # Snowflake password
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"                     # Snowflake warehouse name

# Azure SQL
AZURE_SQL_SERVER="YOUR_AZURE_SQL_SERVER"             # Azure SQL server
AZURE_SQL_USERNAME="YOUR_AZURE_SQL_USERNAME"         # Azure SQL username
AZURE_SQL_PASSWORD="YOUR_AZURE_SQL_PASSWORD"         # Azure SQL password
AZURE_SQL_VERSION=18                                 # Azure SQL version

# Airflow
AIRFLOW_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"             # GitHub token with full repo access
AIRFLOW_REPO="YOUR_AIRFLOW_REPO_URL"                 # Repo URL (e.g., https://github.com/YOUR_ORG/YOUR_REPO)
AIRFLOW_DAG_PATH="dags/"                             # Path to the DAG folder
AIRFLOW_REQUIREMENTS_PATH="Requirements/"            # Path to requirements folder
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
DATABRICKS_HOST="YOUR_DATABRICKS_HOST"               # Databricks workspace host
DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"             # Databricks access token
DATABRICKS_CLUSTER_ID="YOUR_DATABRICKS_CLUSTER_ID"   # Databricks cluster ID
DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"     # Databricks HTTP path
DATABRICKS_JOBS_WORKSPACE_URL="YOUR_DATABRICKS_WORKSPACE_URL"  # Databricks jobs workspace URL
DATABRICKS_JOBS_ACCESS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"    # Databricks jobs access token
DATABRICKS_JOBS_GITHUB_TOKEN="YOUR_DATABRICKS_GITHUB_TOKEN"    # GitHub token for Databricks
DATABRICKS_JOBS_REPO="YOUR_DATABRICKS_REPO_URL"      # Databricks repository URL
DATABRICKS_JOBS_REPO_PATH="/Repos/YOUR_EMAIL/"       # Databricks repository path

# Finch
FINCH_ACCESS_TOKEN="YOUR_FINCH_ACCESS_TOKEN"         # Finch access token

# Astro (Astronomer)
ASTRO_WORKSPACE_ID="YOUR_ASTRO_WORKSPACE_ID"         # Astro workspace ID
ASTRO_ACCESS_TOKEN="YOUR_ASTRO_ACCESS_TOKEN"         # Astro access token (expires after 1 hour)
ASTRO_CLOUD_PROVIDER="aws"                           # Astro cloud provider
ASTRO_REGION="us-east-1"                             # Astro region

# Benchmark
BENCHMARK_ROOT="FULL_PATH_TO_BENCHMARK_FOLDER"       # Full path of the folder you clone the repo into
MODEL_PATH="PATH_TO_YOUR_MODEL"                      # Path to your model
</code></pre>

5. **Edit the Run_Model.py file** to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model

## Running Tests

```bash
# Interactive shell
docker-compose run --rm de-bench bash

# Inside the container, run tests:
pytest -n auto -sv                    # Run with default settings
pytest -sv -k "keyword"               # Run tests by keyword  
pytest -m "one and two"               # Run tests with markers

# Or run directly without shell:
docker-compose run --rm de-bench pytest -sv
```

Pytest supports `and` & `or` operators too. Something like `pytest -m "one and two"` will work.

## Tool Configuration & Permissions:

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

## Test Architecture & Features

### SQL Schema Files
Many database tests now use dedicated SQL schema files for better organization and maintainability:
- **MySQL Tests**: Use `mysql_schema.sql` files for table definitions and initial data
- **PostgreSQL Tests**: Use `schema.sql` files for database setup
- **Benefits**: Version-controlled schemas, easier debugging, and cleaner test separation

### Enhanced MySQL Support
The MySQL fixture system has been significantly improved:
- **SQL File Integration**: Tests can now use dedicated `mysql_schema.sql` files for setup
- **Automatic Database Management**: Creates and drops test databases automatically
- **Connection Pooling**: Efficient connection management for parallel test execution
- **Data Isolation**: Each test gets a clean database state
- **Error Handling**: Robust error handling and cleanup on test failures

### Fixture System
The test suite uses a comprehensive fixture system that automatically:
- Creates and tears down database resources
- Manages test isolation between parallel runs
- Handles connection pooling and cleanup
- Supports both template-based and SQL file-based setup

### Docker Networking Features
The Docker setup includes intelligent networking features:
- **Automatic URL Translation**: Environment variables containing `localhost` or `127.0.0.1` are automatically converted to `host.docker.internal`
- **Host Gateway Access**: Container can access services running on your host machine
- **Persistent Environment**: Converted URLs are saved to `.docker-env` for consistency across container sessions
- **No Manual Configuration**: Works out-of-the-box without modifying your existing environment variables

## Important Notes

A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the necessary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind.

### Requirements & Setup Notes:

- **Tigerbeetle**: Must be set up with VOPR for testing
- **MongoDB**: Must have permissions to create and drop collections and databases
- **Airflow**: Must be set up with git sync enabled to the repo you provide
- **MySQL**: Make sure your MySQL password and username are up to date. AWS sets defaults to rotate once a week
- **PostgreSQL**: Must have the `postgres` database available to function properly
- **Docker Networking**: When using Docker, localhost URLs are automatically converted to `host.docker.internal`
- **Astro Tokens**: Astro access tokens expire after 1 hour, so you'll need to generate a new token every hour for Airflow tests
- **Environment Changes**: If you update any environment variables, restart the Docker container for changes to take effect
