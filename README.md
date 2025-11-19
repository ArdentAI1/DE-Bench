# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This repository contains real-world data engineering problems for AI agents to solve, designed to evaluate agent capabilities across various data engineering tasks including databases, data pipelines, and workflow orchestration.

## 📖 **For Test Development**

**See [Tests/TESTS.md](Tests/TESTS.md)** for comprehensive documentation on:
- **Test pattern and structure** 
- **Writing new tests**
- **Fixture system usage**
- **Validation patterns**
- **Best practices**

## 🚀 **Quick Start**

### 1. Clone and Setup

```bash
git clone <repo-url>
cd DE-Bench
```

### 2. Environment Variables

Set up your environment variables to provide credentials and configuration for various services:

## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials. If there is an actual value there already do not change it:

<pre><code>
# AWS Credentials
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"

# AWS S3 Credentials (for Snowflake S3 integration)
AWS_ACCESS_KEY="YOUR_AWS_ACCESS_KEY"
AWS_SECRET_KEY="YOUR_AWS_SECRET_KEY"

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

# DE-Bench Database (Supabase instance for distributed locking and coordination)
DE_BENCH_DB_URL="http://127.0.0.1:54321"
DE_BENCH_DB_SERVICE_KEY="YOUR_LOCAL_SUPABASE_SERVICE_ROLE_KEY"

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
SNOWFLAKE_ROLE="SYSADMIN"

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

# Airflow Provider Selection (Optional)
# Options: "modal" (default), "aks", "ecs", "astro"
AIRFLOW_PROVIDER="modal"

# Modal Configuration (if using Modal for Airflow)
# Note: Also requires one-time: modal token set --token-id <ID> --token-secret <SECRET>
MODAL_WORKSPACE="ardent"  # Your Modal workspace name

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
ASTRO_API_TOKEN="YOUR_ASTRO_API_TOKEN"
ASTRO_CLOUD_PROVIDER="aws"
ASTRO_REGION="us-east-1"

# Azure Configuration (for Claude_Code mode and container services)
# Azure Service Principal (for AKS access)
AZURE_CLIENT_ID="YOUR_AZURE_SERVICE_PRINCIPAL_CLIENT_ID"
AZURE_CLIENT_SECRET="YOUR_AZURE_SERVICE_PRINCIPAL_CLIENT_SECRET"
AZURE_TENANT_ID="YOUR_AZURE_TENANT_ID"
AZURE_SUBSCRIPTION_ID="YOUR_AZURE_SUBSCRIPTION_ID"

# Azure Container Services
AZURE_ACR_NAME="YOUR_ACR_NAME"

# Azure Kubernetes Service
AKS_CLUSTER_NAME="YOUR_AKS_CLUSTER_NAME"
AKS_RESOURCE_GROUP="YOUR_AKS_RESOURCE_GROUP"

# Azure Storage Account
AZURE_STORAGE_ACCOUNT_NAME="YOUR_STORAGE_ACCOUNT_NAME"
AZURE_STORAGE_ACCOUNT_KEY="YOUR_STORAGE_ACCOUNT_KEY"

# Azure Key Vault
AZURE_KEY_VAULT_NAME="YOUR_KEY_VAULT_NAME"

# AWS Credentials for Claude Code (Bedrock access)
AWS_ACCESS_KEY_ID_CLAUDE="YOUR_AWS_ACCESS_KEY_FOR_CLAUDE_BEDROCK"
AWS_SECRET_ACCESS_KEY_CLAUDE="YOUR_AWS_SECRET_KEY_FOR_CLAUDE_BEDROCK"
AWS_DEFAULT_REGION_CLAUDE="us-east-1"

# Claude Code Configuration
IS_SANDBOX=1

# OpenAI Configuration (for OpenAI_Codex mode)
OPENAI_API_KEY="YOUR_OPENAI_API_KEY"
AZURE_OPENAI_API_KEY="YOUR_AZURE_OPENAI_API_KEY"
AZURE_OPENAI_ENDPOINT="YOUR_AZURE_OPENAI_ENDPOINT"
AZURE_OPENAI_API_VERSION="2023-12-01-preview"
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME="YOUR_DEPLOYMENT_NAME"
</code></pre>

### Custom Variables for Your Setup

Below are custom variables for your specific setup. We set up the Ardent configs as an example:

<pre><code>
# Ardent AI Configuration (Example Custom Setup)

# Option 1: Use environment API key directly (simpler setup)
ARDENT_API_KEY="YOUR_ARDENT_API_KEY"  # Single bearer token (e.g., sk-ard_test_xxxxx)
ARDENT_BASE_URL="http://localhost:8000"

# Option 2: Use Supabase to create dynamic test users/API keys (advanced)
# If both SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are set, 
# tests will create temporary users and API keys automatically.
# SUPABASE_URL="https://your-project.supabase.co"
# SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"

# Note: For Ardent mode, you must provide EITHER:
#   - ARDENT_API_KEY (direct API key), OR
#   - SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (dynamic user creation)
</code></pre>

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model




4. Set up and run the Docker Compose environment:

```bash
# Copy the environment template
cp env.example .env

# Edit the .env file with your actual credentials
# Replace TODO_REQUIRED values with your credentials
# TODO_OPTIONAL values can be left empty if you don't need those services
```

The template includes:
- **Core Framework Variables**: Required for all tests (Supabase, Ardent)
- **Execution Mode Configuration**: Choose between Ardent, Claude_Code, or OpenAI_Codex modes
- **Database Services**: MongoDB, MySQL, PostgreSQL, Snowflake, Azure SQL
- **Workflow Orchestration**: Airflow, Databricks, Astronomer
- **Cloud Services**: AWS, Azure configurations
- **Third-party APIs**: Finch and other service integrations

Each section is clearly documented with:
- Purpose and usage explanations
- Required vs optional variables
- Default values where applicable
- Example formats for complex values

### 3. Setup Local Supabase (for DE-Bench Database)

DE-Bench uses a local Supabase instance for distributed locking and coordination between test runners:

```bash
# Install Supabase CLI (if not already installed)
npm install -g supabase

# Start local Supabase instance
npx supabase start

# This will output your local credentials, including:
# - API URL: http://127.0.0.1:43210
# - Service Role Key: [copy this to DE_BENCH_DB_SERVICE_KEY in your .env]
```

**Important**: Copy the `service_role key` from the output and set it as `DE_BENCH_DB_SERVICE_KEY` in your `.env` file. The `DE_BENCH_DB_URL` should be set to `http://127.0.0.1:43210` (the default local API URL).

The distributed locking mechanism will be automatically initialized when you run tests.

### 4. Install Dependencies

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable dependency management.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies and create virtual environment
uv sync
```

**Note**: `uv sync` will automatically create a `.venv` virtual environment and install all dependencies from `pyproject.toml` and `uv.lock`. You can run commands using `uv run` which automatically uses the project's virtual environment without manual activation.

### 5. Run Tests

The framework uses Braintrust for evaluation. Run tests using the evaluation script:

```bash
# Run all tests
uv run python run_braintrust_eval.py Ardent

# Run specific test
uv run python run_braintrust_eval.py --filter "MongoDB_Agent_Add_Record" Ardent

# Run tests by category
uv run python run_braintrust_eval.py --filter "PostgreSQL_Agent.*" Ardent
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent

# Run multiple test patterns
uv run python run_braintrust_eval.py --filter "MongoDB.*" "MySQL.*" Ardent

# Run with different AI modes
uv run python run_braintrust_eval.py --filter "MongoDB_Agent_Add_Record" Claude_Code
uv run python run_braintrust_eval.py --filter "MongoDB_Agent_Add_Record" OpenAI_Codex

# Test infrastructure only (skip model execution)
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" --skip-model-run Ardent

```

### Available Modes:
- **Ardent** (Default) - Uses Ardent AI's backend service
- **Claude_Code** - Uses Claude Code via AWS Bedrock in Kubernetes containers
- **OpenAI_Codex** - Uses OpenAI Codex via OpenAI API in Kubernetes containers

### Filter Examples:
```bash
# Database tests
uv run python run_braintrust_eval.py --filter ".*Agent_Add_Record" Ardent

# Airflow pipeline tests  
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent

# Specific database types
uv run python run_braintrust_eval.py --filter "PostgreSQL.*" Ardent
uv run python run_braintrust_eval.py --filter "Snowflake.*" Ardent
uv run python run_braintrust_eval.py --filter "MongoDB.*" Ardent

```

### 6. Airflow Provider Configuration

Airflow tests support multiple deployment providers for flexibility and performance:

#### Available Providers:
- **Modal** (Default) - Serverless deployment, fastest performance (~55s ready time)
- **AKS** - Azure Kubernetes Service 
- **ECS** - AWS ECS Fargate
- **Astro** - Astronomer Cloud

#### Using Modal (Recommended):
Modal is the default provider and offers the fastest deployment times:

```bash
# Modal is used by default (no configuration needed)
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent

# Or explicitly set Modal
export AIRFLOW_PROVIDER=modal
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent
```

**Modal Setup:**

**Get your tokens from:** https://modal.com/settings → Tokens → Create new token

1. **Authenticate Modal CLI** (one-time setup per machine):
   ```bash
   # Install dependencies (Modal CLI included)
   uv sync
   
   # Authenticate with Modal
   modal token set \
     --token-id "ak-xxxxx" \
     --token-secret "as-xxxxx"
   ```

2. **Set environment variables** (add to your shell profile or `.env`):
   ```bash
   export MODAL_WORKSPACE="ardent"  # Your Modal workspace name (check: modal profile list)
   ```

3. **Create GCP secret** for private registry access (one-time setup):
   ```bash
   modal secret create gcp-registry-secret \
     SERVICE_ACCOUNT_JSON="$(cat gcp.json)"
   ```

4. Tests will automatically use Modal for Airflow deployments

**Note:** Modal CLI stores authentication in `~/.modal/config`. The `modal token set` command only needs to be run once per machine.

**Modal Benefits:**
- ⚡ **Fast deployment**: ~1.5s infrastructure + ~54s Airflow init
- 💰 **Cost-effective**: Only pay when running tests
- 🔄 **Auto-scaling**: Automatic resource management
- 🧹 **Auto-cleanup**: Resources automatically torn down after tests

#### Using Other Providers:

```bash
# Use Azure Kubernetes Service
export AIRFLOW_PROVIDER=aks
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent

# Use AWS ECS Fargate
export AIRFLOW_PROVIDER=ecs
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent

# Use Astronomer Cloud
export AIRFLOW_PROVIDER=astro
uv run python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent
```

**Note**: AKS and ECS require additional cloud infrastructure setup. See provider-specific documentation for details.

### 7. Service Configuration

Configure your tools and permissions:

**DE-Bench Database (Local Supabase)**:
- Purpose: Distributed locking and coordination between test runners
- Setup: Run `npx supabase start` to initialize local instance
- Automatic: Database schema and functions are auto-created on first use
- No manual configuration required beyond environment variables

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

Snowflake:
- Required Role: SYSADMIN (or custom role with database creation permissions)
- Required Permissions:
  - CREATE DATABASE
  - CREATE SCHEMA
  - CREATE TABLE
  - COPY INTO (for S3 loading)
- AWS S3 Access: Ensure AWS credentials have S3 read permissions for parquet files

## 📝 **Important Notes**

**Cost Awareness**: Many tests use cloud services that may incur charges. Monitor your usage across:
- Database services (MongoDB Atlas, AWS RDS, Snowflake)
- Airflow/Astronomer deployments 
- AWS S3 and other cloud resources

**Service-Specific Requirements**:
- **DE-Bench Database**: Local Supabase instance must be running (`npx supabase start`)
- **MongoDB**: Must have permissions to create and drop collections and databases
- **Airflow**: 
  - **Modal** (Default): Requires one-time CLI authentication (`modal token set`), `MODAL_WORKSPACE` env var, and GCP secret setup
  - **AKS/ECS/Astro**: Must be set up with git sync enabled to your repository
- **MySQL**: Check credentials regularly (AWS RDS defaults rotate weekly)
- **PostgreSQL**: Must have the default `postgres` database available
- **Tigerbeetle**: Must be set up with VOPR for testing (if used)

**AI Mode Requirements**:
- **Claude_Code**: Requires AWS Bedrock access and Azure Kubernetes Service setup
- **OpenAI_Codex**: Requires valid OpenAI API key and Azure Kubernetes Service setup

## 🔍 **Test Discovery & Debugging**

### Viewing Available Tests
```bash
# See all available tests
uv run python run_braintrust_eval.py --help

# Tests are automatically discovered from Tests/ directory
# Each test must follow the standard pattern (see Tests/TESTS.md)
```

### Debugging Failed Tests
```bash
# Use verbose mode for detailed error information
uv run python run_braintrust_eval.py --filter "Test_Name" --verbose Ardent

# Check Braintrust dashboard for detailed execution logs
# URL will be provided in the output
```

### Test Development
- See **[Tests/TESTS.md](Tests/TESTS.md)** for the complete development guide
- All tests use the unified `DEBenchFixture` pattern
- Resources are automatically set up and cleaned up
- Validation includes detailed test steps for debugging

## ⚠️ **Common Errors**

### **DE-Bench Database Connection Errors**
```
ValueError: Missing required environment variables: DE_BENCH_DB_URL and DE_BENCH_DB_SERVICE_KEY
supabase._sync.client.SupabaseException: Invalid API key
```
**Solution:** 
1. Start local Supabase: `npx supabase start`
2. Copy the `service_role key` from the output to `DE_BENCH_DB_SERVICE_KEY` in your `.env`
3. Set `DE_BENCH_DB_URL=http://127.0.0.1:54321` in your `.env`

### **Astronomer Token Expired**
```
subprocess.CalledProcessError: Command '['astro', 'login', '--token-login', 'eyJhbGciOiJSUzI1NiIs...']' returned non-zero exit status 1.
```
**Solution:** Your `ASTRO_API_TOKEN` has expired or is invalid. Generate a new API token from your Astronomer account (see https://www.astronomer.io/docs/astro/automation-authentication) and update your `.env` file.

### **Database Connection Errors**
```
psycopg2.OperationalError: could not connect to server
mysql.connector.errors.DatabaseError: Can't connect to MySQL server
```
**Solution:** Check your database credentials in the `.env` file. For AWS RDS, credentials may rotate weekly - update them as needed.

### **Test Discovery Issues**
```
❌ Test 'Test_Name' does not match pattern: missing 'get_fixtures'
```
**Solution:** Test doesn't follow the new pattern. See [Tests/TESTS.md](Tests/TESTS.md) for conversion guide. Tests must have `get_fixtures()`, `create_model_inputs()`, and `validate_test()` functions.

### **Resource Setup Failures**
```
Exception: MongoDB resource data not available - ensure test_setup was called
```
**Solution:** Fixture setup failed. Check environment variables for the specific service and ensure credentials are correct.

### **Configuration Validation Errors**
```
ardent.exceptions.ArdentValidationError: Invalid type at user. Expected str, got NoneType
```
**Solution:** Environment variable is missing or None. Check your `.env` file for the required service credentials.

---

## 🎯 **Framework Benefits**

This new DE-Bench framework provides:

- **🔄 Unified Testing**: All tests follow the same pattern for consistency
- **🛡️ Robust Resource Management**: Automatic setup and cleanup of databases, services
- **📊 Detailed Validation**: Test steps provide granular pass/fail information  
- **⚡ Parallel Execution**: Tests run efficiently with proper resource isolation
- **🔍 Easy Debugging**: Clear error messages and Braintrust integration
- **📚 Comprehensive Documentation**: Complete guides in [Tests/TESTS.md](Tests/TESTS.md)

For detailed test development, patterns, and examples, see **[Tests/TESTS.md](Tests/TESTS.md)**.
