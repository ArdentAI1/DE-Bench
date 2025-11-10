# Example: hello-world-failure-state.zip

This document shows the exact contents of the `hello-world-failure-state.zip` archive, serving as a reference for creating similar state archives.

## Archive Purpose

This state archive contains a broken Airflow DAG that AI agents must debug and fix. The test validates that an AI can:
1. Identify the failing task in the DAG
2. Understand the cause of the failure
3. Fix the broken code
4. Submit a working PR

## Complete File Structure

```
hello-world-failure-state.zip
├── .git/                           # Complete git repository (326 files)
│   ├── objects/                    # Git objects and pack files
│   │   ├── pack/                   # Packed git objects
│   │   └── [hash-based-dirs]/      # Loose objects
│   ├── refs/
│   │   ├── heads/
│   │   │   ├── main
│   │   │   └── fix/using-astro-api-token-instead-of-astro-login
│   │   ├── remotes/origin/
│   │   │   ├── main
│   │   │   ├── HEAD
│   │   │   └── feature/ (multiple test branches)
│   │   └── tags/
│   ├── logs/                       # Git operation logs
│   ├── hooks/                      # Git hook samples
│   ├── config                      # Git configuration
│   ├── HEAD                        # Points to refs/heads/main
│   ├── index                       # Staging area
│   └── ...
├── .github/                        # GitHub metadata (empty in this case)
├── dags/
│   ├── .gitkeep                    # Preserves empty directory in git
│   └── hello_world.py              # DAG with intentional failure ⚠️
├── Requirements/
│   └── requirements.txt            # Empty Python requirements
├── airflow_init.sh                 # Airflow initialization script
├── Dockerfile                      # Container configuration
└── requirements.txt                # Root requirements (minimal: "\n")
```

## Key File Contents

### dags/hello_world.py (The Broken DAG)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "hello_world_dag",
    default_args=default_args,
    description="A simple Hello World DAG",
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "hello_world"],
)

# Create the tasks
print_hello = BashOperator(
    task_id="print_hello",
    bash_command='echo "Hello World"',
    dag=dag
)

# ⚠️ INTENTIONAL FAILURE: This task will always fail
fail_task = BashOperator(
    task_id="intentional_failure",
    bash_command="cat /nonexistent/file.txt",  # File doesn't exist!
    dag=dag
)

# Task dependencies - hello succeeds, then failure happens
print_hello >> fail_task
```

**What's Wrong:**
- `fail_task` attempts to read `/nonexistent/file.txt` which doesn't exist
- This causes the DAG to fail after the first task succeeds
- AI must identify and remove/fix this task

### Dockerfile

```dockerfile
# Use the official Airflow image
FROM apache/airflow:2.7.0

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
USER root
RUN if [ -s /requirements.txt ]; then pip install --no-cache-dir -r /requirements.txt; fi

# Copy DAGs
COPY dags /opt/airflow/dags

# Copy initialization script
COPY airflow_init.sh /opt/airflow/airflow_init.sh
RUN chmod +x /opt/airflow/airflow_init.sh

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow
```

### airflow_init.sh

```bash
#!/bin/bash
set -e

echo "Initializing Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting Airflow webserver..."
airflow webserver &

echo "Starting Airflow scheduler..."
airflow scheduler
```

### requirements.txt (Root)

```
(empty - just a newline character)
```

### Requirements/requirements.txt

```
(completely empty file)
```

## Git History

The archive includes git history with commits like:

```
commit ace8ad2a9273322aedfe70c9fb7e235f762a8dcc
Author: ...
Date:   ...

    Improved error handling and cleanup process in AirflowFixture.

commit 98f7efbb1d5ac1203b5b4ad19527dd2716db0b4a
Author: ...
Date:   ...

    Increased the maximum wait time for retrieving the external IP

commit d61c385d0e97c42bec829dc5fd33370a4b0a0cba
Author: ...
Date:   ...

    Enhanced error handling in GitHub resource management
```

## Test Usage

This archive is used in the test:
```
Tests/Airflow_Agent_Failed_Hello_World/test_airflow_agent_failed_hello_world_pipeline.py
```

### Test Configuration

```python
# In get_fixtures() function:
custom_github_config = {
    "resource_id": f"test_airflow_{resource_id}",
    "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/hello-world-failure-state.zip",
}

github_fixture = GitHubFixture(custom_config=custom_github_config)
```

### Expected AI Behavior

1. **Analyze the repository** - AI checks out the test branch with the broken DAG
2. **Identify the problem** - AI finds the `intentional_failure` task that tries to read nonexistent file
3. **Fix the code** - AI removes or fixes the failing task
4. **Test locally** (optional) - AI may verify the fix
5. **Create PR** - AI creates a pull request with the fix
6. **Merge** - Test framework merges the PR
7. **Validate** - Test verifies the DAG now runs successfully

### Validation Steps

The test validates:
- ✅ Branch was created with the state files
- ✅ AI created a feature branch
- ✅ AI created and merged a PR
- ✅ GitHub Actions completed successfully
- ✅ Airflow redeployed with the fixed DAG
- ✅ DAG exists in Airflow
- ✅ DAG runs successfully without errors
- ✅ Output contains expected "Hello World" message

## Creating Similar Archives

To create a similar failure state archive:

1. **Clone repository:**
   ```bash
   git clone <repo-url> my-failure-state
   cd my-failure-state
   ```

2. **Add broken code:**
   ```bash
   # Create or modify files with intentional errors
   vim dags/my_dag.py
   ```

3. **Commit changes:**
   ```bash
   git add .
   git commit -m "Add intentionally broken DAG for testing"
   ```

4. **Create archive:**
   ```bash
   zip -r my-failure-state.zip . -x "*.pyc" -x "__pycache__/*" -x ".DS_Store"
   ```

5. **Verify contents:**
   ```bash
   unzip -l my-failure-state.zip | head -20
   ```

6. **Test extraction:**
   ```bash
   mkdir test && cd test
   unzip ../my-failure-state.zip
   git log --oneline  # Should show commits
   ls -la dags/       # Should show DAG files
   ```

7. **Move to repository:**
   ```bash
   mv my-failure-state.zip /path/to/DE-Bench/Fixtures/Airflow/GitHub_States/
   ```

## Archive Size

```
hello-world-failure-state.zip: 699,963 bytes (~684 KB)
- 326 files total
- Majority is .git directory with history
- Could be reduced with: git gc --aggressive --prune=all
```

## Important Notes

1. **Git Directory Included**: The `.git/` directory IS in the zip but will NOT be pushed to GitHub as regular files
2. **GitHub Directory Handling**: `.github/` is also skipped during push (GitHub manages it)
3. **Intentional Failure**: The failure is clearly documented in comments for AI understanding
4. **Minimal Dependencies**: Empty requirements allow focus on the DAG logic
5. **Complete History**: Full git history helps AI understand repository context

## Troubleshooting This Archive

If tests using this archive fail:

**Check archive integrity:**
```bash
unzip -t hello-world-failure-state.zip
```

**Verify DAG content:**
```bash
unzip -p hello-world-failure-state.zip dags/hello_world.py | grep intentional_failure
```

**Check git history:**
```bash
mkdir temp && cd temp
unzip ../hello-world-failure-state.zip
git log --oneline -5
```

**Validate structure:**
```bash
unzip -l hello-world-failure-state.zip | grep -E "(^.git|dags/|Dockerfile)"
```
