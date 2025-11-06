# Airflow Resource Fixture

The `airflow_resource` fixture provides isolated Airflow instances for each test using Astronomer Cloud with intelligent deployment caching and hibernation management.

## Overview

The fixture creates and manages Airflow deployments in Astronomer Cloud for each test function, with sophisticated caching to reuse hibernating deployments across tests. It provides a fully managed Airflow environment with automatic cleanup and GitHub integration.

## Features

- **Function-scoped**: Each test reuses a hibernating deployment or will create a new one if all deployments aren't hibernating
- **Cloud-based**: Uses Astronomer Cloud for managed Airflow instances
- **Intelligent Caching**: Reuses hibernating deployments to reduce setup time and costs
- **Deployment Hibernation**: Automatically hibernates deployments after tests for cost optimization
- **Automatic cleanup**: Resources are automatically cleaned up and returned to cache after each test
- **GitHub integration**: Automatically syncs with GitHub repositories for DAG deployment
- **API token authentication**: Uses Bearer token authentication for API access with support for both long-lived and short-lived tokens
- **Parallel execution support**: File-based coordination prevents conflicts in parallel test execution
- **Session-scoped login**: Astro CLI login is managed once per test session

## Prerequisites

### Required Environment Variables

The fixture requires the following environment variables to be set:

```bash
# Astronomer Cloud credentials (REQUIRED)
ASTRO_WORKSPACE_ID=your_workspace_id
ASTRO_CLOUD_PROVIDER=aws  # or gcp, azure
ASTRO_REGION=us-east-1
ASTRO_RUNTIME_VERSION=13.1.0  # Optional, defaults to 13.1.0

# Astronomer authentication (REQUIRED)
ASTRO_API_TOKEN=your_astro_api_token  # see https://www.astronomer.io/docs/astro/automation-authentication for more information and how to create one

# GitHub integration (REQUIRED)
AIRFLOW_GITHUB_TOKEN=your_github_token
AIRFLOW_REPO=https://github.com/your-org/your-repo

# Airflow user credentials (OPTIONAL, defaults to "airflow")
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
```

#### Token Authentication

The fixture requires `ASTRO_API_TOKEN` for authentication. This is a long-lived token with configurable expiry that you can generate from your Astronomer account.

### Required Tools

- **Astro CLI**: Must be installed and in PATH
- **GitHub access**: Repository must be accessible with the provided token

## Usage

### Basic Usage

```python
import pytest

@pytest.mark.airflow
def test_my_airflow_test(airflow_resource):
    # Access the Airflow instance
    airflow_instance = airflow_resource["airflow_instance"]
    
    # Access connection details
    base_url = airflow_resource["base_url"]
    api_token = airflow_resource["api_token"]
    username = airflow_resource["username"]
    password = airflow_resource["password"]
    
    # Your test logic here
    # The Airflow deployment is already created and ready to use
```

### Resource Data Structure

The `airflow_resource` fixture returns a dictionary with the following structure:

```python
{
    "resource_id": "unique_test_resource_id",
    "type": "airflow_resource",
    "test_name": "test_function_name",
    "creation_time": timestamp,
    "worker_pid": process_id,
    "creation_duration": setup_time_in_seconds,
    "description": "An Airflow resource for unique_test_resource_id",
    "status": "active",
    "project_name": "airflow_test_unique_id_timestamp",
    "base_url": "https://deployment-name.region.astro.io",
    "deployment_id": "astro_deployment_id",
    "deployment_name": "deployment_name",
    "api_url": "https://deployment-name.region.astro.io/api/v1",
    "api_token": "astro_deployment_token",
    "api_headers": {
        "Authorization": "Bearer token",
        "Cache-Control": "no-cache"
    },
    "username": "airflow",
    "password": "airflow",
    "airflow_instance": Airflow_Local_instance,
    "created_resources": [("deployment_name", cache_manager)],
    "cache_manager": shared_cache_manager_instance
}
```

### Complete Example Test

```python
import pytest
import requests
import time

@pytest.mark.airflow
@pytest.mark.pipeline
def test_simple_airflow_pipeline(request, airflow_resource):
    """Example test showing complete Airflow workflow."""
    
    # Get connection details from fixture
    base_url = airflow_resource["base_url"]
    api_token = airflow_resource["api_token"]
    headers = {"Authorization": f"Bearer {api_token}", "Cache-Control": "no-cache"}
    
    # Wait for Airflow to be ready
    airflow_instance = airflow_resource["airflow_instance"]
    if not airflow_instance.wait_for_airflow_to_be_ready():
        raise Exception("Airflow instance did not deploy successfully.")
    
    # Example: Check if DAG exists
    dag_response = requests.get(
        f"{base_url.rstrip('/')}/api/v1/dags/hello_world_dag",
        headers=headers,
    )
    
    if dag_response.status_code == 200:
        # Unpause the DAG
        requests.patch(
            f"{base_url.rstrip('/')}/api/v1/dags/hello_world_dag",
            headers=headers,
            json={"is_paused": False},
        )
        
        # Trigger the DAG
        trigger_response = requests.post(
            f"{base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns",
            headers=headers,
            json={"conf": {}},
        )
        
        if trigger_response.status_code == 200:
            dag_run_id = trigger_response.json()["dag_run_id"]
            
            # Monitor DAG execution
            max_wait = 120  # 2 minutes timeout
            start_time = time.time()
            while time.time() - start_time < max_wait:
                status_response = requests.get(
                    f"{base_url.rstrip('/')}/api/v1/dags/hello_world_dag/dagRuns/{dag_run_id}",
                    headers=headers,
                )
                
                if status_response.status_code == 200:
                    state = status_response.json()["state"]
                    if state == "success":
                        print("DAG run completed successfully!")
                        break
                    elif state in ["failed", "error"]:
                        raise Exception(f"DAG failed with state: {state}")
                
                time.sleep(5)
            else:
                raise Exception("DAG run timed out")
```

## How It Works

### Session-Level Setup
1. **Astro Login**: Session-scoped fixture ensures Astro CLI is logged in once per test session
2. **Cache Manager Initialization**: Shared cache manager is initialized to track hibernating deployments
3. **Deployment Discovery**: Existing hibernating deployments are discovered and cached

### Per-Test Setup
1. **Deployment Allocation**: 
   - First tries to allocate an existing hibernating deployment from cache
   - If none available, creates a new deployment in Astronomer Cloud
   - Wakes up hibernating deployments as needed
2. **Local Project Setup**: Creates a unique temporary directory and initializes Astro project
3. **GitHub Integration**: Updates GitHub secrets with deployment information
4. **API Token Creation**: Creates or retrieves API token for the deployment
5. **User Creation**: Creates Airflow user with configured credentials
6. **Deployment Validation**: Waits for Airflow to be ready and validates deployment

### Test Execution
- Provides the Airflow deployment details to the test
- Each test gets its own isolated deployment or reuses a hibernating one
- GitHub integration automatically syncs DAGs from the repository via GitHub Actions

### Per-Test Cleanup
1. **Deployment Hibernation**: Hibernates the deployment to reduce costs
2. **Cache Management**: Returns deployment to cache for reuse by other tests
3. **Local Cleanup**: Removes temporary directories and files
4. **Resource Tracking**: Updates cache with deployment status and metadata

### Session-Level Cleanup
- Cache manager persists deployment information for future test sessions
- No session-level cleanup needed as deployments remain hibernated for reuse

## Cache Manager and Deployment Lifecycle

### SQLite-Based Cache Management

The fixture uses a sophisticated SQLite-based cache manager to optimize deployment usage:

- **Persistent Storage**: Deployment information is stored in `Environment/CacheManager/cluster_cache.db`
- **Hibernation Tracking**: Tracks which deployments are hibernating and available for reuse
- **Cross-Session Persistence**: Cache persists across test sessions for maximum efficiency
- **Atomic Operations**: SQLite transactions ensure data consistency in parallel execution

### Deployment States

Deployments can be in the following states:

- **HIBERNATING**: Deployment is hibernated and available for reuse
- **HEALTHY**: Deployment is active and running
- **UNKNOWN**: Deployment status could not be determined

### Cache Benefits

- **Cost Optimization**: Reuses hibernating deployments instead of creating new ones
- **Faster Setup**: Waking up a hibernating deployment is faster than creating a new one
- **Resource Efficiency**: Reduces Astronomer Cloud resource consumption
- **Parallel Safety**: File-based coordination prevents conflicts in parallel execution

## GitHub Integration

The fixture automatically:

- Updates GitHub repository secrets with deployment information
- Syncs DAGs from the configured GitHub repository
- Manages deployment lifecycle through GitHub Actions (if configured)

### Required GitHub Secrets

The fixture automatically creates/updates these secrets in your GitHub repository:

- `ASTRO_DEPLOYMENT_ID`: The deployment ID in Astronomer
- `ASTRO_DEPLOYMENT_NAME`: The deployment name
- `ASTRO_API_TOKEN`: The Astronomer API token

## Benefits

- **Cloud-based**: No local Docker setup required
- **Managed**: Astronomer handles infrastructure management
- **Cost-optimized**: Intelligent caching and hibernation reduce costs
- **Scalable**: Can run multiple tests concurrently with file-based coordination
- **Isolated**: Each test gets its own deployment or reuses a hibernating one
- **Fast setup**: Reuses hibernating deployments for faster test execution
- **GitHub integration**: Automatic DAG synchronization
- **Automatic cleanup**: Resources are cleaned up and returned to cache after tests
- **Parallel-safe**: File-based locking prevents conflicts in parallel execution

## Integration with Existing Tests

To update existing tests to use the fixture:

1. Add `airflow_resource` as a parameter to your test function
2. Replace local Airflow setup with the fixture-provided deployment
3. Use the provided API token for authentication instead of username/password
4. Update API calls to use the provided base URL and headers

### Migration Example

**Before (local setup):**
```python
def test_airflow_local():
    airflow = Airflow_Local(airflow_dir=Path("/tmp/airflow"))
    airflow.wait_for_airflow_to_be_ready()
    # Test logic...
```

**After (fixture):**
```python
@pytest.mark.airflow
def test_airflow_fixture(airflow_resource):
    base_url = airflow_resource["base_url"]
    api_token = airflow_resource["api_token"]
    headers = {"Authorization": f"Bearer {api_token}"}
    # Test logic using API calls...
```

## Troubleshooting

### Common Issues

- **Missing environment variables**: Ensure all required environment variables are set
- **Token expiry issues**: Ensure your `ASTRO_API_TOKEN` is valid and hasn't expired
- **Astro CLI not installed**: Install Astro CLI and ensure it's in PATH
- **GitHub access issues**: Verify GitHub token has repository access
- **Deployment creation failures**: Check Astronomer Cloud quotas and permissions
- **API token creation failures**: Check for rate limits and deployment ID mismatches
- **Cache database issues**: Ensure `Environment/CacheManager/` directory is writable
- **File lock conflicts**: File-based coordination may cause delays in parallel execution
- **Deployment hibernation failures**: Check Astronomer Cloud permissions for hibernation

### Debug Information

The fixture provides detailed logging including:
- Worker process ID for parallel execution tracking
- Creation timestamps and durations
- Deployment IDs and names
- Cache allocation and release information
- API URLs and tokens
- Hibernation and wake-up status
- Error messages with context and retry attempts

### Timeouts and Performance

- **Deployment creation**: ~3-5 minutes for new deployments
- **Deployment wake-up**: ~1-2 minutes for hibernating deployments
- **Airflow readiness**: ~1-2 minutes after deployment/wake-up
- **API token creation**: Up to 5 retries with progressive backoff
- **Cache operations**: Typically <1 second with SQLite
- **DAG execution**: Varies by DAG complexity

### Cache Management

- **Cache location**: `Environment/CacheManager/cluster_cache.db`
- **Cache persistence**: Survives across test sessions
- **Manual cache reset**: Delete the cache database file to start fresh
- **Cache monitoring**: Check cache database for deployment status and usage

### Token Management Best Practices

- **Use ASTRO_API_TOKEN**: The fixture requires `ASTRO_API_TOKEN` for authentication
- **Token configuration**: `ASTRO_API_TOKEN` allows you to configure custom expiry times for your specific needs
- **Security**: Keep your token secure and rotate it periodically according to your security policies

## Dependencies

- **Astro CLI**: For Astronomer Cloud management
- **PyGithub**: For GitHub repository management
- **pytest**: For fixture functionality
- **requests**: For HTTP communication with Airflow API
- **GitPython**: For repository operations
- **SQLite3**: For cache management (built into Python)
- **fcntl**: For file-based coordination (Unix/Linux)

## Airflow_Local Class Integration

The fixture provides an `Airflow_Local` instance that offers additional functionality:

### Key Methods

- `wait_for_airflow_to_be_ready()`: Waits for Airflow webserver to be ready
- `verify_airflow_dag_exists(dag_id)`: Verifies if a DAG exists in Airflow
- `unpause_and_trigger_airflow_dag(dag_id)`: Unpauses and triggers a DAG
- `verify_dag_id_ran(dag_id, dag_run_id)`: Verifies DAG execution completion
- `get_task_instance_logs(dag_id, dag_run_id, task_id)`: Retrieves task logs
- `check_dag_task_instances(dag_id, dag_run_id)`: Checks task instance status

### Usage in Tests

```python
@pytest.mark.airflow
def test_airflow_with_helper_methods(airflow_resource):
    airflow_instance = airflow_resource["airflow_instance"]
    
    # Wait for Airflow to be ready
    if not airflow_instance.wait_for_airflow_to_be_ready():
        raise Exception("Airflow not ready")
    
    # Verify DAG exists
    if not airflow_instance.verify_airflow_dag_exists("my_dag"):
        raise Exception("DAG not found")
    
    # Trigger and monitor DAG
    dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag("my_dag")
    airflow_instance.verify_dag_id_ran("my_dag", dag_run_id)
    
    # Get task logs
    logs = airflow_instance.get_task_instance_logs("my_dag", dag_run_id, "my_task")
    assert "expected_output" in logs
```

## Security Notes

- API tokens are automatically generated and cleaned up
- GitHub secrets are managed securely through the GitHub API
- All sensitive information is handled through environment variables
- Temporary files are cleaned up after tests complete
- File-based coordination uses secure locking mechanisms
- Cache database is stored locally and not transmitted 