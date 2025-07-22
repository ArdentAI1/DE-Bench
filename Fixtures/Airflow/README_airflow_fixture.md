# Airflow Resource Fixture

The `airflow_resource` fixture provides isolated Airflow instances for each test using Astronomer Cloud, ensuring complete isolation between tests.

## Overview

The fixture creates unique Airflow deployments in Astronomer Cloud for each test function, providing a fully managed Airflow environment with automatic cleanup.

## Features

- **Function-scoped**: Each test gets its own Airflow deployment
- **Cloud-based**: Uses Astronomer Cloud for managed Airflow instances
- **Unique deployments**: Each test runs in its own isolated environment
- **Automatic cleanup**: Resources are automatically cleaned up after each test
- **GitHub integration**: Automatically syncs with GitHub repositories for DAG deployment
- **Airflow API token authentication**: Uses Bearer token authentication for API access instead of Basic Authentication

## Prerequisites

### Required Environment Variables

The fixture requires the following environment variables to be set:

```bash
# Astronomer Cloud credentials
ASTRO_WORKSPACE_ID=your_workspace_id
ASTRO_ACCESS_TOKEN=your_astro_access_token
ASTRO_CLOUD_PROVIDER=aws  # or gcp, azure
ASTRO_REGION=us-east-1

# GitHub integration
AIRFLOW_GITHUB_TOKEN=your_github_token
AIRFLOW_REPO=https://github.com/your-org/your-repo

# Airflow user credentials if (optional, defaults to "airflow")
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
```

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
    "resource_id": "airflow_resource",
    "type": "airflow_resource",
    "test_name": "test_function_name",
    "creation_time": timestamp,
    "worker_pid": process_id,
    "creation_duration": setup_time_in_seconds,
    "description": "An Airflow resource for test_name",
    "status": "active",
    "project_name": "test_name_timestamp",
    "base_url": "https://deployment-name.region.astro.io",
    "api_url": "https://deployment-name.region.astro.io/api/v1",
    "api_token": "astro_deployment_token",
    "api_headers": {
        "Authorization": "Bearer token",
        "Cache-Control": "no-cache"
    },
    "username": "airflow",
    "password": "airflow",
    "airflow_instance": Airflow_Local_instance,
    "created_resources": ["deployment_id"]
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

1. **Setup Phase**:
   - Logs into Astronomer Cloud using access token
   - Creates a unique temporary directory locally for the test
   - Initializes an Astro project in the local temp directory
   - Creates a new Airflow deployment in Astronomer Cloud
   - Updates GitHub secrets with deployment information
   - Creates an API token for the deployment
   - Creates a user in the Airflow deployment

2. **Test Execution**:
   - Provides the Airflow deployment details to the test
   - Each test gets its own isolated deployment
   - GitHub integration automatically syncs DAGs from the repository via GitHub action

3. **Cleanup Phase**:
   - Deletes the Astronomer deployment
   - Removes temporary directories

## GitHub Integration

The fixture automatically:

- Updates GitHub repository secrets with deployment information
- Syncs DAGs from the configured GitHub repository
- Manages deployment lifecycle through GitHub Actions (if configured)

### Required GitHub Secrets

The fixture automatically creates/updates these secrets in your GitHub repository:

- `ASTRO_DEPLOYMENT_ID`: The deployment ID in Astronomer
- `ASTRO_DEPLOYMENT_NAME`: The deployment name
- `ASTRO_ACCESS_TOKEN`: The Astronomer access token

## Benefits

- **Cloud-based**: No local Docker setup required
- **Managed**: Astronomer handles infrastructure management
- **Scalable**: Can run multiple tests concurrently
- **Isolated**: Each test gets its own deployment
- **GitHub integration**: Automatic DAG synchronization
- **Automatic cleanup**: Resources are cleaned up after tests

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
- **Astro CLI not installed**: Install Astro CLI and ensure it's in PATH
- **GitHub access issues**: Verify GitHub token has repository access
- **Deployment creation failures**: Check Astronomer Cloud quotas and permissions
- **Unauthorized Tokens**: When using personal tokens, they expire typically with 1-2 hours

### Debug Information

The fixture provides detailed logging including:
- Worker process ID
- Creation timestamps
- Deployment IDs
- API URLs and tokens
- Error messages with context

### Timeouts

- **Deployment creation**: ~2-3 minutes
- **Airflow readiness**: ~3 minutes after deployment
- **DAG execution**: Varies by DAG complexity

## Dependencies

- **Astro CLI**: For Astronomer Cloud management
- **PyGithub**: For GitHub repository management
- **pytest**: For fixture functionality
- **requests**: For HTTP communication with Airflow API
- **GitPython**: For repository operations

## Security Notes

- API tokens are automatically generated and cleaned up
- GitHub secrets are managed securely through the GitHub API
- All sensitive information is handled through environment variables
- Temporary files are cleaned up after tests complete 