# Airflow Resource Fixture

The `airflow_resource` fixture provides isolated Airflow instances for each test, similar to the MongoDB fixture pattern.

## Overview

The fixture creates unique Airflow instances using Docker Compose for each test function, ensuring complete isolation between tests.

## Features

- **Function-scoped**: Each test gets its own Airflow instance
- **Unique instances**: Each test runs in its own isolated environment
- **Dynamic port allocation**: Avoids port conflicts between concurrent tests
- **Automatic cleanup**: Resources are automatically cleaned up after each test
- **Temporary directories**: Each instance uses a separate temporary directory

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
    username = airflow_resource["username"]
    password = airflow_resource["password"]
    
    # Your test logic here
    # The Airflow instance is already started, verified it is healthy via API and ready to use
```

### Resource Data Structure

The `airflow_resource` fixture returns a dictionary with the following structure:

```python
{
    "resource_id": "airflow_test_name_timestamp",
    "type": "airflow_resource",
    "test_name": "test_function_name",
    "creation_time": timestamp,
    "worker_pid": process_id,
    "creation_duration": setup_time_in_seconds,
    "description": "An Airflow instance for test_name",
    "status": "active",
    "airflow_instance": Airflow_Local_instance,
    "temp_dir": "/tmp/airflow_test_xxx",
    "project_name": "airflow_test_name_timestamp",
    "base_url": "http://localhost:port",
    "username": "airflow",
    "password": "airflow"
}
```

### Example Test

```python
import pytest
import requests
from requests.auth import HTTPBasicAuth

@pytest.mark.airflow
def test_airflow_dag_execution(airflow_resource):
    """Test that a DAG can be executed in the isolated Airflow instance."""
    
    # Get connection details
    base_url = airflow_resource["base_url"]
    username = airflow_resource["username"]
    password = airflow_resource["password"]
    
    # Your test logic here
    # The Airflow instance is ready to use
    
    # Example: Check if Airflow is running
    response = requests.get(
        f"{base_url}/api/v1/dags",
        auth=HTTPBasicAuth(username, password)
    )
    assert response.status_code == 200
```

## Configuration

The fixture uses the following environment variables (with defaults):

- `AIRFLOW_USERNAME`: Username for Airflow (default: "airflow")
- `AIRFLOW_PASSWORD`: Password for Airflow (default: "airflow")
- `AIRFLOW_HOST`: Host for Airflow (default: "localhost")

## How It Works

1. **Setup Phase**:
   - Creates a unique temporary directory
   - Copies the Airflow environment files
   - Generates a unique project name and port
   - Modifies docker-compose.yml for isolation
   - Starts the Airflow containers

2. **Test Execution**:
   - Provides the Airflow instance to the test
   - Each test gets its own isolated environment

3. **Cleanup Phase**:
   - Stops the Airflow containers
   - Removes temporary directories
   - Restores original environment variables

## Benefits

- **Isolation**: Each test runs in its own Airflow environment
- **No Conflicts**: Tests can run concurrently without interfering
- **Clean State**: Each test starts with a fresh Airflow instance
- **Automatic Management**: No manual cleanup required
- **Consistent**: Same setup for all tests using the fixture

## Integration with Existing Tests

The fixture is designed to be a drop-in replacement for manual Airflow setup. Existing tests can be updated by:

1. Adding `airflow_resource` as a parameter
2. Replacing manual Airflow_Local instantiation with the fixture
3. Using the provided connection details instead of environment variables

## Troubleshooting

- **Port Conflicts**: The fixture uses dynamic port allocation to avoid conflicts
- **Startup Time**: Airflow instances take ~30 seconds to start
- **Resource Usage**: Each test creates its own Docker containers
- **Cleanup**: Temporary directories are automatically removed after tests

## Dependencies

- `python-on-whales`: For Docker Compose management
- `pytest`: For fixture functionality
- `requests`: For HTTP communication with Airflow
- Docker: For running Airflow containers 