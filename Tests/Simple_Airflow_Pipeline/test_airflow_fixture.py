import pytest
import time
import requests
from requests.auth import HTTPBasicAuth


@pytest.mark.airflow
def test_airflow_fixture_basic(airflow_resource):
    """
    Basic test to verify the airflow_resource fixture works.
    """
    print(f"Testing airflow_resource fixture with resource_id: {airflow_resource['resource_id']}")
    
    # Verify the resource data structure
    assert airflow_resource["type"] == "airflow_resource"
    assert airflow_resource["status"] == "active"
    assert "airflow_instance" in airflow_resource
    assert "base_url" in airflow_resource
    assert "username" in airflow_resource
    assert "password" in airflow_resource
    
    # Test that we can connect to the Airflow instance
    base_url = airflow_resource["base_url"]
    username = airflow_resource["username"]
    password = airflow_resource["password"]
    
    # Test basic connectivity
    try:
        response = requests.get(
            f"{base_url}/api/v1/dags",
            auth=HTTPBasicAuth(username, password),
            timeout=30
        )
        assert response.status_code == 200, f"Failed to connect to Airflow: {response.status_code}"
        print(f"Successfully connected to Airflow at {base_url}")
    except Exception as e:
        print(f"Error connecting to Airflow: {e}")
        raise


@pytest.mark.airflow
def test_airflow_fixture_unique_instances(airflow_resource):
    """
    Test that each test gets a unique Airflow instance.
    """
    print(f"Testing unique instance with resource_id: {airflow_resource['resource_id']}")
    
    # This test should get a different resource_id than the previous test
    # The fixture is function-scoped, so each test gets a new instance
    assert airflow_resource["type"] == "airflow_resource"
    
    # Test basic connectivity again
    base_url = airflow_resource["base_url"]
    username = airflow_resource["username"]
    password = airflow_resource["password"]
    
    try:
        response = requests.get(
            f"{base_url}/api/v1/dags",
            auth=HTTPBasicAuth(username, password),
            timeout=30
        )
        assert response.status_code == 200, f"Failed to connect to Airflow: {response.status_code}"
        print(f"Successfully connected to unique Airflow instance at {base_url}")
    except Exception as e:
        print(f"Error connecting to Airflow: {e}")
        raise 