"""
Shared fixtures for Databricks Hello World tests.
These fixtures allow cluster caching between the main test and sanity check test.
"""

import pytest
import importlib
import os
from databricks_api import DatabricksAPI
from Environment.Databricks import get_or_create_cluster


@pytest.fixture(scope="session")
def databricks_client():
    """Initialize Databricks API client for validation - shared across all tests"""
    # Import the current test's configs
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir_name = os.path.basename(current_dir)
    module_path = f"Tests.{parent_dir_name}.Test_Configs"
    Test_Configs = importlib.import_module(module_path)
    
    config = Test_Configs.Configs["services"]["databricks"]
    
    # Check if required environment variables are set
    if not config["host"] or not config["token"]:
        pytest.skip("Databricks credentials not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
    
    # Ensure host has proper format
    host = config["host"]
    if not host.startswith("https://"):
        host = f"https://{host}"
    
    return DatabricksAPI(
        host=host,
        token=config["token"]
    )


@pytest.fixture(scope="session")
def shared_cluster(databricks_client):
    """
    Get or create a shared cluster for all tests in this session.
    This enables cluster caching between the main test and sanity check.
    """
    # Import the current test's configs
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir_name = os.path.basename(current_dir)
    module_path = f"Tests.{parent_dir_name}.Test_Configs"
    Test_Configs = importlib.import_module(module_path)
    
    config = Test_Configs.Configs["services"]["databricks"]
    
    # Get or create cluster (with caching)
    cluster_id, cluster_created_by_us = get_or_create_cluster(databricks_client, config)
    
    # Store cluster info for cleanup decisions
    cluster_info = {
        "cluster_id": cluster_id,
        "created_by_us": cluster_created_by_us,
        "client": databricks_client,
        "config": config
    }
    
    yield cluster_info
    
    # Session-level cleanup is handled by individual tests
    # since they may have different cleanup needs 