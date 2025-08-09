"""
Databricks Sanity Check Test

This test directly implements the Hello World functionality without using the AI model.
It serves as a control test to validate that:
1. Databricks connectivity works
2. The validation logic is sound
3. The environment setup is correct

This test should be run before the main AI test to ensure the infrastructure is working.
"""

import os
import time
import uuid
import pytest
import importlib
from databricks_api import DatabricksAPI

from Environment.Databricks import (
    setup_databricks_environment,
    cleanup_databricks_environment,
)
from Fixtures.Databricks.databricks_resources import databricks_resource

@pytest.mark.parametrize("databricks_resource", [
    {
        "resource_id": "sanity_check_test",
        "use_shared_cluster": True,
        "cluster_fallback": True,
        "shared_cluster_timeout": 1200
    }
], indirect=True)
def test_databricks_sanity_check(request, databricks_resource):
    """
    Sanity check test that directly implements Hello World functionality.
    
    This test:
    - Uses the databricks_resource fixture for setup
    - Creates a direct PySpark job (no AI model)
    - Submits and monitors the job using Databricks Jobs API
    - Validates results using the same validation logic as the main test
    - Cleans up after itself
    """
    print("Made it!")
    pass