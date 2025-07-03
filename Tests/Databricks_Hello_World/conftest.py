"""
Pytest configuration file for Databricks Hello World tests.
This file makes shared fixtures available to both the main test and sanity check test.
"""

# Import shared fixtures to make them available to all test files in this directory
from Tests.Databricks_Hello_World.shared_fixtures import databricks_client, shared_cluster 