"""
Pytest configuration file for Databricks Hello World tests.
This file makes shared fixtures available to both the main test and sanity check test.
"""

# Import centralized fixtures from Fixtures/Databricks package
from Fixtures.Databricks.databricks_resources import databricks_resource 