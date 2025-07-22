"""
Databricks Environment Setup Module

This module provides cluster management, caching, and validation functionality
for Databricks testing environments.
"""

from .setup import (
    get_or_create_cluster,
    setup_databricks_environment,
    cleanup_databricks_environment,
    clear_cluster_cache,
    get_cached_cluster_info
)

from .validation import (
    extract_warehouse_id_from_http_path,
    execute_sql_query
)

__all__ = [
    'get_or_create_cluster',
    'setup_databricks_environment',
    'cleanup_databricks_environment',
    'clear_cluster_cache',
    'get_cached_cluster_info',
    'extract_warehouse_id_from_http_path',
    'execute_sql_query'
] 