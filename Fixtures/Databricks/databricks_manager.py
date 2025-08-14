"""
Helper classes for managing Databricks resources.
"""

import hashlib
import importlib
import json
import os
import time
from typing import Any, Optional, Tuple

import pytest
from databricks_api import DatabricksAPI
from dotenv import load_dotenv

from Fixtures import parse_test_name
from Fixtures.Databricks.cache_manager import CacheManager


class DatabricksManager:
    """
    A class to manage Databricks resources such as clusters, jobs, and notebooks.

    :param Optional[dict] config: Configuration dictionary containing Databricks connection details, defaults to None.
    :param Optional[pytest.FixtureRequest] request: pytest.FixtureRequest object containing the test request, defaults to None.
    :param Optional[int] default_expiry_hours: Default expiry time for cached clusters, defaults to 1 hour.
    """

    def __init__(
        self,
        cluster_name: str,
        config: Optional[dict] = None,
        request: Optional[pytest.FixtureRequest] = None,
        default_expiry_hours: Optional[int] = 1,
        cluster_id: Optional[str] = None,
        cluster_created_by_us: Optional[bool] = False,
        shared_cluster: Optional[bool] = False,
    ):
        self.cluster_name: str = cluster_name
        config = config or {}
        self.cluster_id: Optional[str] = config.get("cluster_id", cluster_id) or os.getenv("DATABRICKS_CLUSTER_ID")
        self.cluster_id: Optional[str] = config.get("cluster_id") or self.cluster_id
        self.cluster_created_by_us = cluster_created_by_us
        self.is_shared: bool = shared_cluster
        self.cache_manager: CacheManager = CacheManager(default_expiry_hours=default_expiry_hours)
        self.expiry_time: str = self.cache_manager.expiry_time
        self.config: dict = self.verify_config_and_envars(config)
        self.test_name: str = parse_test_name(getattr(request.node, "name", "direct_call"))
        self.status: str = "creating"  # Initial status
        self.error: str = ""  # Error message if any
        self.token: str = self.config["token"]
        self.host: str = (
            self.config["host"]
            if self.config["host"].startswith("https://")
            else f"https://{self.config['host']}"
        )
        self.client: Optional[DatabricksAPI] = None  # Will be set by create_databricks_client
        self.creation_time = time.time()
        self.worker_pid = os.getpid()
        self.created_by_us = False
        self.create_databricks_client(request)
        self.cluster_config_hash = self.get_cluster_config_hash()
        self.config_hash = self.cluster_config_hash
        self.headers = {
            "Authorization": f"Bearer {self.config['token']}",
            "Content-Type": "application/json",
        }
        # SQLite-based coordination replaces in-memory structures
        self.access_count: int = 1
        self.remove_terminated_clusters()

    @staticmethod
    def verify_config_and_envars(config: Optional[dict] = None) -> dict[str, Any]:
        """
        Method to verify and return the configuration dictionary by using environment variables or provided config.

        :param Optional[dict] config: Config dictionary containing Databricks connection details, defaults to None.
        :raises ValueError: If required keys are missing in the config and environment variables.
        :return: Verified configuration dictionary with host, token, and cluster_id.
        :rtype: dict[str, Any]
        """
        # Load environment variables from .env file
        load_dotenv()
        missing_keys = []

        backup_config = {
            "host": os.getenv("DATABRICKS_HOST", ""),
            "token": os.getenv("DATABRICKS_TOKEN", ""),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
            "is_shared": os.getenv("DATABRICKS_IS_SHARED", "false").lower() == "true"
        }
        if config is None:
            config = {}
        for key, env_value in backup_config.items():
            if key not in config or not config[key]:
                config[key] = env_value

        if missing_keys := [
            key
            for key in backup_config.keys()
            if key not in config or config[key] == ""
        ]:
            raise ValueError(
                f"Missing required keys in config: {', '.join(missing_keys)}"
            )
        return config

    def update_config_with_attributes(self) -> None:
        """
        Update the cluster configuration using the class' attributes and save to cache.
        """
        attributes = ["is_shared", "cluster_id", "cluster_name", "created_by_us", "status", "expiry_time", "access_count"]
        for attr in attributes:
            if val := getattr(self, attr):
                self.config[attr] = val
        self.cache_manager.save_cluster_cache(self.config)

    def remove_terminated_clusters(self) -> None:
        """
        Remove terminated clusters from the cache.

        :rtype: None
        """
        removed_count = 0
        print("Removing terminated clusters from cache...")
        clusters = self.client.cluster.list_clusters()
        if not clusters:
            print("No clusters found.")
            return
        for cluster in clusters.get("clusters", []):
            try:
                if cluster["state"] == "TERMINATED":
                    self.client.cluster.permanent_delete_cluster(cluster["cluster_id"])
                    self.cache_manager.remove_terminated_cluster(cluster["cluster_id"])
                    removed_count += 1
            except Exception as e:
                print(f"Error removing terminated clusters: {e}")
        print(f"Removed {removed_count} terminated clusters.")

    def setup_databricks_environment(self, cluster_id: str, config: dict[str, Any]) -> None:
        """
        Set up the Databricks environment for testing.

        :param str cluster_id: The ID of the cluster to set up the environment for.
        :param dict[str, Any] config: Configuration dictionary containing Databricks connection details.
        :rtype: None
        """
        # Update config with the cluster_id we're using
        config["cluster_id"] = cluster_id

        # Ensure output directory is clean
        try:
            self.client.dbfs.delete(
                path=config["delta_table_path"].replace("dbfs:", ""),
                recursive=True
            )
            print(f"Cleaned up existing output directory: {config['delta_table_path']}")
        except Exception as e:
            if "not found" not in str(e).lower():
                print(f"Warning during cleanup: {e}")

    def get_cluster_config_hash(self) -> str:
        """
        Create a unique hash for cluster configuration, excluding test-specific fields.
        Only configuration that affects cluster creation should be included.

        :return: Unique hash for cluster configuration.
        :rtype: str
        """
        cluster_relevant_config = {
            "host": self.host,
            "cluster_name": self.config.get("cluster_name", ""),
            "node_type_id": self.config.get("node_type_id", ""),
            "spark_version": self.config.get("spark_version", ""),
            "num_workers": self.config.get("num_workers", 1),
            "autotermination_minutes": self.config.get("autotermination_minutes", 120),
            "spark_conf": self.config.get("spark_conf", {}),
            "aws_attributes": self.config.get("aws_attributes", {}),
            # Excludes: resource_id, test_id, unique_message, delta_table_path, etc.
        }
        config_str = json.dumps(cluster_relevant_config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()

    def wait_for_cluster_creation(
        self, timeout=300, poll_interval=5, fallback=True
    ) -> "DatabricksManager":
        """
        Wait for another worker to finish creating the cluster.

        :param int timeout: Maximum time to wait for cluster creation, defaults to 300 seconds.
        :param int poll_interval: Time to wait between checks, defaults to 5 seconds.
        :param bool fallback: Whether to create a fallback cluster if the shared cluster creation fails, defaults to True.
        :raises RuntimeError: If shared cluster creation fails and fallback is disabled.
        :return: DataBricks manager instance with shared cluster information.
        :rtype: DatabricksManager
        """
        start_time = time.time()
        print(
            f"Worker {os.getpid()}: Waiting for shared cluster creation (config_hash: {self.cluster_config_hash[:8]}..., timeout={timeout}s)"
        )

        while time.time() - start_time < timeout:
            time.sleep(poll_interval)

            # Check SQLite registry for cluster status
            
            if cluster_info := self.cache_manager.get_shared_cluster_info(self.cluster_config_hash):
                if cluster_info["status"] == "ready":
                    # Increment usage count
                    new_count = self.cache_manager.increment_shared_cluster_usage(self.cluster_config_hash)
                    print(
                        f"Worker {os.getpid()}: Shared cluster {cluster_info['cluster_id']} is now ready (usage: {new_count})"
                    )
                    self.cluster_id = cluster_info["cluster_id"]
                    self.created_by_us = False
                    self.creation_time = time.time()
                    self.update_config_with_attributes()
                    return self
                elif cluster_info["status"] == "failed":
                    print(
                        f"Worker {os.getpid()}: Shared cluster creation failed: {cluster_info.get('error_message', 'Unknown error')}"
                    )
                    break

        # Timeout or failure - handle based on fallback setting
        if fallback:
            print(
                f"Worker {os.getpid()}: Timeout/failure waiting for shared cluster, creating fallback cluster"
            )
            return self.create_fallback_cluster()
        else:
            error_msg = (
                "Shared cluster creation failed/timed out and fallback is disabled"
            )
            print(f"Worker {os.getpid()}: {error_msg}")
            raise RuntimeError(error_msg)

    def create_fallback_cluster(self) -> "DatabricksManager":
        """
        Create a fallback cluster when shared cluster creation fails or times out.

        :return: DatabricksManager instance with fallback cluster information.
        :rtype: DatabricksManager
        """
        print(f"Worker {os.getpid()}: Creating fallback cluster")
        cluster_id, cluster_created_by_us = self.get_or_create_cluster()

        self.cluster_id = cluster_id
        self.created_by_us = cluster_created_by_us
        self.creation_time = time.time()
        self.status = "ready"
        self.update_config_with_attributes()
        return self

    def create_new_shared_cluster(self) -> "DatabricksManager":
        """
        Create a new shared cluster and register it in the global registry.

        :return: DatabricksManager instance with new shared cluster information.
        :rtype: DatabricksManager
        """
        print(
            f"Worker {os.getpid()}: Creating new shared cluster (config_hash: {self.cluster_config_hash[:8]}...)"
        )

        # Register this worker as creating the shared cluster
        if not self.cache_manager.register_shared_cluster_creation(self.cluster_config_hash, os.getpid()):
            print(f"Worker {os.getpid()}: Another worker is already creating this shared cluster")
            return self.wait_for_cluster_creation()

        try:
            # Actually create the cluster
            cluster_id, cluster_created_by_us = self.get_or_create_cluster()

            print(
                f"Worker {os.getpid()}: Successfully created shared cluster {cluster_id}"
            )

            # Update status to ready
            self.cache_manager.update_shared_cluster_status(
                self.cluster_config_hash, 
                "ready", 
                cluster_id=cluster_id
            )

            self.cluster_id = cluster_id
            self.created_by_us = cluster_created_by_us
            self.worker_pid = os.getpid()
            self.creation_time = time.time()
            self.status = "ready"
            self.is_shared = True
            self.update_config_with_attributes()
            return self

        except Exception as e:
            # Mark as failed on error
            self.status = "failed"
            self.error = str(e)

            # Update status to failed
            self.cache_manager.update_shared_cluster_status(
                self.cluster_config_hash, 
                "failed", 
                error_message=str(e)
            )

            print(f"Worker {os.getpid()}: Failed to create shared cluster: {e}")

            # Fall back to creating our own cluster
            return self.create_fallback_cluster()

    def create_new_cluster(self) -> "DatabricksManager":
        """
        Create a new non-shared cluster and register it in the global registry.

        :return: DatabricksManager instance with new shared cluster information.
        :rtype: DatabricksManager
        """
        print(
            f"Worker {os.getpid()}: Creating new non-shared cluster (config_hash: {self.cluster_config_hash[:8]}...)"
        )

        # Register this worker as creating the cluster
        if not self.cache_manager.register_shared_cluster_creation(self.cluster_config_hash, os.getpid()):
            print(f"Worker {os.getpid()}: Another worker is already creating this cluster")
            return self.wait_for_cluster_creation()

        try:
            # Actually create the cluster
            cluster_id, cluster_created_by_us = self.get_or_create_cluster()

            print(
                f"Worker {os.getpid()}: Successfully created non-shared cluster {cluster_id}"
            )

            # Update status to ready
            self.cache_manager.update_shared_cluster_status(
                self.cluster_config_hash, 
                "ready", 
                cluster_id=cluster_id
            )

            self.cluster_id = cluster_id
            self.created_by_us = cluster_created_by_us
            self.worker_pid = os.getpid()
            self.creation_time = time.time()
            self.is_shared = False
            self.status = "ready"
            self.update_config_with_attributes()
            return self

        except Exception as e:
            # Mark as failed on error
            self.status = "failed"
            self.error = str(e)

            # Update status to failed
            self.cache_manager.update_shared_cluster_status(
                self.cluster_config_hash, 
                "failed", 
                error_message=str(e)
            )

            print(f"Worker {os.getpid()}: Failed to create cluster: {e}")

            # Fall back to creating our own cluster
            return self.create_fallback_cluster()

    def create_shared_cluster_with_mutex(
        self, timeout: Optional[int] = 300, fallback: Optional[bool] = True
    ) -> "DatabricksManager":
        """
        Thread-safe shared cluster creation with SQLite-based coordination.

        :param Optional[int] timeout: Maximum time to wait for cluster creation, defaults to 300 seconds.
        :param Optional[bool] fallback: Whether to create a fallback cluster if the shared cluster creation fails, defaults to True.
        :return: DatabricksManager instance with shared cluster information.
        :rtype: None
        """
        print(
            f"Worker {os.getpid()}: Requesting shared cluster (config_hash: {self.cluster_config_hash[:8]}..., timeout={timeout}s)"
        )

        # Check if cluster already exists and is ready
        if cluster_info := self.cache_manager.get_shared_cluster_info(self.cluster_config_hash):
            if cluster_info["status"] == "ready":
                # Cluster is ready, increment usage and return
                new_count = self.cache_manager.increment_shared_cluster_usage(self.cluster_config_hash)
                print(
                    f"Worker {os.getpid()}: Reusing existing shared cluster {cluster_info['cluster_id']} (usage: {new_count})"
                )
                self.cluster_id = cluster_info["cluster_id"]
                self.created_by_us = False
                self.creation_time = time.time()
                self.worker_pid = os.getpid()
                self.is_shared = True
                self.config_hash = self.cluster_config_hash
                self.update_config_with_attributes()
                return self
            elif cluster_info["status"] == "failed":
                # Previous creation failed, clean up and try again
                print(
                    f"Worker {os.getpid()}: Previous shared cluster creation failed, retrying"
                )
                self.cache_manager.cleanup_shared_cluster_registry(self.cluster_config_hash)
            elif cluster_info["status"] == "creating":
                # Another worker is creating, wait for it
                print(
                    f"Worker {os.getpid()}: Another worker ({cluster_info['worker_pid']}) is creating shared cluster"
                )
                return self.wait_for_cluster_creation(timeout=timeout, fallback=fallback)

        # We need to create the cluster
        print(
            f"Worker {os.getpid()}: Creating new shared cluster (config_hash: {self.cluster_config_hash[:8]}...)"
        )
        return self.create_new_shared_cluster()

    def cleanup_shared_cluster(self) -> None:
        """
        Coordinate cleanup of shared clusters - only delete when no tests are using it.

        :rtype: None
        """
        print(
            f"Worker {os.getpid()}: Cleaning up shared cluster {self.cluster_id} (config_hash: {self.cluster_config_hash[:8]}...)"
        )

        # Decrement usage count
        remaining_count = self.cache_manager.decrement_shared_cluster_usage(self.cluster_config_hash)
        
        if remaining_count <= 0:
            # No more tests using this cluster, safe to delete
            cluster_info = self.cache_manager.get_shared_cluster_info(self.cluster_config_hash)
            should_delete = self.created_by_us if cluster_info else False

            # Clean up registry
            self.cache_manager.cleanup_shared_cluster_registry(self.cluster_config_hash)

            if should_delete:
                print(
                    f"Worker {os.getpid()}: Deleting shared cluster {self.cluster_id} (last user)"
                )
                try:
                    self.client.cluster.delete_cluster(self.cluster_id)
                    print(
                        f"Worker {os.getpid()}: Successfully deleted shared cluster {self.cluster_id}"
                    )
                except Exception as e:
                    print(
                        f"Warning: Could not delete shared cluster {self.cluster_id}: {e}"
                    )
            else:
                print(
                    f"Worker {os.getpid()}: Not deleting shared cluster {self.cluster_id} (not creator)"
                )
        else:
            print(
                f"Worker {os.getpid()}: Shared cluster {self.cluster_id} still in use by {remaining_count} other test(s)"
            )

    def create_databricks_client(
        self, request: Optional[pytest.FixtureRequest] = None
    ) -> None:
        """
        Shared method to create a Databricks API client.
        Can be used by fixtures or directly by test code.

        :param Optional[pytest.FixtureRequest] request: pytest.FixtureRequest object containing the test request.
        :raises ValueError: If Databricks credentials are not configured.
        :raises pytest.skip: If Databricks configuration is not found and request is provided.
        :rtype: None
        """
        start_time = time.time()
        print(f"Worker {os.getpid()}: Starting databricks_client for {self.test_name}")

        # Try to get config from param first, then fall back to test directory detection
        if not self.config:
            if request:
                current_dir = os.path.dirname(os.path.abspath(request.fspath))
                parent_dir_name = os.path.basename(current_dir)
                try:
                    module_path = f"Tests.{parent_dir_name}.Test_Configs"
                    Test_Configs = importlib.import_module(module_path)
                    self.config = Test_Configs.Configs["services"]["databricks"]
                    self.token = self.config.get("token")
                except (ImportError, KeyError, ValueError) as e:
                    print(
                        f"Worker {os.getpid()}: Error importing Databricks configuration: {e}"
                    )
                    raise e
            else:
                raise ValueError(
                    "Either config parameter or request parameter must be provided"
                )

        # Check if required environment variables are set
        if not self.config["host"] or not self.config["token"]:
            error_msg = "Databricks credentials not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables."
            if request:
                pytest.skip(error_msg)
            else:
                raise ValueError(error_msg)

        self.client = DatabricksAPI(host=self.host, token=self.config["token"])

        setup_time = time.time() - start_time
        print(f"Worker {os.getpid()}: Databricks client setup took {setup_time:.2f}s")

    def create_shared_cluster(
        self, timeout: Optional[int] = 300, fallback: Optional[bool] = True
    ) -> "DatabricksManager":
        """
        Shared method to create or get a shared cluster with mutex coordination.
        Can be used by fixtures or directly by test code.

        :param Optional[int] timeout: Maximum time to wait for cluster creation, defaults to 300 seconds.
        :param Optional[bool] fallback: Whether to create a fallback cluster if the shared cluster creation fails, defaults to True.
        :return: DatabricksManager instance with shared cluster information.
        :rtype: DatabricksManager
        """
        start_time = time.time()
        print(f"Worker {os.getpid()}: Starting shared_cluster setup with mutex")

        # Use the new mutex-based approach
        databricks_manager = self.create_shared_cluster_with_mutex(
            timeout=timeout, fallback=fallback
        )

        setup_time = time.time() - start_time
        print(f"Worker {os.getpid()}: Shared cluster setup took {setup_time:.2f}s")
        print(
            f"Worker {os.getpid()}: Using cluster {self.cluster_id} (created_by_us: {self.created_by_us})"
        )
        return databricks_manager

    @staticmethod
    def extract_warehouse_id_from_http_path(http_path: str) -> Optional[str]:
        """
        Extract warehouse ID from HTTP path like /sql/1.0/warehouses/abc123

        :param str http_path: HTTP path to extract warehouse ID from.
        :return: Warehouse ID or None if not found.
        :rtype: Optional[str]
        """
        if "/warehouses/" in http_path:
            return http_path.split("/warehouses/")[-1]
        return None

    def execute_sql_query(
        self,
        warehouse_id: str,
        sql_query: str,
        catalog: Optional[str] = "hive_metastore",
        schema: Optional[str] = "default",
        timeout: Optional[int] = 30,
    ) -> dict[str, Any]:
        """
        Execute SQL query using Databricks SQL Statement Execution API

        :param str warehouse_id: Warehouse ID to execute SQL query on.
        :param str sql_query: SQL query to execute.
        :param Optional[str] catalog: Catalog to execute SQL query on, defaults to "hive_metastore".
        :param Optional[str] schema: Schema to execute SQL query on, defaults to "default".
        :param Optional[int] timeout: Timeout for SQL query execution, defaults to 30 seconds.
        :return: Dictionary containing query result.
        :rtype: dict[str, Any]
        """
        from urllib.parse import urljoin

        import requests

        # Databricks SQL API requires wait_timeout to be 0 or between 5-50 seconds
        sql_timeout = min(max(timeout, 5), 50) if timeout > 0 else 0

        # Prepare request payload for SQL execution
        payload = {
            "warehouse_id": warehouse_id,
            "catalog": catalog,
            "schema": schema,
            "statement": sql_query,
            "wait_timeout": f"{sql_timeout}s",
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
        }

        try:
            # Execute SQL statement
            response = requests.post(
                urljoin(self.host, "/api/2.0/sql/statements/"),
                headers=self.headers,
                json=payload,
                timeout=timeout + 10,  # Add buffer for HTTP timeout
            )

            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "status_code": response.status_code,
                }

            result = response.json()

            # Check if statement executed successfully
            if result.get("status", {}).get("state") == "SUCCEEDED":
                return {
                    "success": True,
                    "data": result.get("result", {}).get("data_array", []),
                    "schema": result.get("manifest", {}).get("schema", {}),
                    "row_count": result.get("manifest", {}).get("total_row_count", 0),
                }
            elif result.get("status", {}).get("state") == "PENDING":
                return {
                    "success": False,
                    "error": f"Query timed out after {timeout} seconds",
                    "state": "PENDING",
                }
            else:
                return {
                    "success": False,
                    "error": f"Query failed with state: {result.get('status', {}).get('state')}",
                    "details": result,
                }

        except requests.exceptions.RequestException as e:
            return {"success": False, "error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": f"Unexpected error: {str(e)}"}

    def _cleanup_shared_resources(self, resource_data: dict[str, Any]) -> None:
        """
        Cleanup shared resources.

        :param dict[str, Any] resource_data: Dictionary containing resource data.
        :rtype: None
        """
        cluster_id = resource_data["cluster_id"]
        cluster_created_by_us = resource_data["cluster_created_by_us"]

        # Handle shared cluster cleanup with coordination
        if resource_data.get("is_shared_cluster", False) and resource_data.get(
            "cluster_config_hash"
        ):
            self.cleanup_shared_cluster()
        elif cluster_id and not resource_data.get("is_shared_cluster", False):
            # Handle non-shared cluster cleanup (fallback clusters, etc.)
            if cluster_created_by_us:
                try:
                    self.client.cluster.delete_cluster(cluster_id)
                    print(
                        f"Worker {os.getpid()}: Deleted non-shared cluster {cluster_id}"
                    )
                except Exception as e:
                    print(
                        f"Warning: Could not delete non-shared cluster {cluster_id}: {e}"
                    )

    def cleanup_databricks_resources(
        self, resources: list[dict[str, Any]], resource_data: dict[str, Any]
    ) -> None:
        """
        Cleanup Databricks resources.

        :param list[dict[str, Any]] resources: List of resources to cleanup.
        :param dict[str, Any] resource_data: Dictionary containing resource data.
        :rtype: None
        """
        warning_message = "An error occurred during cleanup"
        try:
            for resource in reversed(resources):
                if resource["type"] == "table":
                    warning_message = f"Could not drop table {resource['full_name']}"
                    self.client.sql.execute_query(
                        f"DROP TABLE IF EXISTS {resource['full_name']}"
                    )
                elif resource["type"] == "database":
                    warning_message = f"Could not drop database {resource['name']}"
                    self.client.sql.execute_query(
                        f"DROP DATABASE IF EXISTS {resource['name']} CASCADE"
                    )
                elif resource["type"] == "notebook":
                    warning_message = f"Could not delete notebook {resource['path']}"
                    self.client.workspace.delete(resource["path"], recursive=False)
                elif resource["type"] == "cluster" and resource.get(
                    "created_by_us", False
                ):
                    # This handles non-shared clusters
                    warning_message = (
                        f"Could not delete cluster {resource['cluster_id']}"
                    )
                    self.client.clusters.delete_cluster(resource["cluster_id"])
        except Exception as e:
            print(f"{warning_message}: {e}")

        self._cleanup_shared_resources(resource_data)
        print(
            f"Worker {os.getpid()}: Databricks resource {resource_data['resource_id']} cleaned up successfully"
        )

    def create_test_cluster(self) -> str:
        """Create a test cluster for the Hello World test"""
        cluster_config = {
            "cluster_name": self.cluster_name,
            "spark_version": "13.3.x-scala2.12",  # Latest LTS version
            "node_type_id": "m5.large",  # Small, supported instance type
            "num_workers": 0,  # Single node cluster to minimize cost
            "autotermination_minutes": self.cache_manager.default_expiry_hours * 65,  # added buffer
            "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]"
            },
            "aws_attributes": {
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 1,
                "ebs_volume_size": 100  # Minimum size in GB
            },
            "custom_tags": {
                "purpose": "de-bench-hello-world-testing",
                "auto-created": "true",
                "cached": "true",
                "shared": str(self.is_shared).lower(),
                "test_name": self.test_name,
            }
        }
        
        print(f"Creating test cluster: {self.test_name}")
        response = self.client.cluster.create_cluster(**cluster_config)
        cluster_id = response["cluster_id"]
        self.cluster_id = cluster_id
        
        # Wait for cluster to start
        print(f"Waiting for cluster {cluster_id} to start...")
        max_wait = 1200  # 20 minutes timeout
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            cluster_info = self.client.cluster.get_cluster(cluster_id)
            state = cluster_info["state"]

            print(f"Cluster {cluster_id} is in state: {state}")

            if state == "RUNNING":
                self.created_by_us = True
                self.status = state
                self.update_config_with_attributes()
                print(f"Cluster {cluster_id} is now running")
                return cluster_id
            elif state in ["ERROR", "TERMINATED"]:
                raise Exception(f"Cluster failed to start. State: {state}")
            
            time.sleep(5)
        
        raise Exception(f"Cluster {cluster_id} failed to start within {max_wait} seconds")
    
    def get_or_create_cluster(self, cluster_config: Optional[dict[str, Any]] = None) -> Tuple[str, bool]:
        """
        Get existing cluster or create a new one if needed, with caching support

        :param Optional[dict[str, Any]] cluster_config: Configuration dictionary containing Databricks connection details, defaults to None.
        :return: Tuple containing cluster ID and boolean indicating if the cluster was created by us.
        :rtype: Tuple[str, bool]
        """
        if cluster_config:
            self.config = cluster_config
        cluster_id = self.config.get("cluster_id", getattr(self, "cluster_id"))
        
        # Check cache first
        cache_data = self.cache_manager.load_cluster_cache()
        if cache_data and not self.cache_manager.is_cluster_cache_valid(cache_data):
            print(f"Found expired cached cluster {cache_data.get('cluster_id', 'unknown')}, clearing cache")
            cache_data = {}
        
        if self.cache_manager.is_cluster_cache_valid(cache_data):
            cached_cluster_id = cache_data["cluster_id"]
            print(f"Found valid cached cluster: {cached_cluster_id}")
            cached_cluster_id, created_by_us = self.try_existing_or_cached_cluster(cached_cluster_id, use_cache=True)
            if not created_by_us:
                return cached_cluster_id, False
        
        # If cluster_id is provided in config, try to use it
        if cluster_id:
            cluster_id, created_by_us = self.try_existing_or_cached_cluster(cluster_id, use_cache=False)
            if not created_by_us:
                return cluster_id, False

        print("Checking all clusters in databricks for a shared one...")
        all_clusters = self.client.cluster.list_clusters()
        # check all running clusters to see if there is a shared one we can use
        for cluster in all_clusters.get("clusters", []):
            if cluster.get("custom_tags", {}).get("is_shared", "true").lower() == "true" and cluster["state"] == "RUNNING":
                self.status = cluster["state"]
                self.cluster_id = cluster["cluster_id"]
                self.created_by_us = False
                self.is_shared = True
                self.update_config_with_attributes()
                print(f"Found and using existing shared cluster: {cluster['cluster_id']}")
                return cluster["cluster_id"], False
        # Create a new cluster and cache it
        print("Creating new test cluster")
        new_cluster_id = self.create_test_cluster()
        # Cache the cluster with its configuration details
        self.cache_manager.cache_new_cluster(new_cluster_id, self.config)  # type: ignore
        return new_cluster_id, True  # True = created by us
    
    def try_existing_or_cached_cluster(self, cluster_id: str, use_cache: bool = True) -> Tuple[str, bool]:
        """
        Try to use an existing cluster or a cached cluster

        :param str cluster_id: Cluster ID to try to use.
        :param bool use_cache: Whether to use the cache, defaults to True.
        :return: Tuple containing cluster ID and boolean indicating if the cluster was created by us.
        :rtype: Tuple[str, bool]
        """
        image_type = 'Cached' if use_cache else 'Existing'
        try:
            cluster_info = self.client.cluster.get_cluster(cluster_id)
            state = cluster_info["state"]

            if state == "RUNNING":
                print(f"Using {image_type} running cluster: {cluster_id}")
                # Update access tracking for cached clusters
                if use_cache:
                    self.cache_manager.update_cluster_access(cluster_id)
                return cluster_id, False
            elif state == "TERMINATED":
                print(f"{image_type} cluster {cluster_id} is terminated, creating new one")
        except Exception as e:
            print(f"Error with {image_type} cluster {cluster_id}: {e}. Creating new cluster.")
        print("Checking cache for a cluster...")
        if self.can_use_any_shared_cached_cluster(cluster_id):
            print(f"Using cached cluster: {cluster_id}")
            return cluster_id, False
        return cluster_id, True
    
    def can_use_any_shared_cached_cluster(self, cluster_id: str) -> Optional[str]:
        """
        Check if there are any cached clusters that can be used, if so, return the cluster ID

        :param str cluster_id: Cluster ID to exclude from the cache check.
        :return: Cluster ID if there are any cached clusters that can be used, None otherwise.
        :rtype: str
        """
        clusters = self.cache_manager.get_all_clusters(where_clause=f"cluster_id != {cluster_id} and is_shared = 1")
        if clusters:
            return clusters[0]["cluster_id"]
        return None

    def cleanup_databricks_environment(self, config: dict[str, Any]) -> None:
        """Clean up the Databricks environment after testing"""
        print("Starting cleanup...")

        # 1. Try to drop the table if we have SQL capabilities
        try:
            warehouse_id = self.extract_warehouse_id_from_http_path(config.get("http_path", ""))
            if warehouse_id:
                catalog = config["catalog"]
                schema = config["schema"]
                table = config["table"]

                drop_table_query = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
                result = self.execute_sql_query(warehouse_id, drop_table_query, catalog, schema)

                if result["success"]:
                    print(f"✓ Dropped table: {catalog}.{schema}.{table}")
                else:
                    print(f"Warning: Could not drop table: {result['error']}")
            else:
                print("Note: Table cleanup skipped (no warehouse ID available)")
        except Exception as e:
            print(f"Warning: Could not drop table: {e}")

        # 2. Remove the output directory
        try:
            self.client.dbfs.delete(
                path=config["delta_table_path"].replace("dbfs:", ""),
                recursive=True
            )
            print(f"✓ Removed Delta table directory: {config['delta_table_path']}")
        except Exception as e:
            print(f"Warning: Could not delete output directory: {e}")

        # # 3. DO NOT terminate cached clusters - let them expire naturally
        # # Only terminate if explicitly created by us and not cached
        # cluster_from_env = os.getenv("DATABRICKS_CLUSTER_ID") is not None
        # cache_data = load_cluster_cache()
        # cluster_is_cached = cache_data.get("cluster_id") == config.get("cluster_id")

        # if cluster_created_by_us and config.get("cluster_id") and not cluster_from_env and not cluster_is_cached:
        #     try:
        #         print(f"Terminating test cluster: {config['cluster_id']}")
        #         client.cluster.delete_cluster(config["cluster_id"])
        #         print(f"✓ Terminated cluster: {config['cluster_id']}")
        #     except Exception as e:
        #         print(f"Warning: Could not terminate cluster: {e}")
        # else:
        #     if cluster_from_env:
        #         print(f"Cluster cleanup skipped (cluster from env var: {config.get('cluster_id')})")
        #     elif cluster_is_cached:
        #         print(f"Cluster cleanup skipped (cached cluster: {config.get('cluster_id')})")
        #     else:
        #         print(f"Cluster cleanup skipped (using existing cluster: {config.get('cluster_id')})")

        print("Cleanup completed.")
