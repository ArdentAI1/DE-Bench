import pytest
import json
import time
import os
import importlib
import uuid
import hashlib
import threading
from threading import Lock
from databricks_api import DatabricksAPI
from Environment.Databricks import get_or_create_cluster

# Global registry for shared cluster coordination
SHARED_CLUSTERS = {}
CLUSTER_LOCKS = {}
CLUSTER_USAGE_COUNT = {}
REGISTRY_LOCK = Lock()  # Protects the global registries themselves


class ClusterInfo:
    """Information about a shared cluster being created or in use."""
    def __init__(self):
        self.cluster_id = None
        self.created_by_us = False
        self.status = "creating"  # creating, ready, failed
        self.error = None
        self.creation_time = None
        self.creator_pid = None
        self.client_info = None


def get_cluster_config_hash(config):
    """
    Create a unique hash for cluster configuration, excluding test-specific fields.
    Only configuration that affects cluster creation should be included.
    """
    cluster_relevant_config = {
        "host": config.get("host", ""),
        "cluster_name": config.get("cluster_name", ""),
        "node_type_id": config.get("node_type_id", ""),
        "spark_version": config.get("spark_version", ""),
        "num_workers": config.get("num_workers", 1),
        "autotermination_minutes": config.get("autotermination_minutes", 120),
        "spark_conf": config.get("spark_conf", {}),
        "aws_attributes": config.get("aws_attributes", {}),
        # Exclude: resource_id, test_id, unique_message, delta_table_path, etc.
    }
    config_str = json.dumps(cluster_relevant_config, sort_keys=True)
    return hashlib.md5(config_str.encode()).hexdigest()


def wait_for_cluster_creation(config_hash, client, config, timeout=300, poll_interval=5, fallback=True):
    """
    Wait for another worker to finish creating the cluster.
    """
    start_time = time.time()
    print(f"Worker {os.getpid()}: Waiting for shared cluster creation (config_hash: {config_hash[:8]}..., timeout={timeout}s)")
    
    while time.time() - start_time < timeout:
        time.sleep(poll_interval)
        
        if config_hash in SHARED_CLUSTERS:
            cluster_info = SHARED_CLUSTERS[config_hash]
            
            if cluster_info.status == "ready":
                with REGISTRY_LOCK:
                    CLUSTER_USAGE_COUNT[config_hash] += 1
                print(f"Worker {os.getpid()}: Shared cluster {cluster_info.cluster_id} is now ready")
                return {
                    "cluster_id": cluster_info.cluster_id,
                    "created_by_us": False,
                    "client": client,
                    "config": config,
                    "creation_time": cluster_info.creation_time,
                    "worker_pid": os.getpid(),
                    "is_shared": True,
                    "config_hash": config_hash
                }
            
            elif cluster_info.status == "failed":
                print(f"Worker {os.getpid()}: Shared cluster creation failed: {cluster_info.error}")
                break
    
    # Timeout or failure - handle based on fallback setting
    if fallback:
        print(f"Worker {os.getpid()}: Timeout/failure waiting for shared cluster, creating fallback cluster")
        return create_fallback_cluster(client, config)
    else:
        error_msg = f"Shared cluster creation failed/timed out and fallback is disabled"
        print(f"Worker {os.getpid()}: {error_msg}")
        raise RuntimeError(error_msg)


def create_fallback_cluster(client, config):
    """
    Create a fallback cluster when shared cluster creation fails or times out.
    """
    print(f"Worker {os.getpid()}: Creating fallback cluster")
    cluster_id, cluster_created_by_us = get_or_create_cluster(client, config)
    
    return {
        "cluster_id": cluster_id,
        "created_by_us": cluster_created_by_us,
        "client": client,
        "config": config,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "is_shared": False
    }


def create_new_shared_cluster(config_hash, client_info):
    """
    Create a new shared cluster and register it in the global registry.
    """
    config = client_info["config"]
    client = client_info["client"]
    
    print(f"Worker {os.getpid()}: Creating new shared cluster (config_hash: {config_hash[:8]}...)")
    
    # Create cluster info and mark as creating
    cluster_info = ClusterInfo()
    cluster_info.creator_pid = os.getpid()
    cluster_info.creation_time = time.time()
    cluster_info.client_info = client_info
    
    with REGISTRY_LOCK:
        SHARED_CLUSTERS[config_hash] = cluster_info
        CLUSTER_USAGE_COUNT[config_hash] = 1  # This worker will use it
    
    try:
        # Actually create the cluster
        cluster_id, cluster_created_by_us = get_or_create_cluster(client, config)
        
        # Update cluster info on success
        cluster_info.cluster_id = cluster_id
        cluster_info.created_by_us = cluster_created_by_us
        cluster_info.status = "ready"
        
        print(f"Worker {os.getpid()}: Successfully created shared cluster {cluster_id}")
        
        return {
            "cluster_id": cluster_id,
            "created_by_us": cluster_created_by_us,
            "client": client,
            "config": config,
            "creation_time": cluster_info.creation_time,
            "worker_pid": os.getpid(),
            "is_shared": True,
            "config_hash": config_hash
        }
        
    except Exception as e:
        # Mark as failed on error
        cluster_info.status = "failed"
        cluster_info.error = str(e)
        
        with REGISTRY_LOCK:
            CLUSTER_USAGE_COUNT[config_hash] -= 1  # This worker won't use it
        
        print(f"Worker {os.getpid()}: Failed to create shared cluster: {e}")
        
        # Fall back to creating our own cluster
        return create_fallback_cluster(client, config)


def create_shared_cluster_with_mutex(client_info, timeout=300, fallback=True):
    """
    Thread-safe shared cluster creation with mutex/wait mechanism.
    """
    config = client_info["config"]
    config_hash = get_cluster_config_hash(config)
    client = client_info["client"]
    
    print(f"Worker {os.getpid()}: Requesting shared cluster (config_hash: {config_hash[:8]}..., timeout={timeout}s)")
    
    # Get or create lock for this config
    with REGISTRY_LOCK:
        if config_hash not in CLUSTER_LOCKS:
            CLUSTER_LOCKS[config_hash] = Lock()
            CLUSTER_USAGE_COUNT[config_hash] = 0
    
    with CLUSTER_LOCKS[config_hash]:
        # Check if cluster already exists and is ready
        if config_hash in SHARED_CLUSTERS:
            cluster_info = SHARED_CLUSTERS[config_hash]
            
            if cluster_info.status == "ready":
                # Cluster is ready, increment usage and return
                with REGISTRY_LOCK:
                    CLUSTER_USAGE_COUNT[config_hash] += 1
                print(f"Worker {os.getpid()}: Reusing existing shared cluster {cluster_info.cluster_id}")
                return {
                    "cluster_id": cluster_info.cluster_id,
                    "created_by_us": False,  # We didn't create it
                    "client": client,
                    "config": config,
                    "creation_time": cluster_info.creation_time,
                    "worker_pid": os.getpid(),
                    "is_shared": True,
                    "config_hash": config_hash
                }
            
            elif cluster_info.status == "failed":
                # Previous creation failed, clean up and try again
                print(f"Worker {os.getpid()}: Previous shared cluster creation failed, retrying")
                with REGISTRY_LOCK:
                    if config_hash in SHARED_CLUSTERS:
                        del SHARED_CLUSTERS[config_hash]
                    CLUSTER_USAGE_COUNT[config_hash] = 0
            
            elif cluster_info.status == "creating":
                # Another worker is creating, wait for it
                print(f"Worker {os.getpid()}: Another worker ({cluster_info.creator_pid}) is creating shared cluster")
                # Release the config-specific lock to allow creator to work
    
    # Check if we need to wait for creation (outside the lock)
    if config_hash in SHARED_CLUSTERS and SHARED_CLUSTERS[config_hash].status == "creating":
        return wait_for_cluster_creation(config_hash, client, config, timeout=timeout, fallback=fallback)
    
    # We need to create the cluster
    return create_new_shared_cluster(config_hash, client_info)


def cleanup_shared_cluster(cluster_id, config_hash, client):
    """
    Coordinate cleanup of shared clusters - only delete when no tests are using it.
    """
    if config_hash not in CLUSTER_USAGE_COUNT:
        print(f"Worker {os.getpid()}: No usage count found for cluster {cluster_id}")
        return
    
    print(f"Worker {os.getpid()}: Cleaning up shared cluster {cluster_id} (config_hash: {config_hash[:8]}...)")
    
    should_delete = False
    
    with REGISTRY_LOCK:
        if config_hash in CLUSTER_USAGE_COUNT:
            CLUSTER_USAGE_COUNT[config_hash] -= 1
            remaining_count = CLUSTER_USAGE_COUNT[config_hash]
            
            if remaining_count <= 0:
                # No more tests using this cluster, safe to delete
                if config_hash in SHARED_CLUSTERS:
                    cluster_info = SHARED_CLUSTERS[config_hash]
                    should_delete = cluster_info.created_by_us
                    
                    # Clean up registry
                    del SHARED_CLUSTERS[config_hash]
                    del CLUSTER_USAGE_COUNT[config_hash]
                    if config_hash in CLUSTER_LOCKS:
                        del CLUSTER_LOCKS[config_hash]
            else:
                print(f"Worker {os.getpid()}: Shared cluster {cluster_id} still in use by {remaining_count} other test(s)")
    
    if should_delete:
        print(f"Worker {os.getpid()}: Deleting shared cluster {cluster_id} (last user)")
        try:
            client.clusters.delete_cluster(cluster_id)
            print(f"Worker {os.getpid()}: Successfully deleted shared cluster {cluster_id}")
        except Exception as e:
            print(f"Warning: Could not delete shared cluster {cluster_id}: {e}")
    else:
        print(f"Worker {os.getpid()}: Not deleting shared cluster {cluster_id} (not creator or still in use)")


def create_databricks_client(config=None, request=None):
    """
    Shared method to create a Databricks API client.
    Can be used by fixtures or directly by test code.
    """
    start_time = time.time()
    test_name = request.node.name if request and hasattr(request.node, 'name') else "direct_call"
    print(f"Worker {os.getpid()}: Starting databricks_client for {test_name}")
    
    # Try to get config from param first, then fall back to test directory detection
    if not config:
        if request:
            current_dir = os.path.dirname(os.path.abspath(request.fspath))
            parent_dir_name = os.path.basename(current_dir)
            try:
                module_path = f"Tests.{parent_dir_name}.Test_Configs"
                Test_Configs = importlib.import_module(module_path)
                config = Test_Configs.Configs["services"]["databricks"]
            except ImportError:
                if request:
                    pytest.skip("Databricks configuration not found. Either pass config as param or ensure Test_Configs module exists.")
                else:
                    raise ValueError("Databricks configuration not found. Pass config parameter.")
        else:
            raise ValueError("Either config parameter or request parameter must be provided")
    
    # Check if required environment variables are set
    if not config["host"] or not config["token"]:
        error_msg = "Databricks credentials not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables."
        if request:
            pytest.skip(error_msg)
        else:
            raise ValueError(error_msg)
    
    # Ensure host has proper format
    host = config["host"]
    if not host.startswith("https://"):
        host = f"https://{host}"
    
    client = DatabricksAPI(host=host, token=config["token"])
    
    client_info = {
        "client": client,
        "config": config,
        "host": host,
        "creation_time": time.time(),
        "worker_pid": os.getpid()
    }
    
    setup_time = time.time() - start_time
    print(f"Worker {os.getpid()}: Databricks client setup took {setup_time:.2f}s")
    
    return client_info


def create_shared_cluster(client_info, timeout=300, fallback=True):
    """
    Shared method to create or get a shared cluster with mutex coordination.
    Can be used by fixtures or directly by test code.
    """
    start_time = time.time()
    print(f"Worker {os.getpid()}: Starting shared_cluster setup with mutex")
    
    # Use the new mutex-based approach
    cluster_info = create_shared_cluster_with_mutex(client_info, timeout=timeout, fallback=fallback)
    
    setup_time = time.time() - start_time
    print(f"Worker {os.getpid()}: Shared cluster setup took {setup_time:.2f}s")
    print(f"Worker {os.getpid()}: Using cluster {cluster_info['cluster_id']} (created_by_us: {cluster_info['created_by_us']})")
    
    return cluster_info


# DEPRECATED: Session fixtures commented out - use shared methods above instead
# @pytest.fixture(scope="session")
# def databricks_client(request):
#     """DEPRECATED: Use create_databricks_client() method instead"""
#     return create_databricks_client(request=request)


# @pytest.fixture(scope="session")
# def shared_cluster(databricks_client):
#     """DEPRECATED: Use create_shared_cluster() method instead"""
#     return create_shared_cluster(databricks_client)


@pytest.fixture(scope="function")
def databricks_resource(request):
    """
    A function-scoped fixture that creates Databricks resources based on template.
    Template structure: {
        "resource_id": "id", 
        "databricks_config": {...},  # Optional, uses client config if not provided
        "cluster_config": {...},     # Optional cluster configuration overrides
        "databases": [{"name": "db", "tables": [{"name": "table", "data": [], "format": "delta"}]}],
        "notebooks": [{"path": "/path/to/notebook", "content": "# Notebook content", "language": "python"}],
        "jobs": [{"name": "job_name", "notebook_path": "/path", "cluster_id": "optional"}]
    }
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting databricks_resource for {test_name}")
    
    build_template = request.param
    
    # Get databricks config from template or auto-detect
    template_config = build_template.get("databricks_config")
    client_info = create_databricks_client(config=template_config, request=request)
    client = client_info["client"]
    base_config = client_info["config"]
    
    # Merge any template-specific config with base config
    config = base_config.copy()
    if "databricks_config" in build_template:
        config.update(build_template["databricks_config"])
    
    print(f"Worker {os.getpid()}: Creating Databricks resource for {test_name}")
    creation_start = time.time()
    
    created_resources = []
    
    # Handle cluster creation/selection
    cluster_id = None
    cluster_created_by_us = False
    
    cluster_config_hash = None
    is_shared_cluster = False
    
    if build_template.get("use_shared_cluster", False):
        # Use shared cluster approach with mutex coordination
        timeout = build_template.get("shared_cluster_timeout", 300)
        fallback = build_template.get("cluster_fallback", True)
        
        cluster_info = create_shared_cluster(client_info, timeout=timeout, fallback=fallback)
        cluster_id = cluster_info["cluster_id"]
        cluster_created_by_us = cluster_info["created_by_us"]
        cluster_config_hash = cluster_info.get("config_hash")
        is_shared_cluster = cluster_info.get("is_shared", True)
        # Don't add to created_resources since this is a shared cluster managed globally
    elif "cluster_config" in build_template:
        # Create a specific cluster for this resource
        cluster_config = {**config, **build_template["cluster_config"]}
        cluster_id, cluster_created_by_us = get_or_create_cluster(client, cluster_config)
        created_resources.append({"type": "cluster", "cluster_id": cluster_id, "created_by_us": cluster_created_by_us})
        is_shared_cluster = False
    
    # Process databases and tables
    if "databases" in build_template:
        for db_config in build_template["databases"]:
            db_name = db_config["name"]
            
            # Create database
            try:
                client.sql.execute_query(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                created_resources.append({"type": "database", "name": db_name})
            except Exception as e:
                print(f"Warning: Could not create database {db_name}: {e}")
            
            # Process tables in this database
            if "tables" in db_config:
                for table_config in db_config["tables"]:
                    table_name = table_config["name"]
                    table_format = table_config.get("format", "delta")
                    full_table_name = f"{db_name}.{table_name}"
                    
                    try:
                        # Create table (basic structure)
                        if "data" in table_config and table_config["data"]:
                            # If data is provided, infer schema and create table
                            # This is a simplified approach - in practice you might want more sophisticated schema handling
                            pass  # Implementation would depend on specific data formats
                        
                        created_resources.append({
                            "type": "table", 
                            "database": db_name, 
                            "name": table_name,
                            "full_name": full_table_name,
                            "format": table_format
                        })
                    except Exception as e:
                        print(f"Warning: Could not create table {full_table_name}: {e}")
    
    # Process notebooks
    if "notebooks" in build_template:
        for notebook_config in build_template["notebooks"]:
            notebook_path = notebook_config["path"]
            content = notebook_config.get("content", "# Default notebook content")
            language = notebook_config.get("language", "python")
            
            try:
                # Upload notebook
                client.workspace.upload_notebook(
                    path=notebook_path,
                    content=content,
                    language=language,
                    format="SOURCE",
                    overwrite=True
                )
                created_resources.append({"type": "notebook", "path": notebook_path})
            except Exception as e:
                print(f"Warning: Could not create notebook {notebook_path}: {e}")
    
    # Process jobs
    if "jobs" in build_template:
        for job_config in build_template["jobs"]:
            job_name = job_config["name"]
            notebook_path = job_config.get("notebook_path")
            job_cluster_id = job_config.get("cluster_id", cluster_id)
            
            if notebook_path and job_cluster_id:
                try:
                    # Create job (simplified - real implementation would need full job specification)
                    job_spec = {
                        "name": job_name,
                        "existing_cluster_id": job_cluster_id,
                        "notebook_task": {"notebook_path": notebook_path}
                    }
                    # Note: This would need actual job creation implementation
                    created_resources.append({"type": "job", "name": job_name, "notebook_path": notebook_path})
                except Exception as e:
                    print(f"Warning: Could not create job {job_name}: {e}")
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: Databricks resource creation took {creation_end - creation_start:.2f}s")
    
    # Generate unique resource ID
    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    resource_id = build_template.get("resource_id", f"databricks_resource_{test_name}_{test_timestamp}_{test_uuid}")
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "databricks_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A Databricks resource for {test_name}",
        "status": "active",
        "created_resources": created_resources,
        "client": client,
        "config": config,
        "cluster_id": cluster_id,
        "cluster_created_by_us": cluster_created_by_us,
        "is_shared_cluster": is_shared_cluster,
        "cluster_config_hash": cluster_config_hash
    }
    
    print(f"Worker {os.getpid()}: Created Databricks resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: Databricks fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up Databricks resource {resource_id}")
    try:
        # Clean up created resources in reverse order
        for resource in reversed(created_resources):
            if resource["type"] == "table":
                try:
                    client.sql.execute_query(f"DROP TABLE IF EXISTS {resource['full_name']}")
                except Exception as e:
                    print(f"Warning: Could not drop table {resource['full_name']}: {e}")
            elif resource["type"] == "database":
                try:
                    client.sql.execute_query(f"DROP DATABASE IF EXISTS {resource['name']} CASCADE")
                except Exception as e:
                    print(f"Warning: Could not drop database {resource['name']}: {e}")
            elif resource["type"] == "notebook":
                try:
                    client.workspace.delete(resource["path"], recursive=False)
                except Exception as e:
                    print(f"Warning: Could not delete notebook {resource['path']}: {e}")
            elif resource["type"] == "cluster" and resource.get("created_by_us", False):
                # This handles non-shared clusters
                try:
                    client.clusters.delete_cluster(resource["cluster_id"])
                except Exception as e:
                    print(f"Warning: Could not delete cluster {resource['cluster_id']}: {e}")
        
        # Handle shared cluster cleanup with coordination
        if resource_data.get("is_shared_cluster", False) and resource_data.get("cluster_config_hash"):
            cleanup_shared_cluster(
                cluster_id=resource_data["cluster_id"],
                config_hash=resource_data["cluster_config_hash"],
                client=client
            )
        elif cluster_id and not resource_data.get("is_shared_cluster", False):
            # Handle non-shared cluster cleanup (fallback clusters, etc.)
            if cluster_created_by_us:
                try:
                    client.clusters.delete_cluster(cluster_id)
                    print(f"Worker {os.getpid()}: Deleted non-shared cluster {cluster_id}")
                except Exception as e:
                    print(f"Warning: Could not delete non-shared cluster {cluster_id}: {e}")
                    
        print(f"Worker {os.getpid()}: Databricks resource {resource_id} cleaned up successfully")
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up Databricks resource: {e}") 