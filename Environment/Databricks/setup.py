import os
import time
import json
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
from databricks_api import DatabricksAPI

EPHEMERAL_CACHE_FILE = os.path.join(os.path.dirname(__file__), "ephemeral.json")
DEFAULT_EXPIRY_HOURS = 1

def load_cluster_cache() -> Dict[str, Any]:
    """Load cluster cache from ephemeral.json"""
    if not os.path.exists(EPHEMERAL_CACHE_FILE):
        return {}
    
    try:
        with open(EPHEMERAL_CACHE_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

def save_cluster_cache(cache_data: Dict[str, Any]) -> None:
    """Save cluster cache to ephemeral.json"""
    os.makedirs(os.path.dirname(EPHEMERAL_CACHE_FILE), exist_ok=True)
    
    try:
        with open(EPHEMERAL_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except IOError as e:
        print(f"Warning: Could not save cluster cache: {e}")

def is_cluster_cache_valid(cache_data: Dict[str, Any]) -> bool:
    """Check if cached cluster data is still valid"""
    if not cache_data or "cluster_id" not in cache_data or "expiry_time" not in cache_data:
        return False
    
    expiry_time = datetime.fromisoformat(cache_data["expiry_time"])
    return datetime.now() < expiry_time

def create_test_cluster(client: DatabricksAPI, cluster_name: str = "de-bench-hello-world-cluster") -> str:
    """Create a test cluster for the Hello World test"""
    cluster_config = {
        "cluster_name": cluster_name,
        "spark_version": "13.3.x-scala2.12",  # Latest LTS version
        "node_type_id": "m5.large",  # Small, supported instance type
        "num_workers": 0,  # Single node cluster to minimize cost
        "autotermination_minutes": 120,  # Auto-terminate after 2 hours (longer than our cache)
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
            "cached": "true"
        }
    }
    
    print(f"Creating test cluster: {cluster_name}")
    response = client.cluster.create_cluster(**cluster_config)
    cluster_id = response["cluster_id"]
    
    # Wait for cluster to start
    print(f"Waiting for cluster {cluster_id} to start...")
    max_wait = 600  # 10 minutes timeout
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        cluster_info = client.cluster.get_cluster(cluster_id)
        state = cluster_info["state"]

        print(f"Cluster {cluster_id} is in state: {state}")
        
        if state == "RUNNING":
            print(f"Cluster {cluster_id} is now running")
            return cluster_id
        elif state in ["ERROR", "TERMINATED"]:
            raise Exception(f"Cluster failed to start. State: {state}")
        
        time.sleep(5)
    
    raise Exception(f"Cluster {cluster_id} failed to start within {max_wait} seconds")

def get_or_create_cluster(client: DatabricksAPI, config: Dict[str, Any], timeout: int = 600) -> Tuple[str, bool]:
    """Get existing cluster or create a new one if needed, with caching support"""
    cluster_id = config.get("cluster_id")
    
    # Check cache first
    cache_data = load_cluster_cache()
    if cache_data and not is_cluster_cache_valid(cache_data):
        print(f"Found expired cached cluster {cache_data.get('cluster_id', 'unknown')}, clearing cache")
        clear_cluster_cache()
        cache_data = {}
    
    if is_cluster_cache_valid(cache_data):
        cached_cluster_id = cache_data["cluster_id"]
        print(f"Found valid cached cluster: {cached_cluster_id}")
        
        # Verify the cached cluster is still available and running
        try:
            cluster_info = client.cluster.get_cluster(cached_cluster_id)
            state = cluster_info["state"]
            
            if state == "RUNNING":
                print(f"Using cached running cluster: {cached_cluster_id}")
                return cached_cluster_id, False
            elif state == "TERMINATED":
                print(f"Cached cluster {cached_cluster_id} is terminated, creating new one")
                # Clear the cache since this cluster is terminated
                clear_cluster_cache()
            else:
                print(f"Cached cluster {cached_cluster_id} is in state {state}, creating new one")
                # Clear the cache since this cluster is in an unexpected state
                clear_cluster_cache()
        except Exception as e:
            print(f"Error with cached cluster {cached_cluster_id}: {e}. Creating new cluster.")
            # Clear the cache since this cluster is problematic
            clear_cluster_cache()
    
    # If cluster_id is provided in config, try to use it
    if cluster_id:
        try:
            cluster_info = client.cluster.get_cluster(cluster_id)
            state = cluster_info["state"]
            
            if state == "RUNNING":
                print(f"Using existing running cluster: {cluster_id}")
                # Cache this cluster for future use
                cache_new_cluster(cluster_id)
                return cluster_id, False  # False = not created by us
            elif state == "TERMINATED":
                print(f"Existing cluster {cluster_id} is terminated, creating new cluster")
            else:
                print(f"Cluster {cluster_id} is in state {state}, creating new cluster")
                
        except Exception as e:
            print(f"Error with cluster {cluster_id}: {e}. Creating new cluster.")
    
    # Create a new cluster and cache it
    print("Creating new test cluster")
    new_cluster_id = create_test_cluster(client)
    cache_new_cluster(new_cluster_id)
    return new_cluster_id, True  # True = created by us

def cache_new_cluster(cluster_id: str, expiry_hours: int = DEFAULT_EXPIRY_HOURS) -> None:
    """Cache a new cluster with expiry time"""
    expiry_time = datetime.now() + timedelta(hours=expiry_hours)
    cache_data = {
        "cluster_id": cluster_id,
        "expiry_time": expiry_time.isoformat(),
        "created_at": datetime.now().isoformat()
    }
    save_cluster_cache(cache_data)
    print(f"Cached cluster {cluster_id} until {expiry_time}")

def setup_databricks_environment(client: DatabricksAPI, config: Dict[str, Any], cluster_id: str) -> None:
    """Set up the Databricks environment for Hello World test"""
    # Update config with the cluster_id we're using
    config["cluster_id"] = cluster_id
    
    # Ensure output directory is clean
    try:
        client.dbfs.delete(
            path=config["delta_table_path"].replace("dbfs:", ""),
            recursive=True
        )
        print(f"Cleaned up existing output directory: {config['delta_table_path']}")
    except Exception as e:
        if "not found" not in str(e).lower():
            print(f"Warning during cleanup: {e}")

def cleanup_databricks_environment(client: DatabricksAPI, config: Dict[str, Any], cluster_created_by_us: bool = False) -> None:
    """Clean up the Databricks environment after testing"""
    print("Starting cleanup...")
    
    # 1. Try to drop the table if we have SQL capabilities
    try:
        from .validation import extract_warehouse_id_from_http_path, execute_sql_query
        
        warehouse_id = extract_warehouse_id_from_http_path(config.get("http_path", ""))
        if warehouse_id:
            host = config["host"]
            token = config["token"]
            catalog = config["catalog"]
            schema = config["schema"]
            table = config["table"]
            
            drop_table_query = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"
            result = execute_sql_query(host, token, warehouse_id, drop_table_query, catalog, schema)
            
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
        client.dbfs.delete(
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

def clear_cluster_cache() -> None:
    """Clear the cluster cache (for testing or manual cleanup)"""
    if os.path.exists(EPHEMERAL_CACHE_FILE):
        os.remove(EPHEMERAL_CACHE_FILE)
        print("Cluster cache cleared")
    else:
        print("No cluster cache to clear")

def get_cached_cluster_info() -> Optional[Dict[str, Any]]:
    """Get information about the currently cached cluster"""
    cache_data = load_cluster_cache()
    if not cache_data:
        return None
    
    return {
        "cluster_id": cache_data.get("cluster_id"),
        "expiry_time": cache_data.get("expiry_time"),
        "created_at": cache_data.get("created_at"),
        "is_valid": is_cluster_cache_valid(cache_data)
    } 