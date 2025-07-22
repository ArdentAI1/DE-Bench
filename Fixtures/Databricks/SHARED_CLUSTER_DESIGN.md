# Shared Cluster System Documentation

## Overview

The Databricks shared cluster system allows multiple tests to efficiently share a single cluster when they have compatible configurations. When tests specify `"use_shared_cluster": True` in their `databricks_resource` parameter, the system automatically coordinates cluster creation and usage to ensure:

- **Resource efficiency**: Only ONE cluster per unique configuration
- **Thread safety**: Multiple tests can run in parallel safely
- **Automatic coordination**: Tests wait for cluster creation rather than creating duplicates
- **Smart cleanup**: Clusters are only deleted when no tests are using them

## How to Use Shared Clusters

Add the `use_shared_cluster` flag to your test's `databricks_resource` parameter:

```python
@pytest.mark.parametrize("databricks_resource", [
    {
        "resource_id": "my_test",
        "use_shared_cluster": True,
        "cluster_fallback": True,           # Optional: create fallback on failure
        "shared_cluster_timeout": 1200      # Optional: wait time in seconds
    }
], indirect=True)
def test_my_databricks_feature(databricks_resource):
    cluster_id = databricks_resource["cluster_id"]
    client = databricks_resource["client"]
    # ... test logic
```

## Configuration Parameters

### Required Parameters
- `"use_shared_cluster": True` - Enables shared cluster mode

### Optional Parameters
- `"cluster_fallback": True` (default) - Creates individual cluster if shared cluster fails
- `"shared_cluster_timeout": 1200` (default 300) - Seconds to wait for shared cluster creation

## How It Works Under the Hood

### 1. Configuration-Based Cluster Sharing

The system determines cluster compatibility using a configuration hash that includes only cluster-relevant settings:

```python
# These fields determine if tests can share a cluster:
cluster_relevant_config = {
    "host": config.get("host", ""),
    "cluster_name": config.get("cluster_name", ""),
    "node_type_id": config.get("node_type_id", ""),
    "spark_version": config.get("spark_version", ""),
    "num_workers": config.get("num_workers", 1),
    "autotermination_minutes": config.get("autotermination_minutes", 120),
    "spark_conf": config.get("spark_conf", {}),
    "aws_attributes": config.get("aws_attributes", {})
}
# Excluded: resource_id, test_id, unique_message, delta_table_path
```

Tests with identical cluster configurations will share the same cluster, regardless of their individual `resource_id` or test-specific data.

### 2. Thread-Safe Coordination

The system uses a global registry with thread-safe coordination:

```python
# Global state tracking
SHARED_CLUSTERS = {}      # ClusterInfo objects by config hash
CLUSTER_LOCKS = {}        # Per-config locks for coordination  
CLUSTER_USAGE_COUNT = {}  # Reference counting for cleanup
REGISTRY_LOCK = Lock()    # Protects the registries themselves
```

### 3. Coordination Flow

When a test requests a shared cluster, here's what happens:

1. **Configuration Hashing**: The test's Databricks config is hashed to create a unique cluster identifier
2. **Lock Acquisition**: A per-config lock ensures only one thread can modify cluster state at a time
3. **Status Check**: The system checks if a cluster for this config already exists:
   - **Ready**: Increment usage count and return existing cluster
   - **Creating**: Release lock and wait for the creating thread to finish
   - **Failed**: Clean up and attempt new creation
   - **Not Found**: Become the creator thread

4. **Cluster Creation**: The "winner" thread creates the cluster while others wait
5. **Usage Tracking**: Reference counting tracks how many tests are using each cluster

```python
# Simplified flow example:
def request_shared_cluster(client_info):
    config_hash = get_cluster_config_hash(client_info["config"])
    
    with per_config_lock(config_hash):
        if cluster_exists_and_ready(config_hash):
            increment_usage_count(config_hash)
            return existing_cluster_info
        elif cluster_being_created(config_hash):
            # Release lock and wait outside
            pass
    
    # Either wait for creation or become the creator
    if someone_else_creating:
        return wait_for_cluster_ready(config_hash)
    else:
        return create_new_cluster(config_hash, client_info)
```

### 4. Wait and Fallback Behavior

**Waiting Process**: When a test finds another thread creating a cluster:
- Polls every 5 seconds for cluster readiness
- Times out after configured duration (default 300s, configurable up to 1200s)
- Logs progress with worker PIDs for debugging

**Fallback Options**: When shared cluster creation fails or times out:
- `"cluster_fallback": True` (default): Creates an individual cluster for this test
- `"cluster_fallback": False`: Raises an exception and fails the test

```python
# Example timeout behavior:
Worker 12345: Requesting shared cluster (config_hash: a1b2c3d4...)
Worker 12345: Another worker (67890) is creating shared cluster
Worker 12345: Waiting for shared cluster creation (timeout=1200s)
# ... polling every 5s ...
Worker 12345: Shared cluster cluster-abc123 is now ready
```

### 5. Automatic Cleanup

**Reference Counting**: Each shared cluster tracks how many tests are using it:
- **Test starts**: Usage count increments
- **Test ends**: Usage count decrements  
- **Count reaches zero**: Cluster is deleted (if created by our test session)

**Smart Deletion**: Only the session that created the cluster can delete it:
- Prevents accidental deletion by tests that just reused an existing cluster
- Handles edge cases where multiple test sessions might overlap

```python
# Cleanup logging example:
Worker 12345: Cleaning up shared cluster cluster-abc123 (config_hash: a1b2c3d4...)
Worker 12345: Shared cluster cluster-abc123 still in use by 2 other test(s)
# ... later when last test finishes ...
Worker 67890: Deleting shared cluster cluster-abc123 (last user)
Worker 67890: Successfully deleted shared cluster cluster-abc123
```

## Resource Data Structure

When using shared clusters, the `databricks_resource` fixture returns additional metadata:

```python
resource_data = {
    "resource_id": "unique_test_id",
    "cluster_id": "cluster-abc123",
    "client": databricks_client,
    "config": {...},
    "cluster_created_by_us": False,      # True only for the creating thread
    "is_shared_cluster": True,           # Indicates this is a shared cluster
    "cluster_config_hash": "a1b2c3d4...", # Used for coordination
    # ... other standard fields
}
```

## Example Usage Patterns

### Basic Shared Cluster
```python
@pytest.mark.parametrize("databricks_resource", [
    {"resource_id": "basic_test", "use_shared_cluster": True}
], indirect=True)
def test_basic_feature(databricks_resource):
    # Automatically gets shared cluster or waits for one
    pass
```

### Custom Timeout with Fallback Disabled
```python
@pytest.mark.parametrize("databricks_resource", [
    {
        "resource_id": "critical_test", 
        "use_shared_cluster": True,
        "shared_cluster_timeout": 600,  # Wait up to 10 minutes
        "cluster_fallback": False       # Fail if shared cluster unavailable
    }
], indirect=True)
def test_requires_shared_cluster(databricks_resource):
    # Will fail rather than create individual cluster
    pass
```

## Benefits of the System

### **Resource Efficiency**
- Only one cluster created per unique configuration across all tests
- Significant cost savings when running multiple tests with similar requirements
- Faster test execution (no waiting for individual cluster creation)

### **Robust Coordination**
- Thread-safe operation with proper locking mechanisms
- Reference counting prevents premature cluster deletion
- Detailed logging helps debug coordination issues

### **Flexible Configuration**
- Configurable timeouts for different test requirements
- Fallback options balance reliability with resource efficiency
- Compatible with existing test patterns

### **Production Ready**
- Handles edge cases like creation failures and timeouts
- Works with parallel test execution (pytest-xdist compatible within single process)
- Automatic cleanup prevents resource leaks

## Current Limitations

### **Single Process Only**
- Thread-safe within one Python process
- Multiple pytest-xdist workers (separate processes) will create separate clusters
- Future enhancement needed for cross-process coordination

### **Session Scope**
- Clusters are shared within a test session but not across sessions
- Each pytest run starts fresh (no persistent cluster reuse)
- Good for test isolation but limits sharing opportunities

## Monitoring and Debugging

### **Log Output**
The system provides detailed logging to help understand cluster coordination:

```
Worker 12345: Requesting shared cluster (config_hash: a1b2c3d4..., timeout=1200s)
Worker 12345: Creating new shared cluster (config_hash: a1b2c3d4...)
Worker 12345: Successfully created shared cluster cluster-abc123
Worker 67890: Requesting shared cluster (config_hash: a1b2c3d4..., timeout=1200s)
Worker 67890: Reusing existing shared cluster cluster-abc123
```

### **Configuration Hash Debugging**
- Config hashes are shown in logs (first 8 characters)
- Tests with identical hashes will share clusters
- Different hashes indicate configuration differences preventing sharing

## Future Enhancements

Potential improvements for future versions:

1. **Cross-Process Coordination**: File-based locks for pytest-xdist support
2. **Persistent Clusters**: Reuse clusters across test sessions
3. **Cluster Pools**: Integration with Databricks cluster pools
4. **Usage Metrics**: Monitoring of sharing efficiency and cost savings
5. **Smart Scheduling**: Optimize test order to maximize cluster sharing