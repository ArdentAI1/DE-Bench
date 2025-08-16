# Databricks SQLite Cache Manager

This document describes the SQLite-based cache manager for Databricks clusters, which replaces the previous JSON-based caching system.

## Overview

The SQLite-based cache manager provides improved data integrity, atomic operations, and better performance for storing Databricks cluster information. It maintains backward compatibility with the existing API while adding new features.

## Key Improvements

### 1. **Data Integrity**
- **Atomic Operations**: SQLite transactions ensure data consistency
- **ACID Compliance**: Full ACID properties for reliable data storage
- **Indexed Queries**: Fast lookups with proper database indexing

### 2. **Enhanced Features**
- **Access Tracking**: Monitor cluster usage with access counts and timestamps
- **Multiple Clusters**: Support for storing multiple cluster configurations
- **Expiry Management**: Automatic cleanup of expired clusters
- **Statistics**: Comprehensive cache statistics and monitoring

### 3. **Better Performance**
- **Indexed Lookups**: Fast queries with database indexes
- **Efficient Storage**: Optimized data storage compared to JSON
- **Connection Pooling**: Reusable database connections

### 4. **Concurrent Access** ✅ **NEW**
- **WAL Mode**: Write-Ahead Logging for better concurrent access
- **Retry Mechanism**: Automatic retry with exponential backoff for lock situations
- **Timeout Configuration**: 30-second timeout prevents indefinite waiting
- **Cross-thread Support**: Proper multi-threaded access support
- **Lock Resolution**: Database lock errors are automatically handled

### 5. **Shared Cluster Coordination** ✅ **NEW**
- **SQLite-based Registry**: Persistent shared cluster coordination across processes
- **Atomic Operations**: Transaction-based coordination prevents race conditions
- **Usage Tracking**: Automatic usage count management for shared clusters
- **Status Management**: Real-time status updates (creating, ready, failed)
- **Cross-process Safety**: Coordination works across multiple processes and threads

## Database Schema

The cache manager creates a SQLite database with the following structure:

```sql
CREATE TABLE clusters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id TEXT UNIQUE NOT NULL,
    cluster_name TEXT,
    host TEXT,
    num_workers INTEGER,
    status TEXT DEFAULT 'RUNNING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_time TIMESTAMP NOT NULL,
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    access_count INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT 1,
    is_shared BOOLEAN DEFAULT 0
);

CREATE TABLE shared_cluster_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_hash TEXT UNIQUE NOT NULL,
    cluster_id TEXT,
    status TEXT DEFAULT 'creating',
    worker_pid INTEGER,
    creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (cluster_id) REFERENCES clusters(cluster_id)
);

-- Indexes for performance
CREATE INDEX idx_cluster_id ON clusters(cluster_id);
CREATE INDEX idx_expiry_time ON clusters(expiry_time);
CREATE INDEX idx_is_active ON clusters(is_active);
CREATE INDEX idx_config_hash ON shared_cluster_registry(config_hash);
CREATE INDEX idx_registry_status ON shared_cluster_registry(status);
CREATE INDEX idx_registry_usage_count ON shared_cluster_registry(usage_count);
```

## Usage

### Basic Usage

```python
from Fixtures.Databricks.cache_manager import CacheManager

# Initialize cache manager
cache_manager = CacheManager(default_expiry_hours=1)

# Cache a new cluster
cluster_config = {
    "cluster_name": "test-cluster",
    "cluster_name": "test_name",
    "host": "https://workspace.cloud.databricks.com",
    "node_type_id": "m5.large",
    "spark_version": "13.3.x-scala2.12",
    "num_workers": 1,
    "created_at": "2025-06-09T13:37:23.420000"
    "autotermination_minutes": 120,
    "is_shared": False
}
cache_manager.cache_new_cluster("cluster-123", cluster_config)

# Load cached cluster
cache_data = cache_manager.load_cluster_cache()
if cache_data:
    print(f"Found cached cluster: {cache_data['cluster_id']}")

# Check if cache is valid
is_valid = cache_manager.is_cluster_cache_valid(cache_data)
```

### Advanced Features

```python
# Update access tracking
cache_manager.update_cluster_access("cluster-123")

# Get cache statistics
stats = cache_manager.get_cache_statistics()
print(f"Active clusters: {stats['active_clusters']}")
print(f"Total accesses: {stats['total_accesses']}")

# Get all cached clusters
all_clusters = cache_manager.get_all_clusters()
for cluster in all_clusters:
    print(f"Cluster {cluster['cluster_id']}: {cluster['cluster_name']}")

# Clean up expired clusters
removed_count = cache_manager.cleanup_expired_clusters()
print(f"Removed {removed_count} expired clusters")

# Clear all cache
cache_manager.clear_cluster_cache()
```

### Shared Cluster Coordination

```python
# Register shared cluster creation
success = cache_manager.register_shared_cluster_creation("config_hash_123", os.getpid())

# Check if we can join existing creation
can_join = cache_manager.can_join_shared_cluster_creation("config_hash_123")

# Update cluster status
cache_manager.update_shared_cluster_status("config_hash_123", "ready", cluster_id="cluster-456")

# Get shared cluster information
info = cache_manager.get_shared_cluster_info("config_hash_123")

# Manage usage counts
new_count = cache_manager.increment_shared_cluster_usage("config_hash_123")
remaining_count = cache_manager.decrement_shared_cluster_usage("config_hash_123")

# Cleanup shared cluster registry
success = cache_manager.cleanup_shared_cluster_registry("config_hash_123")

# Get all shared clusters
all_shared = cache_manager.get_all_shared_clusters()
```

## CLI Commands

The Databricks CLI provides commands for managing the cache:

```bash
# Show cache status and statistics
python Environment/Databricks/cli.py status

# Clear all cluster cache
python Environment/Databricks/cli.py clear

# Clean up expired clusters
python Environment/Databricks/cli.py cleanup

# Show all cached clusters
python Environment/Databricks/cli.py list

# Optimize database performance
python Environment/Databricks/cli.py optimize

# Show database file information
python Environment/Databricks/cli.py dbinfo

# Show shared cluster registry
python Environment/Databricks/cli.py shared

# Clean up shared cluster registry
python Environment/Databricks/cli.py cleanup-shared
```

### Example CLI Output

```bash
$ python Environment/Databricks/cli.py status

=== Cache Statistics ===
  Total Clusters: 5
  Active Clusters: 1
  Expired Clusters: 4
  Total Accesses: 12
  Shared Clusters: 2
  Cache File Size: 8192 bytes

=== Active Cached Cluster ===
  Cluster ID: cluster-123
  Created At: 2024-01-15T10:30:00
  Expires At: 2024-01-15T11:30:00
  Is Valid: Yes
  Access Count: 3
  Is Shared: Yes
  Last Accessed: 2024-01-15T10:45:00
  Time Remaining: 45 minutes
```

## Migration from JSON

The SQLite cache manager is **backward compatible** with the existing API. No changes are required to existing code that uses the cache manager.

### Automatic Migration
- The cache manager automatically creates the SQLite database on first use
- Existing JSON cache files are ignored (they remain but are not used)
- New clusters are stored in the SQLite database

### Manual Migration
If you want to manually migrate or clean up:

```bash
# Clear old JSON cache (optional)
rm Environment/Databricks/ephemeral.json

# Clear new SQLite cache
python Environment/Databricks/cli.py clear
```

## Configuration

### Environment Variables
The cache manager uses the same environment variables as before:
- `DATABRICKS_HOST`: Databricks workspace host
- `DATABRICKS_TOKEN`: Databricks access token
- `DATABRICKS_CLUSTER_ID`: Default cluster ID (optional)

### Cache Settings
- **Default Expiry**: 1 hour (configurable via `default_expiry_hours`)
- **Database Location**: `Environment/Databricks/cluster_cache.db`
- **Auto-cleanup**: Expired clusters are automatically filtered out

## Testing

Run the test script to verify the cache manager functionality:

```bash
python Fixtures/Databricks/test_cache_manager.py
```

This will run comprehensive tests covering:
- Cache initialization
- Cluster caching and retrieval
- Access tracking
- Statistics generation
- Expired cluster cleanup
- Cache clearing

## Troubleshooting

### Common Issues

1. **Database Lock Errors** ✅ **RESOLVED**
   - **Fixed**: The cache manager now uses WAL mode and proper connection configuration
   - **Retry Mechanism**: Automatic retry with exponential backoff for lock situations
   - **Timeout**: 30-second timeout prevents indefinite waiting
   - **Cross-thread Support**: `check_same_thread=False` allows multi-threaded access

2. **Cache Not Found**
   - Verify the cache directory exists: `Environment/Databricks/`
   - Check file permissions on the database file

3. **Performance Issues**
   - The database is automatically indexed for optimal performance
   - Large numbers of expired clusters can be cleaned up with the `cleanup` command
   - Use the `optimize` command to maintain database performance

4. **WAL File Management**
   - WAL files are automatically managed by SQLite
   - Use the `optimize` command to clean up WAL files periodically
   - Monitor WAL file size with the `dbinfo` command

### Debug Information

Enable debug output by setting the log level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Manual Database Inspection

You can inspect the SQLite database directly:

```bash
sqlite3 Environment/Databricks/cluster_cache.db
.tables
SELECT * FROM clusters;
.quit
```

## Benefits Over JSON

| Feature | JSON Cache | SQLite Cache |
|---------|------------|--------------|
| **Data Integrity** | ❌ No transactions | ✅ ACID compliance |
| **Concurrent Access** | ❌ File locking issues | ✅ Proper locking |
| **Query Performance** | ❌ Linear search | ✅ Indexed queries |
| **Access Tracking** | ❌ Not available | ✅ Built-in tracking |
| **Statistics** | ❌ Manual calculation | ✅ Automatic stats |
| **Expiry Management** | ❌ Basic | ✅ Advanced cleanup |
| **Multiple Clusters** | ❌ Single cluster only | ✅ Multiple clusters |

## Future Enhancements

Potential improvements for future versions:
- **Connection Pooling**: Optimize database connections
- **Compression**: Compress cluster configuration data
- **Backup/Restore**: Database backup and restore functionality
- **Metrics Export**: Export cache statistics to monitoring systems
- **Cluster History**: Maintain historical cluster information
