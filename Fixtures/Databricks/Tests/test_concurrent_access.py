#!/usr/bin/env python3
"""
Test script for concurrent access to the SQLite-based Databricks cache manager.
"""

import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from Fixtures.Databricks.cache_manager import CacheManager


def test_concurrent_writes():
    """Test concurrent write operations to the cache."""
    print("Testing concurrent write operations...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            def write_cluster(cluster_id: int):
                """Write a cluster to the cache."""
                cluster_config = {
                    "cluster_id": f"test-cluster-{cluster_id}",
                    "cluster_name": f"test-cluster-{cluster_id}",
                    "host": "https://test-workspace.cloud.databricks.com",
                    "num_workers": 1,
                    "status": "RUNNING",
                    "is_shared": False
                }
                
                try:
                    cache_manager.save_cluster_cache(cluster_config)
                    return f"Successfully wrote cluster {cluster_id}"
                except Exception as e:
                    return f"Failed to write cluster {cluster_id}: {e}"
            
            # Test concurrent writes
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(write_cluster, i) for i in range(10)]
                
                for future in as_completed(futures):
                    result = future.result()
                    print(f"  {result}")
            
            # Verify final state
            final_cache = cache_manager.load_cluster_cache()
            print(f"  Final cache state: {final_cache.get('cluster_id', 'None')}")
            
        finally:
            os.chdir(original_cwd)


def test_concurrent_reads():
    """Test concurrent read operations from the cache."""
    print("Testing concurrent read operations...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            # First, write a cluster
            cluster_config = {
                "cluster_id": "test-cluster-read",
                "cluster_name": "test-cluster-read",
                "host": "https://test-workspace.cloud.databricks.com",
                "num_workers": 1,
                "status": "RUNNING",
                "is_shared": False
            }
            cache_manager.save_cluster_cache(cluster_config)
            
            def read_cluster(thread_id: int):
                """Read from the cache."""
                try:
                    cache_data = cache_manager.load_cluster_cache()
                    return f"Thread {thread_id}: Read cluster {cache_data.get('cluster_id', 'None')}"
                except Exception as e:
                    return f"Thread {thread_id}: Failed to read: {e}"
            
            # Test concurrent reads
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(read_cluster, i) for i in range(20)]
                
                for future in as_completed(futures):
                    result = future.result()
                    print(f"  {result}")
            
        finally:
            os.chdir(original_cwd)


def test_mixed_operations():
    """Test mixed read and write operations."""
    print("Testing mixed read and write operations...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            def mixed_operation(thread_id: int):
                """Perform mixed read/write operations."""
                try:
                    # Read current state
                    cache_data = cache_manager.load_cluster_cache()
                    
                    # Write new cluster
                    cluster_config = {
                        "cluster_id": f"mixed-cluster-{thread_id}",
                        "cluster_name": f"mixed-cluster-{thread_id}",
                        "host": "https://test-workspace.cloud.databricks.com",
                        "num_workers": 1,
                        "status": "RUNNING",
                        "is_shared": False
                    }
                    cache_manager.save_cluster_cache(cluster_config)
                    
                    # Read again
                    new_cache_data = cache_manager.load_cluster_cache()
                    
                    return f"Thread {thread_id}: Success - wrote {cluster_config['cluster_id']}, read {new_cache_data.get('cluster_id', 'None')}"
                except Exception as e:
                    return f"Thread {thread_id}: Failed: {e}"
            
            # Test mixed operations
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(mixed_operation, i) for i in range(5)]
                
                for future in as_completed(futures):
                    result = future.result()
                    print(f"  {result}")
            
        finally:
            os.chdir(original_cwd)


def test_database_optimization():
    """Test database optimization features."""
    print("Testing database optimization...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            # Write some test data
            for i in range(5):
                cluster_config = {
                    "cluster_id": f"opt-cluster-{i}",
                    "cluster_name": f"opt-cluster-{i}",
                    "host": "https://test-workspace.cloud.databricks.com",
                    "num_workers": 1,
                    "status": "RUNNING",
                    "is_shared": False
                }
                cache_manager.save_cluster_cache(cluster_config)
            
            # Get database info before optimization
            info_before = cache_manager.get_database_info()
            print(f"  Database size before optimization: {info_before['database_size']} bytes")
            
            # Optimize database
            cache_manager.optimize_database()
            
            # Get database info after optimization
            info_after = cache_manager.get_database_info()
            print(f"  Database size after optimization: {info_after['database_size']} bytes")
            
            # Get statistics
            stats = cache_manager.get_cache_statistics()
            print(f"  Total clusters: {stats['total_clusters']}")
            print(f"  Active clusters: {stats['active_clusters']}")
            
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    print("Testing SQLite Cache Manager Concurrent Access")
    print("=" * 50)
    
    test_concurrent_writes()
    print()
    
    test_concurrent_reads()
    print()
    
    test_mixed_operations()
    print()
    
    test_database_optimization()
    print()
    
    print("🎉 All concurrent access tests completed!")
