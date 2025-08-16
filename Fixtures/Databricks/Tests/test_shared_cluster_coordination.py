#!/usr/bin/env python3
"""
Test script for SQLite-based shared cluster coordination.
"""

import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from Fixtures.Databricks.cache_manager import CacheManager


def test_shared_cluster_registration():
    """Test shared cluster registration and coordination."""
    print("Testing shared cluster registration...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            # Test 1: Register a shared cluster creation
            config_hash = "test_config_hash_123"
            worker_pid = os.getpid()
            
            success = cache_manager.register_shared_cluster_creation(config_hash, worker_pid)
            assert success, "Should successfully register shared cluster creation"
            print("✓ Shared cluster registration successful")
            
            # Test 2: Try to register the same config hash again
            success2 = cache_manager.register_shared_cluster_creation(config_hash, worker_pid + 1)
            assert success2, "Should allow joining existing creation"
            print("✓ Can join existing creation")
            
            # Test 3: Check if we can join existing creation
            can_join = cache_manager.can_join_shared_cluster_creation(config_hash)
            assert can_join, "Should be able to join existing creation"
            print("✓ Can join existing creation")
            
            # Test 4: Update status to ready
            cluster_id = "test-cluster-456"
            cache_manager.update_shared_cluster_status(config_hash, "ready", cluster_id=cluster_id)
            
            # Test 5: Get shared cluster info
            info = cache_manager.get_shared_cluster_info(config_hash)
            assert info is not None, "Should get shared cluster info"
            assert info["status"] == "ready", "Status should be ready"
            assert info["cluster_id"] == cluster_id, "Cluster ID should match"
            print("✓ Shared cluster status update successful")
            
            # Test 6: Increment usage count
            new_count = cache_manager.increment_shared_cluster_usage(config_hash)
            assert new_count == 2, "Usage count should be 2"
            print("✓ Usage count increment successful")
            
            # Test 7: Decrement usage count
            new_count = cache_manager.decrement_shared_cluster_usage(config_hash)
            assert new_count == 1, "Usage count should be 1"
            print("✓ Usage count decrement successful")
            
            # Test 8: Cleanup
            success = cache_manager.cleanup_shared_cluster_registry(config_hash)
            assert success, "Should successfully cleanup registry"
            print("✓ Registry cleanup successful")
            
        finally:
            os.chdir(original_cwd)


def test_concurrent_shared_cluster_creation():
    """Test concurrent shared cluster creation coordination."""
    print("Testing concurrent shared cluster creation...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            def create_shared_cluster(worker_id: int):
                """Simulate shared cluster creation."""
                config_hash = f"concurrent_config_{worker_id}"
                worker_pid = os.getpid() + worker_id
                
                # Try to register
                success = cache_manager.register_shared_cluster_creation(config_hash, worker_pid)
                
                if success:
                    # Simulate cluster creation
                    time.sleep(0.1)
                    cluster_id = f"cluster-{worker_id}"
                    cache_manager.update_shared_cluster_status(config_hash, "ready", cluster_id=cluster_id)
                    return f"Worker {worker_id}: Created cluster {cluster_id}"
                else:
                    # Wait for existing creation
                    time.sleep(0.2)
                    info = cache_manager.get_shared_cluster_info(config_hash)
                    if info and info["status"] == "ready":
                        return f"Worker {worker_id}: Joined existing cluster {info['cluster_id']}"
                    else:
                        return f"Worker {worker_id}: Failed to join"
            
            # Test concurrent creation
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(create_shared_cluster, i) for i in range(5)]
                
                for future in as_completed(futures):
                    result = future.result()
                    print(f"  {result}")
            
            # Check final state
            all_shared = cache_manager.get_all_shared_clusters()
            print(f"  Total shared clusters in registry: {len(all_shared)}")
            
        finally:
            os.chdir(original_cwd)


def test_shared_cluster_cleanup():
    """Test shared cluster cleanup scenarios."""
    print("Testing shared cluster cleanup...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=1)
            
            # Create multiple shared clusters
            for i in range(3):
                config_hash = f"cleanup_test_{i}"
                worker_pid = os.getpid() + i
                
                cache_manager.register_shared_cluster_creation(config_hash, worker_pid)
                cache_manager.update_shared_cluster_status(
                    config_hash, "ready", cluster_id=f"cluster-{i}"
                )
                
                # Set different usage counts
                for _ in range(i + 1):
                    cache_manager.increment_shared_cluster_usage(config_hash)
            
            # Check initial state
            all_shared = cache_manager.get_all_shared_clusters()
            print(f"  Initial shared clusters: {len(all_shared)}")
            
            # Test cleanup of specific cluster
            success = cache_manager.cleanup_shared_cluster_registry("cleanup_test_1")
            assert success, "Should cleanup specific cluster"
            print("✓ Specific cluster cleanup successful")
            
            # Check remaining clusters
            remaining = cache_manager.get_all_shared_clusters()
            print(f"  Remaining shared clusters: {len(remaining)}")
            
            # Test cleanup of all clusters
            for cluster in remaining:
                cache_manager.cleanup_shared_cluster_registry(cluster["config_hash"])
            
            final = cache_manager.get_all_shared_clusters()
            assert len(final) == 0, "All clusters should be cleaned up"
            print("✓ All clusters cleanup successful")
            
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    print("Testing SQLite-based Shared Cluster Coordination")
    print("=" * 55)
    
    test_shared_cluster_registration()
    print()
    
    test_concurrent_shared_cluster_creation()
    print()
    
    test_shared_cluster_cleanup()
    print()
    
    print("🎉 All shared cluster coordination tests completed!")
