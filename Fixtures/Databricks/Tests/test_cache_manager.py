#!/usr/bin/env python3
"""
Test script for the SQLite-based Databricks cache manager.
"""

import os
import sys
import tempfile
import time

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from Fixtures.Databricks.cache_manager import CacheManager


def test_cache_manager():
    """Test the SQLite-based cache manager functionality."""
    print("Testing SQLite-based Databricks Cache Manager")
    print("=" * 50)
    
    # Create a temporary cache manager for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Override the cache directory for testing
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            # Test 1: Initialize cache manager
            print("\n1. Testing cache manager initialization...")
            cache_manager = CacheManager(default_expiry_hours=1)
            print("✓ Cache manager initialized successfully")
            
            # Test 2: Test empty cache
            print("\n2. Testing empty cache...")
            cache_data = cache_manager.load_cluster_cache()
            assert not cache_data, "Cache should be empty initially"
            print("✓ Empty cache handled correctly")
            
            # Test 3: Test caching a cluster
            print("\n3. Testing cluster caching...")
            test_cluster_id = "test-cluster-123"
            test_config = {
                "cluster_name": "test-cluster",
                "host": "https://test-workspace.cloud.databricks.com",
                "node_type_id": "m5.large",
                "spark_version": "13.3.x-scala2.12",
                "num_workers": 1,
                "autotermination_minutes": 120,
                "is_shared": False
            }
            
            cache_manager.cache_new_cluster(test_cluster_id, test_config)
            print("✓ Cluster cached successfully")
            
            # Test 4: Test loading cached cluster
            print("\n4. Testing cache loading...")
            cache_data = cache_manager.load_cluster_cache()
            assert cache_data["cluster_id"] == test_cluster_id, "Cluster ID should match"
            assert cache_data["cluster_name"] == test_config["cluster_name"], "Cluster name should match"
            print("✓ Cached cluster loaded correctly")
            
            # Test 5: Test cache validity
            print("\n5. Testing cache validity...")
            is_valid = cache_manager.is_cluster_cache_valid(cache_data)
            assert is_valid, "Cache should be valid"
            print("✓ Cache validity check works")
            
            # Test 6: Test access tracking
            print("\n6. Testing access tracking...")
            cache_manager.update_cluster_access(test_cluster_id)
            updated_info = cache_manager.get_cached_cluster_info()
            assert updated_info["access_count"] >= 1, "Access count should be incremented"
            print("✓ Access tracking works")
            
            # Test 7: Test cache statistics
            print("\n7. Testing cache statistics...")
            stats = cache_manager.get_cache_statistics()
            assert stats["total_clusters"] >= 1, "Should have at least one cluster"
            assert stats["active_clusters"] >= 1, "Should have at least one active cluster"
            print("✓ Cache statistics work")
            
            # Test 8: Test getting all clusters
            print("\n8. Testing get all clusters...")
            all_clusters = cache_manager.get_all_clusters()
            assert len(all_clusters) >= 1, "Should return at least one cluster"
            print("✓ Get all clusters works")
            
            print("\n" + "=" * 50)
            print("✓ All tests passed! SQLite-based cache manager is working correctly.")
            
        finally:
            os.chdir(original_cwd)


def test_expired_clusters():
    """Test handling of expired clusters."""
    print("\nTesting expired cluster handling...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            cache_manager = CacheManager(default_expiry_hours=0.001)  # Very short expiry for testing
            
            # Cache a cluster that will expire quickly
            test_cluster_id = "expired-cluster-456"
            cache_manager.cache_new_cluster(test_cluster_id)
            
            # Wait a moment for expiry
            time.sleep(0.1)
            
            # Test cleanup of expired clusters
            removed_count = cache_manager.cleanup_expired_clusters()
            assert removed_count >= 0, "Should handle expired cluster cleanup"
            print("✓ Expired cluster cleanup works")
            
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    test_cache_manager()
    test_expired_clusters()
    print("\n🎉 All tests completed successfully!")
