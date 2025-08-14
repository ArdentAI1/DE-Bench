#!/usr/bin/env python3
"""
Databricks Environment Management CLI

This script provides command-line utilities for managing the Databricks environment,
including cluster cache management.
"""

import argparse
import os
import sys
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from Fixtures.Databricks.cache_manager import CacheManager


def show_cache_status():
    """Show current cache status"""
    cache_manager = CacheManager()
    info = cache_manager.get_cached_cluster_info()
    stats = cache_manager.get_cache_statistics()

    print("=== Cache Statistics ===")
    print(f"  Total Clusters: {stats.get('total_clusters', 0)}")
    print(f"  Active Clusters: {stats.get('active_clusters', 0)}")
    print(f"  Expired Clusters: {stats.get('expired_clusters', 0)}")
    print(f"  Total Accesses: {stats.get('total_accesses', 0)}")
    print(f"  Shared Clusters: {stats.get('shared_clusters', 0)}")
    print(f"  Cache File Size: {stats.get('cache_file_size', 0)} bytes")
    print()

    if not info:
        print("No active cached cluster found")
        return

    print("=== Active Cached Cluster ===")
    print(f"  Cluster ID: {info['cluster_id']}")
    print(f"  Created At: {info['created_at']}")
    print(f"  Expires At: {info['expiry_time']}")
    print(f"  Is Valid: {'Yes' if info['is_valid'] else 'No'}")
    print(f"  Access Count: {info.get('access_count', 0)}")
    print(f"  Is Shared: {'Yes' if info.get('is_shared', False) else 'No'}")
    print(f"  Last Accessed: {info.get('last_accessed', 'Unknown')}")

    if info["is_valid"]:
        # Calculate time remaining
        try:
            expiry = datetime.fromisoformat(info["expiry_time"])
            remaining = expiry - datetime.now()
            minutes_remaining = int(remaining.total_seconds() / 60)
            print(f"  Time Remaining: {minutes_remaining} minutes")
        except:
            print("  Time Remaining: Unable to calculate")


def clear_cache():
    """Clear the cluster cache"""
    cache_manager = CacheManager()
    cache_manager.clear_cluster_cache()
    print("Cluster cache cleared")


def cleanup_expired():
    """Clean up expired clusters from the cache"""
    cache_manager = CacheManager()
    removed_count = cache_manager.cleanup_expired_clusters()
    print(f"Removed {removed_count} expired clusters from cache")


def show_all_clusters():
    """Show all clusters in the cache"""
    cache_manager = CacheManager()
    clusters = cache_manager.get_all_clusters()
    
    if not clusters:
        print("No clusters found in cache")
        return
    
    print("=== All Cached Clusters ===")
    for i, cluster in enumerate(clusters, 1):
        print(f"\nCluster {i}:")
        print(f"  ID: {cluster['cluster_id']}")
        print(f"  Name: {cluster.get('cluster_name', 'N/A')}")
        print(f"  Status: {cluster.get('status', 'N/A')}")
        print(f"  Created: {cluster['created_at']}")
        print(f"  Expires: {cluster['expiry_time']}")
        print(f"  Active: {'Yes' if cluster.get('is_active') else 'No'}")
        print(f"  Access Count: {cluster.get('access_count', 0)}")
        print(f"  Is Shared: {'Yes' if cluster.get('is_shared', False) else 'No'}")
        print(f"  Last Accessed: {cluster.get('last_accessed', 'N/A')}")


def optimize_database():
    """Optimize the database for better performance"""
    cache_manager = CacheManager()
    cache_manager.optimize_database()


def show_database_info():
    """Show database file information"""
    cache_manager = CacheManager()
    info = cache_manager.get_database_info()
    
    print("=== Database Information ===")
    print(f"  Database Path: {info['database_path']}")
    print(f"  Database Exists: {'Yes' if info['database_exists'] else 'No'}")
    print(f"  Database Size: {info['database_size']} bytes")
    print(f"  WAL File Exists: {'Yes' if info['wal_exists'] else 'No'}")
    print(f"  WAL File Size: {info['wal_size']} bytes")
    print(f"  Shared Memory File Exists: {'Yes' if info['shm_exists'] else 'No'}")
    print(f"  Shared Memory File Size: {info['shm_size']} bytes")


def show_shared_clusters():
    """Show all shared cluster registry entries"""
    cache_manager = CacheManager()
    shared_clusters = cache_manager.get_all_shared_clusters()
    
    if not shared_clusters:
        print("No shared clusters found in registry")
        return
    
    print("=== Shared Cluster Registry ===")
    for i, cluster in enumerate(shared_clusters, 1):
        print(f"\nShared Cluster {i}:")
        print(f"  Config Hash: {cluster['config_hash'][:16]}...")
        print(f"  Cluster ID: {cluster.get('cluster_id', 'N/A')}")
        print(f"  Status: {cluster['status']}")
        print(f"  Worker PID: {cluster['worker_pid']}")
        print(f"  Usage Count: {cluster['usage_count']}")
        print(f"  Created: {cluster['creation_time']}")
        if cluster.get('error_message'):
            print(f"  Error: {cluster['error_message']}")


def cleanup_shared_registry():
    """Clean up all shared cluster registry entries"""
    cache_manager = CacheManager()
    shared_clusters = cache_manager.get_all_shared_clusters()
    
    if not shared_clusters:
        print("No shared clusters to clean up")
        return
    
    cleaned_count = 0
    for cluster in shared_clusters:
        if cache_manager.cleanup_shared_cluster_registry(cluster['config_hash']):
            cleaned_count += 1
    
    print(f"Cleaned up {cleaned_count} shared cluster registry entries")


def main():
    """
    Main function to parse command-line arguments and execute commands.
    Provides commands for managing the Databricks cluster cache.
    Usage:
        python cli.py status    # Show cache status and statistics
        python cli.py clear     # Clear all cluster cache
        python cli.py cleanup   # Clean up expired clusters
        python cli.py list      # Show all cached clusters
        python cli.py optimize  # Optimize database performance
        python cli.py dbinfo    # Show database file information
        python cli.py shared    # Show shared cluster registry
        python cli.py cleanup-shared  # Clean up shared cluster registry
    """
    parser = argparse.ArgumentParser(
        description="Databricks Environment Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show cache status")

    # Clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cluster cache")

    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up expired clusters")

    # List command
    list_parser = subparsers.add_parser("list", help="Show all cached clusters")

    # Optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Optimize database performance")

    # Database info command
    dbinfo_parser = subparsers.add_parser("dbinfo", help="Show database file information")

    # Shared clusters command
    shared_parser = subparsers.add_parser("shared", help="Show shared cluster registry")

    # Cleanup shared registry command
    cleanup_shared_parser = subparsers.add_parser("cleanup-shared", help="Clean up shared cluster registry")

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()

    if args.command == "status":
        show_cache_status()
    elif args.command == "clear":
        clear_cache()
    elif args.command == "cleanup":
        cleanup_expired()
    elif args.command == "list":
        show_all_clusters()
    elif args.command == "optimize":
        optimize_database()
    elif args.command == "dbinfo":
        show_database_info()
    elif args.command == "shared":
        show_shared_clusters()
    elif args.command == "cleanup-shared":
        cleanup_shared_registry()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
