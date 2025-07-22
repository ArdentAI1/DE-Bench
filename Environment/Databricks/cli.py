#!/usr/bin/env python3
"""
Databricks Environment Management CLI

This script provides command-line utilities for managing the Databricks environment,
including cluster cache management.
"""

import argparse
import sys
from datetime import datetime
from .setup import (
    get_cached_cluster_info,
    clear_cluster_cache,
    load_cluster_cache
)

def show_cache_status():
    """Show current cache status"""
    info = get_cached_cluster_info()
    
    if not info:
        print("No cached cluster found")
        return
    
    print("Cached Cluster Information:")
    print(f"  Cluster ID: {info['cluster_id']}")
    print(f"  Created At: {info['created_at']}")
    print(f"  Expires At: {info['expiry_time']}")
    print(f"  Is Valid: {'Yes' if info['is_valid'] else 'No'}")
    
    if info['is_valid']:
        # Calculate time remaining
        try:
            expiry = datetime.fromisoformat(info['expiry_time'])
            remaining = expiry - datetime.now()
            minutes_remaining = int(remaining.total_seconds() / 60)
            print(f"  Time Remaining: {minutes_remaining} minutes")
        except:
            print("  Time Remaining: Unable to calculate")

def clear_cache():
    """Clear the cluster cache"""
    clear_cluster_cache()

def main():
    parser = argparse.ArgumentParser(
        description="Databricks Environment Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show cache status')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear cluster cache')
    
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    args = parser.parse_args()
    
    if args.command == 'status':
        show_cache_status()
    elif args.command == 'clear':
        clear_cache()
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 