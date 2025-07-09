"""
Shared resource fixtures for all tests.
These fixtures use FileLock to coordinate resource creation across pytest-xdist workers.
"""

import pytest
import time
import os
from filelock import FileLock


@pytest.fixture(scope="session")
def shared_resource():
    """
    A shared resource that uses FileLock to coordinate creation across workers.
    Only one worker will create the resource, others will wait and reuse it.
    """
    lock_file = "/tmp/shared_resource.lock"
    resource_file = "/tmp/shared_resource.txt"
    
    with FileLock(lock_file):
        # Check if resource already exists
        if os.path.exists(resource_file):
            print(f"Worker {os.getpid()}: Resource already exists, reusing...")
            with open(resource_file, 'r') as f:
                resource_id = f.read().strip()
        else:
            # Create the resource (simulate expensive operation)
            print(f"Worker {os.getpid()}: Creating shared resource...")
            time.sleep(5)  # Simulate expensive creation
            resource_id = f"resource_{int(time.time())}"
            
            # Save resource info
            with open(resource_file, 'w') as f:
                f.write(resource_id)
            
            print(f"Worker {os.getpid()}: Created resource {resource_id}")
        
        yield resource_id
        
        # Cleanup (only by the worker that created it)
        if os.path.exists(resource_file):
            with open(resource_file, 'r') as f:
                if f.read().strip() == resource_id:
                    os.remove(resource_file)
                    print(f"Worker {os.getpid()}: Cleaned up resource {resource_id}")


@pytest.fixture(scope="session")
def another_shared_fixture():
    """
    Another shared fixture to demonstrate multiple fixtures in the same location.
    """
    print(f"Worker {os.getpid()}: Setting up another shared fixture")
    yield "another_shared_value"
    print(f"Worker {os.getpid()}: Cleaning up another shared fixture") 