"""
Simple test for TEST2 that uses shared fixtures from Fixtures/shared_resources.py
"""

import pytest
import os
import time

@pytest.mark.parametrize("shared_resource", ["test_value"], indirect=True)
def test_shared_resource_works(shared_resource):
    """Test that the shared resource fixture works."""
    print(f"TEST2: Using shared resource: {shared_resource}")
    print(f"TEST2: Worker PID: {os.getpid()}")
    print(f"TEST2: Test start time: {time.time()}")
    assert shared_resource is not None
    assert isinstance(shared_resource, str)
    assert True  # Simple assertion to show it works


def test_test_specific_resource_works(test_specific_resource):
    """Test that the test_specific_resource fixture works."""
    print(f"TEST2: Using test_specific_resource: {test_specific_resource}")
    assert test_specific_resource is not None
    assert isinstance(test_specific_resource, dict)
    assert True  # Simple assertion to show it works


def test_worker_identification():
    """Test to identify which worker is running this test."""
    worker_id = os.getpid()
    print(f"TEST2: Running on worker {worker_id}")
    assert True  # Simple assertion to show it works


def test_additional_work():
    """Additional test to demonstrate parallel execution."""
    worker_id = os.getpid()
    print(f"TEST2: Additional work on worker {worker_id}")
    assert True  # Simple assertion to show it works 