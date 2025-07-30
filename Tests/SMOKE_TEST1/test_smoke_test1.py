"""
Simple test for TEST1 that uses shared fixtures from Fixtures/shared_resources.py
"""

import pytest
import os
import time


@pytest.mark.parametrize("shared_resource", ["test_value"], indirect=True)
def test_shared_resource_works(shared_resource):
    """Test that the shared resource fixture works."""
    print(f"TEST1: Using shared resource: {shared_resource}")
    print(f"TEST1: Worker PID: {os.getpid()}")
    print(f"TEST1: Test start time: {time.time()}")
    assert shared_resource is not None
    assert isinstance(shared_resource, str)
    assert shared_resource == "test_value"
    assert True  # Simple assertion to show it works


@pytest.mark.parametrize("second_shared_resource", ["test_value1"], indirect=True)
def test_second_shared_resource_works(second_shared_resource):
    """Test that the shared resource fixture works."""
    print(f"TEST1: Using shared resource: {second_shared_resource}")
    print(f"TEST1: Worker PID: {os.getpid()}")
    print(f"TEST1: Test start time: {time.time()}")
    assert second_shared_resource is not None
    assert isinstance(second_shared_resource, str)
    assert second_shared_resource == "test_value1"
    assert True  # Simple assertion to show it works





def test_test_specific_resource_works(test_specific_resource):
    """Test that the test_specific_resource fixture works."""
    print(f"TEST1: Using test_specific_resource: {test_specific_resource}")
    assert test_specific_resource is not None
    assert isinstance(test_specific_resource, dict)
    assert True  # Simple assertion to show it works





def test_worker_identification():
    """Test to identify which worker is running this test."""
    worker_id = os.getpid()
    print(f"TEST1: Running on worker {worker_id}")
    assert True  # Simple assertion to show it works 