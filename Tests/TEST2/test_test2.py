"""
Simple test for TEST2 that uses shared fixtures from Fixtures/shared_resources.py
"""

import pytest
import os


def test_shared_resource_works(shared_resource):
    """Test that the shared resource fixture works."""
    print(f"TEST2: Using shared resource: {shared_resource}")
    assert shared_resource is not None
    assert isinstance(shared_resource, str)
    assert True  # Simple assertion to show it works


def test_another_shared_fixture_works(another_shared_fixture):
    """Test that the another shared fixture works."""
    print(f"TEST2: Using another shared fixture: {another_shared_fixture}")
    assert another_shared_fixture == "another_shared_value"
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