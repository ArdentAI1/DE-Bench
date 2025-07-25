"""
Shared resource fixtures for all tests.
These fixtures use FileLock to coordinate resource creation across pytest-xdist workers.
"""

import pytest
import time
import os
import json
import sqlite3
from filelock import FileLock


@pytest.fixture(scope="session")
def shared_resource(request):
    """
    A shared resource that uses FileLock to coordinate creation across workers.
    Only one worker will create the resource, others will wait and reuse it.
    """

    rid = request.param

    
    start_time = time.time()
    
    # Create temp directory in project root
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".tmp")
    

    lock_file = os.path.join(temp_dir, f"shared_resource_{rid}.lock")

    #this should extract the resource FROM the file and see if it exists so it should read hte list and find the object with the right type

    #resource_file = os.path.join(temp_dir, "resources.json")
    
    
    # First check without lock
    #checks if the resource already exists in the resource file


    #we first need to chec


    #we need to check if the resource exists in the sqlite database
    with sqlite3.connect(".tmp/resources.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT resource_id, type, creation_time, worker_pid, creation_duration, description, status FROM resources WHERE resource_id = ?", (rid,))
        existing_resource = cursor.fetchone()
        if existing_resource:
            print(f"Worker {os.getpid()}: Resource {rid} already exists in SQLite")




    if existing_resource:
        # Resource exists, use it immediately

        resource_id, resource_type, creation_time, worker_pid, creation_duration, description, status = existing_resource
        
        print(f"Worker {os.getpid()}: Reusing resource {rid}")
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
        yield {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "creation_time": creation_time,
            "worker_pid": worker_pid,
            "creation_duration": creation_duration,
            "description": description,
            "status": status
        }
        # No cleanup here for a shared resource that happens in the end block
        
    else:
        # Only lock if we need to create
        with FileLock(lock_file):
            with sqlite3.connect(".tmp/resources.db") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT resource_id, type, creation_time, worker_pid, creation_duration, description, status FROM resources WHERE resource_id = ?", (rid,))
                existing_resource = cursor.fetchone()
                if existing_resource:
                    print(f"Worker {os.getpid()}: Resource {rid} already exists in SQLite")
            # Double-check after acquiring lock
            if existing_resource:
                print(f"Worker {os.getpid()}: Reusing resource {rid}")
                fixture_end_time = time.time()
                print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
                resource_id, resource_type, creation_time, worker_pid, creation_duration, description, status = existing_resource

                yield rid
            else:
                

                print(f"Worker {os.getpid()}: Creating shared resource...")
                creation_start = time.time()
                time.sleep(10)  # Simulate expensive creation
                creation_end = time.time()
                print(f"Worker {os.getpid()}: Resource creation took {creation_end - creation_start:.2f}s")
                
                # Create detailed resource data
                custom_info = {
                    "airflow_info": f"execution_date=2024-01-15,resource_id={rid}",
                    "dag_id": "test_dag",
                    "task_id": "test_task",
                    "custom_param": "some_value",
                }
                
                resource_data = {
                    "resource_id": rid,
                    "type": "shared_test_resource",
                    "creation_time": time.time(),
                    "worker_pid": os.getpid(),
                    "creation_duration": creation_end - creation_start,
                    "description": "A shared resource for coordinating test execution across workers",
                    "status": "active",
                    "custom_info": json.dumps(custom_info)
                }

                # Log to SQLite
                try:
                    with sqlite3.connect(".tmp/resources.db") as conn:
                        conn.execute("""
                            INSERT INTO resources (resource_id, type, creation_time, worker_pid, creation_duration, description, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            resource_data["resource_id"],
                            resource_data["type"],
                            resource_data["creation_time"],
                            resource_data["worker_pid"],
                            resource_data["creation_duration"],
                            resource_data["description"],
                            resource_data["status"]
                        ))
                        print(f"Worker {os.getpid()}: Logged resource {resource_data['resource_id']} to SQLite")
                except Exception as e:
                    print(f"Worker {os.getpid()}: Failed to log to SQLite: {e}")

                # Append and save the resource list
               
                print(f"Worker {os.getpid()}: Created resource {rid}")
                fixture_end_time = time.time()
                print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
                yield rid


def cleanup_shared_resource(resource_data):
    print(f"Worker {os.getpid()}: Cleaning up shared resource {resource_data['resource_id']}")
 


@pytest.fixture(scope="session")
def second_shared_resource(request):
    """
    A Second shared resource that uses FileLock to coordinate creation across workers.
    Only one worker will create the resource, others will wait and reuse it.
    """

    rid = request.param

    
    start_time = time.time()
    
    # Create temp directory in project root
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".tmp")
    

    lock_file = os.path.join(temp_dir, f"second_shared_resource_{rid}.lock")

    #this should extract the resource FROM the file and see if it exists so it should read hte list and find the object with the right type

    #resource_file = os.path.join(temp_dir, "resources.json")
    
    
    # First check without lock
    #checks if the resource already exists in the resource file


    #we first need to chec


    #we need to check if the resource exists in the sqlite database
    with sqlite3.connect(".tmp/resources.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT resource_id, type, creation_time, worker_pid, creation_duration, description, status FROM resources WHERE resource_id = ?", (rid,))
        existing_resource = cursor.fetchone()
        if existing_resource:
            print(f"Worker {os.getpid()}: Resource {rid} already exists in SQLite")




    if existing_resource:
        # Resource exists, use it immediately

        resource_id, resource_type, creation_time, worker_pid, creation_duration, description, status = existing_resource
        
        print(f"Worker {os.getpid()}: Reusing resource {rid}")
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
        yield {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "creation_time": creation_time,
            "worker_pid": worker_pid,
            "creation_duration": creation_duration,
            "description": description,
            "status": status
        }
        # No cleanup here for a shared resource that happens in the end block
        
    else:
        # Only lock if we need to create
        with FileLock(lock_file):
            with sqlite3.connect(".tmp/resources.db") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT resource_id, type, creation_time, worker_pid, creation_duration, description, status FROM resources WHERE resource_id = ?", (rid,))
                existing_resource = cursor.fetchone()
                if existing_resource:
                    print(f"Worker {os.getpid()}: Resource {rid} already exists in SQLite")
            # Double-check after acquiring lock
            if existing_resource:
                print(f"Worker {os.getpid()}: Reusing resource {rid}")
                fixture_end_time = time.time()
                print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
                resource_id, resource_type, creation_time, worker_pid, creation_duration, description, status = existing_resource

                yield rid
            else:
                

                print(f"Worker {os.getpid()}: Creating shared resource...")
                creation_start = time.time()
                time.sleep(10)  # Simulate expensive creation
                creation_end = time.time()
                print(f"Worker {os.getpid()}: Resource creation took {creation_end - creation_start:.2f}s")
                
                # Create detailed resource data
                custom_info = {
                    "airflow_info": f"execution_date=2024-01-15,resource_id={rid}",
                    "dag_id": "test_dag",
                    "task_id": "test_task",
                    "custom_param": "some_value",
                }
                
                resource_data = {
                    "resource_id": rid,
                    "type": "second_shared_test_resource",
                    "creation_time": time.time(),
                    "worker_pid": os.getpid(),
                    "creation_duration": creation_end - creation_start,
                    "description": "A shared resource for coordinating test execution across workers",
                    "status": "active",
                    "custom_info": json.dumps(custom_info)
                }

                # Log to SQLite
                try:
                    with sqlite3.connect(".tmp/resources.db") as conn:
                        conn.execute("""
                            INSERT INTO resources (resource_id, type, creation_time, worker_pid, creation_duration, description, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            resource_data["resource_id"],
                            resource_data["type"],
                            resource_data["creation_time"],
                            resource_data["worker_pid"],
                            resource_data["creation_duration"],
                            resource_data["description"],
                            resource_data["status"]
                        ))
                        print(f"Worker {os.getpid()}: Logged resource {resource_data['resource_id']} to SQLite")
                except Exception as e:
                    print(f"Worker {os.getpid()}: Failed to log to SQLite: {e}")

                # Append and save the resource list
               
                print(f"Worker {os.getpid()}: Created resource {rid}")
                fixture_end_time = time.time()
                print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
                yield rid


def cleanup_second_shared_resource(resource_data):
    print(f"Worker {os.getpid()}: Cleaning up shared resource {resource_data['resource_id']}")
 







@pytest.fixture(scope="function")
def test_specific_resource(request):
    """
    A test-scoped fixture that creates a resource specific to each test.
    No FileLock needed since each test gets its own instance.
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting test_specific_resource for {test_name}")
    
    # Create test-specific resource
    print(f"Worker {os.getpid()}: Creating test-specific resource for {test_name}")
    creation_start = time.time()
    time.sleep(12)  # Simulate resource creation
    creation_end = time.time()
    print(f"Worker {os.getpid()}: Test resource creation took {creation_end - creation_start:.2f}s")
    
    resource_id = f"test_resource_{test_name}_{int(time.time())}"
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "test_specific_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A test-specific resource for {test_name}",
        "status": "active"
    }
    
    print(f"Worker {os.getpid()}: Created test resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: Test fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up test-specific resource {resource_id}")
    print(f"Worker {os.getpid()}: Test resource {resource_id} cleaned up successfully") 