"""
Shared resource fixtures for all tests.
These fixtures use FileLock to coordinate resource creation across pytest-xdist workers.
"""

import pytest
import time
import os
import json
from filelock import FileLock


@pytest.fixture(scope="session")
def shared_resource(request):
    """
    A shared resource that uses FileLock to coordinate creation across workers.
    Only one worker will create the resource, others will wait and reuse it.
    """

    rid = request.param

    print("Got ID", rid)

    with open(f".tmp/test_dump_{rid}.txt", "w") as f:
        f.write(rid)






    #print("Got ID", id)

    
    start_time = time.time()
    print(f"Worker {os.getpid()}: Starting shared_resource fixture at {start_time}")
    
    # Create temp directory in project root
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".tmp")
    

    lock_file = os.path.join(temp_dir, f"shared_resource_{rid}.lock")

    #this should extract the resource FROM the file and see if it exists so it should read hte list and find the object with the right type

    resource_file = os.path.join(temp_dir, "resources.json")
    
    
    # First check without lock
    #checks if the resource already exists in the resource file


    #we first need to chec




    if os.path.exists(resource_file):
        # Resource exists, use it immediately
        with open(resource_file, 'r') as f:
            resource_data = json.load(f)


        


        resource_id = resource_data["resource_id"]
        print(f"Worker {os.getpid()}: Resource already exists, reusing...")
        print(f"Worker {os.getpid()}: Reusing resource {resource_id}")
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
        yield resource_id
        
        # No cleanup here for a shared resource that happens in the end block
        
    else:
        # Only lock if we need to create
        with FileLock(lock_file):
            # Double-check after acquiring lock
            if os.path.exists(resource_file):

                #if os.path.exists(resource_file):
                with open(resource_file, 'r') as f:
                    resource_data = json.load(f)


                
                resource_id = resource_data["resource_id"]
                print(f"Worker {os.getpid()}: Resource already exists, reusing...")
                print(f"Worker {os.getpid()}: Reusing resource {resource_id}")
            else:
                # Create resource


                #creates the brackets

                if os.path.exists(resource_file):
                    with open(resource_file, 'w') as f:
                        json.dump([], f, indent=2)



             



                


                print(f"Worker {os.getpid()}: Creating shared resource...")
                creation_start = time.time()
                time.sleep(10)  # Simulate expensive creation
                creation_end = time.time()
                print(f"Worker {os.getpid()}: Resource creation took {creation_end - creation_start:.2f}s")
                
                resource_id = f"resource_{int(time.time())}"
                
                # Create detailed resource data
                resource_data = {
                    "resource_id": resource_id,
                    "type": "shared_test_resource",
                    "creation_time": time.time(),
                    "worker_pid": os.getpid(),
                    "creation_duration": creation_end - creation_start,
                    "description": "A shared resource for coordinating test execution across workers",
                    "status": "active"
                }

                
                # Save resource info as JSON
                with open(resource_file, 'w') as f:
                    json.dump(resource_data, f, indent=2)
                
                print(f"Worker {os.getpid()}: Created resource {resource_id}")
            
            fixture_end_time = time.time()
            print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
            
            yield resource_id
            
            # Cleanup (only by the worker that created it)
           
def cleanup_shared_resource(resource_data):
    print(f"Worker {os.getpid()}: Cleaning up shared resource {resource_data['resource_id']}")
 




@pytest.fixture(scope="session")
def second_shared_resource():
    """
    A shared resource that uses FileLock to coordinate creation across workers.
    Only one worker will create the resource, others will wait and reuse it.
    """
    start_time = time.time()
    print(f"Worker {os.getpid()}: Starting second_shared_resource fixture at {start_time}")
    
    # Create temp directory in project root
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".tmp")
    

    lock_file = os.path.join(temp_dir, "second_shared_resource.lock")

    #this should extract the resource FROM the file and see if it exists so it should read hte list and find the object with the right type

    resource_file = os.path.join(temp_dir, "resources.json")
    
    
    # First check without lock
    #checks if the resource already exists in the resource file
    if os.path.exists(resource_file):
        # Resource exists, use it immediately
        with open(resource_file, 'r') as f:
            resource_data = json.load(f)


        


        resource_id = resource_data["resource_id"]
        print(f"Worker {os.getpid()}: Resource already exists, reusing...")
        print(f"Worker {os.getpid()}: Reusing resource {resource_id}")
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
        yield resource_id
        
        # No cleanup here for a shared resource that happens in the end block
        
    else:
        # Only lock if we need to create
        with FileLock(lock_file):
            # Double-check after acquiring lock
            if os.path.exists(resource_file):

                #if os.path.exists(resource_file):
                with open(resource_file, 'r') as f:
                    resource_data = json.load(f)


                
                resource_id = resource_data["resource_id"]
                print(f"Worker {os.getpid()}: Resource already exists, reusing...")
                print(f"Worker {os.getpid()}: Reusing resource {resource_id}")
            else:
                # Create resource


                #creates the brackets

                if os.path.exists(resource_file):
                    with open(resource_file, 'w') as f:
                        json.dump([], f, indent=2)



             



                


                print(f"Worker {os.getpid()}: Creating shared resource...")
                creation_start = time.time()
                time.sleep(10)  # Simulate expensive creation
                creation_end = time.time()
                print(f"Worker {os.getpid()}: Resource creation took {creation_end - creation_start:.2f}s")
                
                resource_id = f"resource_{int(time.time())}"
                
                # Create detailed resource data
                resource_data = {
                    "resource_id": resource_id,
                    "type": "shared_test_resource",
                    "creation_time": time.time(),
                    "worker_pid": os.getpid(),
                    "creation_duration": creation_end - creation_start,
                    "description": "A shared resource for coordinating test execution across workers",
                    "status": "active"
                }

                
                # Save resource info as JSON
                with open(resource_file, 'w') as f:
                    json.dump(resource_data, f, indent=2)
                
                print(f"Worker {os.getpid()}: Created resource {resource_id}")
            
            fixture_end_time = time.time()
            print(f"Worker {os.getpid()}: Fixture setup took {fixture_end_time - start_time:.2f}s total")
            
            yield resource_id
            
            # Cleanup (only by the worker that created it)
           


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
    time.sleep(20)  # Simulate resource creation
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