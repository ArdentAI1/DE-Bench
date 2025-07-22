import pytest
import json
import time
import os
from Configs.MongoConfig import syncMongoClient
from pymongo.errors import CollectionInvalid


@pytest.fixture(scope="function")
def mongo_resource(request):
    """
    A function-scoped fixture that creates MongoDB resources based on template.
    Template structure: {"resource_id": "id", "databases": [{"name": "db", "collections": [{"name": "col", "data": []}]}]}
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting mongo_resource for {test_name}")
    
    build_template = request.param

    
    # Create MongoDB resource
    print(f"Worker {os.getpid()}: Creating MongoDB resource for {test_name}")
    creation_start = time.time()
    
    created_resources = []
    
    # Process databases from template
    if "databases" in build_template:
        for db_config in build_template["databases"]:
            db_name = db_config["name"]
            db = syncMongoClient[db_name]
            
            # Process collections in this database
            if "collections" in db_config:
                for collection_config in db_config["collections"]:
                    collection_name = collection_config["name"]
                    
                    # Create collection with error handling
                    try:
                        db.create_collection(collection_name)
                    except CollectionInvalid:
                        db.drop_collection(collection_name)
                        db.create_collection(collection_name)
                    
                    created_resources.append({"db": db_name, "collection": collection_name})
                    
                    # Add data if specified
                    if "data" in collection_config:
                        collection = db[collection_name]
                        for record in collection_config["data"]:
                            collection.insert_one(record)
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: MongoDB resource creation took {creation_end - creation_start:.2f}s")
    
    resource_id = build_template.get("resource_id", f"mongo_resource_{test_name}_{int(time.time())}")
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "mongodb_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A MongoDB resource for {test_name}",
        "status": "active",
        "created_resources": created_resources
    }
    
    print(f"Worker {os.getpid()}: Created MongoDB resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: MongoDB fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up MongoDB resource {resource_id}")
    try:
        # Clean up created resources in reverse order
        for resource in reversed(created_resources):
            db = syncMongoClient[resource["db"]]
            db.drop_collection(resource["collection"])
        print(f"Worker {os.getpid()}: MongoDB resource {resource_id} cleaned up successfully")
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up MongoDB resource: {e}")








    