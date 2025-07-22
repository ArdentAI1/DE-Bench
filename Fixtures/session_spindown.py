from Fixtures.Test.shared_resources import cleanup_shared_resource, cleanup_second_shared_resource
import sqlite3

def session_spindown():
    print("Session spindown")


    # Read resources from SQLite and process them
    with sqlite3.connect(".tmp/resources.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT resource_id, type, creation_time, worker_pid, creation_duration, description, status FROM resources")
        
        RESOURCE_HANDLERS = {
            "shared_test_resource": cleanup_shared_resource,
            "second_shared_test_resource": cleanup_second_shared_resource
        }
        
        for row in cursor:
            # Convert SQLite row to resource dictionary
            resource_dict = {
                "resource_id": row[0],
                "type": row[1], 
                "creation_time": row[2],
                "worker_pid": row[3],
                "creation_duration": row[4],
                "description": row[5],
                "status": row[6]
            }

            print(f"Resource: {resource_dict}")
            
            
            # Get the resource type and look up the handler function
            resource_type = resource_dict.get("type", "unknown")
            handler = RESOURCE_HANDLERS.get(resource_type)
            
            if handler:
                print(f"Cleaning up resource: {resource_dict['resource_id']}")
                handler(resource_dict)
            else:
                print(f"No handler found for resource type: {resource_type}")





