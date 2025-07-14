from Fixtures.Test.shared_resources import cleanup_shared_resource


def session_spindown(config):

    print(config)
    print("Session spindown")

    RESOURCE_HANDLERS = {
        "shared_test_resource": cleanup_shared_resource
    }

    # Get the resource type from the config
    resource_type = config.get("type", "unknown")
    
    # Look up the handler function
    handler = RESOURCE_HANDLERS.get(resource_type)
    
    if handler:
        # Call the function normally
        handler(config)
    else:
        print(f"No handler found for resource type: {resource_type}")





