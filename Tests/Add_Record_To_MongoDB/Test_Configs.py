import os

User_Input = "Go to test_collection in MongoDB and add another record. Please add the record with the name 'John Doe' and the age 30."

Configs = {
    "services": {
        "mongodb": {
            "connection_string": os.getenv("MONGODB_URI"),
            "databases": [
                {"name": "test_database", "collections": [{"name": "test_collection"}]}
            ],
        }
    }
}
