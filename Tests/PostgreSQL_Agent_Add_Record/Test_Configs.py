import os

# AI Agent task for PostgreSQL database manipulation
User_Input = """
Connect to the PostgreSQL database and add a new user record to the users table.
Add a user with the following details:
- name: 'Alice Green'
- email: 'alice@example.com'  
- age: 28

Make sure to use the correct PostgreSQL connection parameters and verify the record was inserted successfully.
"""

# Basic system configuration for PostgreSQL agent testing
Configs = {
    "services": {
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "add_record_test_db"}],
        }
    }
} 