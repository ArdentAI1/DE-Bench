import os

# AI Agent task for MySQL database manipulation  
User_Input = """
Connect to the MySQL database and update user records in the users table.
Update all users who are over 30 years old to have their age set to 35.

Make sure to use the correct MySQL connection parameters and verify that the updates were applied correctly.
"""

# Basic system configuration for MySQL agent testing
Configs = {
    "services": {
        "mysql": {
            "host": os.getenv("MYSQL_HOST"),
            "port": os.getenv("MYSQL_PORT"),
            "username": os.getenv("MYSQL_USERNAME"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "databases": [{"name": "update_records_test_db"}],
        }
    }
} 