import os

# AI Agent task - non-prescriptive, focuses on problem identification
User_Input = """
You have a PostgreSQL database with user purchase data that has a calculation issue.

When trying to calculate average items per order, the results are consistently coming out as 0, even for users who clearly have purchase activity.

Please analyze the database, identify what's causing this issue, and implement a proper solution that fixes the underlying data structure problem. The solution should ensure that future calculations work correctly without requiring special queries or views.

Test your solution to verify the calculations are working correctly.
"""

# Basic system configuration for PostgreSQL agent testing
Configs = {
    "services": {
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "purchases_test_db"}],
        }
    }
} 