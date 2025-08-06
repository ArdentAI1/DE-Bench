import os

# AI Agent task - non-prescriptive, focuses on problem identification
User_Input = """
You have a PostgreSQL database with a data quality issue in the 'books_bad' table.

The table stores book information, but there's a problem with how authors are being handled. When someone searches for books by a specific author like 'Gamma', they're not getting complete information about co-authors.

Please analyze the current schema and data, identify the issue, and implement a solution that ensures no author information is lost when querying for books.

Test your solution to verify all co-author relationships are preserved.
"""

# Basic system configuration for PostgreSQL agent testing
Configs = {
    "services": {
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "authors_schema_test_db"}],
        }
    }
} 