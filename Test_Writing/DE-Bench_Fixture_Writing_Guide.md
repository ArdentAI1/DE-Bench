# DE-Bench Fixture Writing Guide

This document explains how to create robust, reusable fixtures in the DE-Bench framework based on lessons learned from building the PostgreSQL fixture system.

## 📋 What We Learned

### Key Principles
1. **Keep SQL/queries direct** - Don't wrap simple operations in unnecessary functions
2. **JSON over SQL strings** - Use structured data that fixtures can interpret
3. **Unique naming for parallel execution** - Avoid resource name conflicts
4. **Clean separation of concerns** - Configs vs Environment vs Fixtures
5. **Resource tracking** - Always track what you create for cleanup
6. **Database-specific patterns** - Different databases have different connection requirements

## 🏗️ Fixture Architecture Pattern

### Directory Structure
```
Configs/
├── ServiceConfig.py          # Basic connections only (like MongoConfig.py)

Environment/
├── Service/
│   └── ServiceHelpers.py     # Optional helpers (only if truly needed)

Fixtures/
├── Service/
│   └── service_resources.py  # Main fixture implementation

Tests/
├── Your_Test/
│   ├── test_your_test.py     # Uses fixture via @pytest.mark.parametrize
│   └── Test_Configs.py       # Ardent system config only
```

### Core Pattern
```python
@pytest.fixture(scope="function")  # Fresh resources per test
def service_resource(request):
    # 1. EXTRACT TEMPLATE from test
    build_template = request.param
    
    # 2. SETUP TRACKING
    created_resources = []
    start_time = time.time()
    
    # 3. CREATE RESOURCES from template
    try:
        # Resource creation logic here
        # Track everything in created_resources
        pass
    finally:
        # Connection cleanup
        pass
    
    # 4. YIELD RESOURCE DATA
    yield {
        "resource_id": resource_id,
        "type": "service_resource", 
        "created_resources": created_resources,
        # ... metadata
    }
    
    # 5. CLEANUP RESOURCES
    try:
        # Clean up in reverse order
        for resource in reversed(created_resources):
            # Cleanup logic
            pass
    except Exception as e:
        print(f"Error cleaning up: {e}")
```

## 🎯 JSON Template Design

### Use Structured JSON, Not SQL Strings
```python
# ✅ GOOD: Structured JSON
{
    "resource_id": "unique_test_id",
    "databases": [
        {
            "name": "test_db_12345_abc", 
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "SERIAL", "primary_key": True},
                        {"name": "name", "type": "VARCHAR(100)", "not_null": True}
                    ],
                    "data": [
                        {"name": "John Doe", "email": "john@test.com"}
                    ]
                }
            ]
        }
    ]
}

# ❌ BAD: SQL strings
{
    "schema": "CREATE TABLE users (id SERIAL PRIMARY KEY, ...)",
    "data_sql": "INSERT INTO users VALUES (1, 'John');"
}
```

### JSON-to-SQL Conversion (Inline)
```python
# Build CREATE TABLE SQL from column definitions - inline where used
column_definitions = []
for col in table_config["columns"]:
    col_def = f"{col['name']} {col['type']}"
    
    if col.get('primary_key'):
        col_def += " PRIMARY KEY"
    if col.get('not_null'):
        col_def += " NOT NULL"
    if col.get('unique'):
        col_def += " UNIQUE"
    if col.get('default'):
        col_def += f" DEFAULT {col['default']}"
        
    column_definitions.append(col_def)

create_table_sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
db_cursor.execute(create_table_sql)
```

## 🔀 Database Connection Patterns

### Single Connection Databases (MySQL, SQLite)
```python
# Simple pattern - one connection for everything
connection = get_connection()
cursor = connection.cursor()

cursor.execute("CREATE DATABASE test_db")
cursor.execute("USE test_db")
cursor.execute("CREATE TABLE users (...)")
cursor.execute("INSERT INTO users (...)")
```

### Multi-Connection Databases (PostgreSQL)
```python
# PostgreSQL requires separate connections for different databases
# Connection 1: System database for DDL
system_connection = psycopg2.connect(database="postgres", ...)
system_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
system_cursor.execute("CREATE DATABASE test_db")
system_connection.close()

# Connection 2: Target database for tables/data
db_connection = psycopg2.connect(database="test_db", ...)
db_cursor.execute("CREATE TABLE users (...)")
db_cursor.execute("INSERT INTO users (...)")
db_connection.close()
```

## 🎲 Unique Naming for Parallel Execution

### The Problem
```python
# ❌ BAD: Fixed names cause conflicts in parallel tests
"databases": [{"name": "test_db"}]  # Multiple tests conflict!
```

### The Solution
```python
# ✅ GOOD: Unique names per test execution
import time
import uuid

test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

"databases": [{"name": f"test_db_{test_timestamp}_{test_uuid}"}]
# Result: test_db_1753809612_aea0d513
```

### Implementation Pattern
```python
# At module level in test file
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

@pytest.mark.parametrize("service_resource", [{
    "resource_id": f"test_{test_timestamp}_{test_uuid}",
    "databases": [{"name": f"test_db_{test_timestamp}_{test_uuid}"}]
}], indirect=True)
```

## 🧹 Resource Cleanup Patterns

### Track Everything You Create
```python
created_resources = []

# When creating resources
db_name = "test_db_12345"
# ... create database ...
created_resources.append({"type": "database", "name": db_name})

table_name = "users" 
# ... create table ...
created_resources[-1]["tables"] = created_resources[-1].get("tables", [])
created_resources[-1]["tables"].append(table_name)
```

### Cleanup in Reverse Order
```python
# Cleanup after test completes
try:
    for resource in reversed(created_resources):  # Reverse order!
        if resource["type"] == "database":
            # Clean up tables first, then database
            cleanup_connection = get_system_connection()
            cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {resource['name']}")
            cleanup_connection.close()
except Exception as e:
    print(f"Error cleaning up: {e}")
```

## 🗂️ Config vs Environment vs Fixture Separation

### Configs/ - Basic Connections Only
```python
# Configs/PostgresConfig.py - Keep it simple like MongoConfig.py
connection = psycopg2.connect(
    host=os.getenv("POSTGRES_HOSTNAME"),
    database="postgres",
    # ... basic connection params
)

def confirmPostgresConnection():
    """Simple connection test."""
    # Basic ping/version check
```

### Environment/ - Operational Helpers (If Needed)
```python
# Environment/PostgreSQL/PostgresHelpers.py - Only if truly useful
def get_postgres_connection(database="postgres", autocommit=False):
    """Factory for different connection types."""
    # Only create if you have multiple distinct connection patterns
```

### Fixtures/ - Main Logic
```python
# Fixtures/PostgreSQL/postgres_resources.py - The fixture itself
# Use direct psycopg2.connect() calls, not helpers unless truly needed
```

## ⚠️ Common Anti-Patterns to Avoid

### 1. Over-Abstraction
```python
# ❌ BAD: Wrapping simple operations
def drop_database_safely(name, cursor):
    cursor.execute(f"DROP DATABASE IF EXISTS {name}")

# ✅ GOOD: Direct SQL where it belongs
cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
```

### 2. Function-Wrapping-Function
```python
# ❌ BAD: Unnecessary indirection
def get_system_connection():
    return get_postgres_connection(database="postgres", autocommit=True)

# ✅ GOOD: Direct calls or meaningful abstraction
system_connection = psycopg2.connect(database="postgres", ...)
```

### 3. SQL in Test Config
```python
# ❌ BAD: SQL strings in Test_Configs.py
Configs = {
    "schema": "CREATE TABLE users (id SERIAL PRIMARY KEY...)"
}

# ✅ GOOD: Only Ardent system config in Test_Configs.py
Configs = {
    "services": {
        "postgresql": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "databases": [{"name": "test_db"}]  # Simple names only
        }
    }
}
```

### 4. Hardcoded Resource Names
```python
# ❌ BAD: Will conflict in parallel execution
"databases": [{"name": "test_fixture_db"}]

# ✅ GOOD: Unique names with timestamp + UUID
"databases": [{"name": f"test_db_{timestamp}_{uuid}"}]
```

## 🔧 Resource Metadata Pattern

### Fixture Should Return Rich Metadata
```python
resource_data = {
    "resource_id": resource_id,
    "type": "postgresql_resource",
    "test_name": test_name,
    "creation_time": time.time(),
    "worker_pid": os.getpid(),
    "creation_duration": creation_end - creation_start,
    "description": f"A PostgreSQL resource for {test_name}",
    "status": "active",
    "created_resources": created_resources  # Detailed tracking
}
```

### Tests Can Use This Metadata
```python
def test_something(request, postgres_resource):
    # Get actual database name (not hardcoded)
    db_name = postgres_resource["created_resources"][0]["name"]
    
    # Connect using actual name
    connection = psycopg2.connect(database=db_name, ...)
```

## 🎯 Test Integration Pattern

### Fixture Usage in Tests
```python
# Generate unique identifiers at module level
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

@pytest.mark.service_name
@pytest.mark.database
@pytest.mark.two  # Difficulty
@pytest.mark.parametrize("service_resource", [{
    "resource_id": f"test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"test_db_{test_timestamp}_{test_uuid}",
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "SERIAL", "primary_key": True},
                        {"name": "name", "type": "VARCHAR(100)", "not_null": True}
                    ],
                    "data": [
                        {"name": "Test User", "email": "test@example.com"}
                    ]
                }
            ]
        }
    ]
}], indirect=True)
def test_service_functionality(request, service_resource):
    # Test uses the fixture-created resources
    db_name = service_resource["created_resources"][0]["name"]
    # ... test logic ...
```

## 🏷️ Fixture Registration

### Update base_resources.py
```python
# Fixtures/base_resources.py
from Fixtures.Service.service_resources import *
```

## ✅ Best Practices Summary

1. **Keep it simple** - Don't over-engineer with unnecessary abstractions
2. **JSON templates** - Structured data that fixtures interpret into SQL
3. **Unique naming** - Always use timestamp + UUID for parallel safety
4. **Track resources** - Maintain detailed created_resources list
5. **Clean up properly** - Reverse order cleanup with error handling
6. **Direct SQL** - Don't wrap simple database operations
7. **Separate concerns** - Configs for connections, Fixtures for logic
8. **Rich metadata** - Return detailed resource information to tests
9. **Database-specific patterns** - Handle each database's connection requirements
10. **Test the fixture** - Create validation tests to ensure fixtures work correctly

## 🚀 Getting Started

1. **Study existing fixtures** - Look at `mongo_resources.py` and `postgres_resources.py`
2. **Start with connection patterns** - Understand how your database handles connections
3. **Design JSON template** - Structure that represents your resources
4. **Implement incrementally** - Start simple, add features as needed
5. **Test thoroughly** - Create fixture validation tests
6. **Document patterns** - Note any database-specific quirks

Remember: **SQL is already a templated abstraction** - don't wrap it unless you're adding real value! 