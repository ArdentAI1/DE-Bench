import pytest
import json
import time
import os
import mysql.connector


@pytest.fixture(scope="function")
def mysql_resource(request):
    """
    A function-scoped fixture that creates MySQL resources based on template.
    Template structure: {
        "resource_id": "id", 
        "databases": [
            {
                "name": "db_name", 
                "tables": [
                    {
                        "name": "table_name",
                        "columns": [
                            {"name": "col_name", "type": "VARCHAR(100)", "not_null": True, "primary_key": False, "unique": False, "default": "value"}
                        ],
                        "data": [{"col1": "val1", "col2": "val2"}]
                    }
                ]
            }
        ]
    }
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting mysql_resource for {test_name}")
    
    build_template = request.param
    
    # Create MySQL resource
    print(f"Worker {os.getpid()}: Creating MySQL resource for {test_name}")
    creation_start = time.time()
    
    created_resources = []
    
    # Connect to MySQL (single connection for everything)
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=os.getenv("MYSQL_PORT"),
        user=os.getenv("MYSQL_USERNAME"),
        password=os.getenv("MYSQL_PASSWORD"),
        connect_timeout=10,
    )
    cursor = connection.cursor()
    
    try:
        # Process databases from template
        if "databases" in build_template:
            for db_config in build_template["databases"]:
                db_name = db_config["name"]
                
                # Drop and create database
                cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                cursor.execute(f"CREATE DATABASE {db_name}")
                print(f"Worker {os.getpid()}: Created database {db_name}")
                
                created_resources.append({"type": "database", "name": db_name, "tables": []})
                db_resource = created_resources[-1]
                
                # Switch to the new database (MySQL allows this!)
                cursor.execute(f"USE {db_name}")
                
                # Process tables in this database
                if "tables" in db_config:
                    for table_config in db_config["tables"]:
                        table_name = table_config["name"]
                        
                        # Generate and execute CREATE TABLE from JSON columns
                        if "columns" in table_config:
                            # Build CREATE TABLE SQL from column definitions
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
                            cursor.execute(create_table_sql)
                            print(f"Worker {os.getpid()}: Created table {table_name} in {db_name}")
                            db_resource["tables"].append(table_name)
                            
                            # Insert data if provided
                            if "data" in table_config and table_config["data"]:
                                for record in table_config["data"]:
                                    # Generate INSERT statement from record data
                                    columns = list(record.keys())
                                    values = list(record.values())
                                    placeholders = ", ".join(["%s"] * len(values))
                                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                                    cursor.execute(insert_sql, values)
                                
                                connection.commit()
                                print(f"Worker {os.getpid()}: Inserted {len(table_config['data'])} records into {table_name}")
        
    finally:
        cursor.close()
        connection.close()
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: MySQL resource creation took {creation_end - creation_start:.2f}s")
    
    resource_id = build_template.get("resource_id", f"mysql_resource_{test_name}_{int(time.time())}")
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "mysql_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A MySQL resource for {test_name}",
        "status": "active",
        "created_resources": created_resources
    }
    
    print(f"Worker {os.getpid()}: Created MySQL resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: MySQL fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up MySQL resource {resource_id}")
    try:
        # Connect for cleanup
        cleanup_connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USERNAME"),
            password=os.getenv("MYSQL_PASSWORD"),
            connect_timeout=10,
        )
        cleanup_cursor = cleanup_connection.cursor()
        
        # Clean up created databases in reverse order
        for resource in reversed(created_resources):
            if resource["type"] == "database":
                db_name = resource["name"]
                cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                print(f"Worker {os.getpid()}: Dropped database {db_name}")
        
        cleanup_connection.commit()
        cleanup_cursor.close()
        cleanup_connection.close()
        print(f"Worker {os.getpid()}: MySQL resource {resource_id} cleaned up successfully")
        
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up MySQL resource: {e}") 