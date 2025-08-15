import pytest
import json
import time
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



@pytest.fixture(scope="function")
def legacy_postgres_resource(request):
    """
    A function-scoped fixture that creates PostgreSQL resources based on template.
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
    print(f"Worker {os.getpid()}: Starting postgres_resource for {test_name}")
    
    build_template = request.param
    
    # Create PostgreSQL resource
    print(f"Worker {os.getpid()}: Creating PostgreSQL resource for {test_name}")
    creation_start = time.time()
    
    created_resources = []
    
    # Connect to postgres system database for database creation
    system_connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOSTNAME"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USERNAME"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database="postgres",
        sslmode="require",
    )
    system_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    system_cursor = system_connection.cursor()
    
    try:
        # Process databases from template
        if "databases" in build_template:
            for db_config in build_template["databases"]:
                db_name = db_config["name"]
                
                # Check and kill any existing connections to the database
                try:
                    system_cursor.execute(
                        """
                        SELECT pg_terminate_backend(pid) 
                        FROM pg_stat_activity 
                        WHERE datname = %s AND pid <> pg_backend_pid()
                        """,
                        (db_name,)
                    )
                except Exception as e:
                    print(f"Worker {os.getpid()}: Warning - could not terminate connections: {e}")
                
                # Drop and create database
                system_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                system_cursor.execute(f"CREATE DATABASE {db_name}")
                print(f"Worker {os.getpid()}: Created database {db_name}")
                
                created_resources.append({"type": "database", "name": db_name, "tables": []})
                db_resource = created_resources[-1]
                
                # Connect to the new database for table operations
                db_connection = psycopg2.connect(
                    host=os.getenv("POSTGRES_HOSTNAME"),
                    port=os.getenv("POSTGRES_PORT"),
                    user=os.getenv("POSTGRES_USERNAME"),
                    password=os.getenv("POSTGRES_PASSWORD"),
                    database=db_name,
                    sslmode="require",
                )
                db_cursor = db_connection.cursor()
                
                try:
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
                                db_cursor.execute(create_table_sql)
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
                                        db_cursor.execute(insert_sql, values)
                                    
                                    db_connection.commit()
                                    print(f"Worker {os.getpid()}: Inserted {len(table_config['data'])} records into {table_name}")
                    
                finally:
                    db_cursor.close()
                    db_connection.close()
        
    finally:
        system_cursor.close()
        system_connection.close()
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: PostgreSQL resource creation took {creation_end - creation_start:.2f}s")
    
    resource_id = build_template.get("resource_id", f"postgres_resource_{test_name}_{int(time.time())}")
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "postgresql_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A PostgreSQL resource for {test_name}",
        "status": "active",
        "created_resources": created_resources
    }
    
    print(f"Worker {os.getpid()}: Created PostgreSQL resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: PostgreSQL fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up PostgreSQL resource {resource_id}")
    try:
        # Connect to system database for cleanup
        cleanup_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database="postgres",
            sslmode="require",
        )
        cleanup_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cleanup_cursor = cleanup_connection.cursor()
        
        # Clean up created databases in reverse order
        for resource in reversed(created_resources):
            if resource["type"] == "database":
                db_name = resource["name"]
                
                # Terminate connections before dropping
                try:
                    cleanup_cursor.execute(
                        """
                        SELECT pg_terminate_backend(pid) 
                        FROM pg_stat_activity 
                        WHERE datname = %s AND pid <> pg_backend_pid()
                        """,
                        (db_name,)
                    )
                except Exception as e:
                    print(f"Worker {os.getpid()}: Warning during cleanup - could not terminate connections: {e}")
                
                # Drop the database
                cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                print(f"Worker {os.getpid()}: Dropped database {db_name}")
        
        cleanup_cursor.close()
        cleanup_connection.close()
        print(f"Worker {os.getpid()}: PostgreSQL resource {resource_id} cleaned up successfully")
        
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up PostgreSQL resource: {e}") 