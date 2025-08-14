import pytest
import time
import os
import psycopg2
import subprocess
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


@pytest.fixture(scope="function")
def postgres_resource(request):
    """
    A function-scoped fixture that creates PostgreSQL databases from SQL files.
    
    Template structure: {
        "resource_id": "id",
        "databases": [
            {
                "name": "db_name",
                "sql_file": "schema.sql"
            }
        ]
    }
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting postgres_sql_resource for {test_name}")
    
    build_template = request.param
    created_resources = []
    
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
        if "databases" in build_template:
            for db_config in build_template["databases"]:
                db_name = db_config["name"]
                
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
                
                system_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                system_cursor.execute(f"CREATE DATABASE {db_name}")
                print(f"Worker {os.getpid()}: Created database {db_name}")
                
                created_resources.append({"type": "database", "name": db_name, "tables": []})
                db_resource = created_resources[-1]
                
                if "sql_file" in db_config:
                    sql_file = db_config["sql_file"]
                    
                    if not os.path.isabs(sql_file):
                        test_file = request.fspath.strpath
                        test_dir = os.path.dirname(test_file)
                        sql_file = os.path.join(test_dir, sql_file)
                    
                    if not os.path.exists(sql_file):
                        raise FileNotFoundError(f"SQL file not found: {sql_file}")
                    
                    print(f"Worker {os.getpid()}: Loading SQL file {sql_file} into database {db_name}")
                    
                    env = os.environ.copy()
                    env['PGPASSWORD'] = os.getenv("POSTGRES_PASSWORD")
                    
                    cmd = [
                        'psql',
                        '-h', os.getenv("POSTGRES_HOSTNAME"),
                        '-p', os.getenv("POSTGRES_PORT"),
                        '-U', os.getenv("POSTGRES_USERNAME"),
                        '-d', db_name,
                        '-f', sql_file,
                        '--quiet'
                    ]
                    
                    try:
                        subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
                        print(f"Worker {os.getpid()}: Successfully loaded SQL file into {db_name}")
                        
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
                            db_cursor.execute("""
                                SELECT table_name 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public'
                                ORDER BY table_name
                            """)
                            tables = [row[0] for row in db_cursor.fetchall()]
                            db_resource["tables"] = tables
                            print(f"Worker {os.getpid()}: Loaded {len(tables)} tables from SQL file into {db_name}")
                            
                        finally:
                            db_cursor.close()
                            db_connection.close()
                            
                    except subprocess.CalledProcessError as e:
                        print(f"Worker {os.getpid()}: Error loading SQL file: {e}")
                        print(f"Worker {os.getpid()}: STDOUT: {e.stdout}")
                        print(f"Worker {os.getpid()}: STDERR: {e.stderr}")
                        raise
        
    finally:
        system_cursor.close()
        system_connection.close()
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: PostgreSQL SQL resource creation took {creation_end - start_time:.2f}s")
    
    resource_id = build_template.get("resource_id", f"postgres_sql_resource_{test_name}_{int(time.time())}")
    
    resource_data = {
        "resource_id": resource_id,
        "type": "postgresql_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - start_time,
        "description": f"A PostgreSQL SQL resource for {test_name}",
        "status": "active",
        "created_resources": created_resources
    }
    
    print(f"Worker {os.getpid()}: Created PostgreSQL SQL resource {resource_id}")
    
    yield resource_data
    
    print(f"Worker {os.getpid()}: Cleaning up PostgreSQL SQL resource {resource_id}")
    try:
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
        
        for resource in reversed(created_resources):
            if resource["type"] == "database":
                db_name = resource["name"]
                
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
                
                cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                print(f"Worker {os.getpid()}: Dropped database {db_name}")
        
        cleanup_cursor.close()
        cleanup_connection.close()
        print(f"Worker {os.getpid()}: PostgreSQL SQL resource {resource_id} cleaned up successfully")
        
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up PostgreSQL SQL resource: {e}")
