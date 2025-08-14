import os
import time
import uuid

import pytest

from Fixtures.Databricks.databricks_manager import DatabricksManager

# SQLite-based coordination replaces global registry


@pytest.fixture(scope="function")
def databricks_resource(request):
    """
    A function-scoped fixture that creates Databricks resources based on template.
    Template structure: {
        "resource_id": "id", 
        "databricks_config": {...},  # Optional, uses client config if not provided
        "cluster_config": {...},     # Optional cluster configuration overrides
        "databases": [{"name": "db", "tables": [{"name": "table", "data": [], "format": "delta"}]}],
        "notebooks": [{"path": "/path/to/notebook", "content": "# Notebook content", "language": "python"}],
        "jobs": [{"name": "job_name", "notebook_path": "/path", "cluster_id": "optional"}]
    }
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting databricks_resource for {test_name}")
    
    build_template = request.param
    
    # Get databricks config from template or auto-detect
    template_config = build_template.get("databricks_config")
    # Handle cluster creation/selection
    cluster_id = build_template.get("cluster_id") or request.param.get("cluster_id")
    cluster_created_by_us = False
    cluster_name = build_template.get("resource_id") or request.param.get("resource_id")
    is_shared_cluster = build_template.get("use_shared_cluster") or request.param.get("use_shared_cluster") or False
    default_expiry_hours = build_template.get("shared_cluster_timeout", 1) or request.param.get("shared_cluster_timeout", 1)
    databricks_manager = DatabricksManager(
        config=template_config,
        request=request,
        default_expiry_hours=default_expiry_hours,
        cluster_id=cluster_id,
        cluster_created_by_us=cluster_created_by_us,
        cluster_name=cluster_name,
        shared_cluster=is_shared_cluster,
    )

    # Merge any template-specific config with base config
    config = databricks_manager.config.copy()
    if "databricks_config" in build_template:
        config.update(build_template["databricks_config"])

    print(f"Worker {os.getpid()}: Creating Databricks resource for {test_name}")
    creation_start = time.time()

    created_resources = []

    cluster_config_hash = None

    if is_shared_cluster:
        # Use shared cluster approach with mutex coordination
        timeout = build_template.get("shared_cluster_timeout", 300)
        fallback = build_template.get("cluster_fallback", True)
        
        databricks_manager = databricks_manager.create_shared_cluster(timeout=timeout, fallback=fallback)
        cluster_id = databricks_manager.cluster_id
        cluster_created_by_us = databricks_manager.created_by_us
        cluster_config_hash = databricks_manager.cluster_config_hash
        is_shared_cluster = databricks_manager.is_shared
        # Don't add to created_resources since this is a shared cluster managed globally
    else:
        if "cluster_config" in build_template:
            cluster_config = {**config, **build_template["cluster_config"]}
        else:
            cluster_config = config
        # Create a specific cluster for this resource
        cluster_id, cluster_created_by_us = databricks_manager.get_or_create_cluster(cluster_config)
        created_resources.append({"type": "cluster", "cluster_id": cluster_id, "created_by_us": cluster_created_by_us})
        is_shared_cluster = False
    
    # Process databases and tables
    if "databases" in build_template:
        for db_config in build_template["databases"]:
            db_name = db_config["name"]
            
            # Create database
            try:
                databricks_manager.client.sql.execute_query(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                created_resources.append({"type": "database", "name": db_name})
            except Exception as e:
                print(f"Warning: Could not create database {db_name}: {e}")
            
            # Process tables in this database
            if "tables" in db_config:
                for table_config in db_config["tables"]:
                    table_name = table_config["name"]
                    table_format = table_config.get("format", "delta")
                    full_table_name = f"{db_name}.{table_name}"
                    
                    try:
                        # Create table (basic structure)
                        if "data" in table_config and table_config["data"]:
                            # If data is provided, infer schema and create table
                            # This is a simplified approach - in practice you might want more sophisticated schema handling
                            pass  # Implementation would depend on specific data formats
                        
                        created_resources.append({
                            "type": "table", 
                            "database": db_name, 
                            "name": table_name,
                            "full_name": full_table_name,
                            "format": table_format
                        })
                    except Exception as e:
                        print(f"Warning: Could not create table {full_table_name}: {e}")
    
    # Process notebooks
    if "notebooks" in build_template:
        for notebook_config in build_template["notebooks"]:
            notebook_path = notebook_config["path"]
            content = notebook_config.get("content", "# Default notebook content")
            language = notebook_config.get("language", "python")
            
            try:
                # Upload notebook
                databricks_manager.client.workspace.upload_notebook(
                    path=notebook_path,
                    content=content,
                    language=language,
                    format="SOURCE",
                    overwrite=True
                )
                created_resources.append({"type": "notebook", "path": notebook_path})
            except Exception as e:
                print(f"Warning: Could not create notebook {notebook_path}: {e}")
    
    # Process jobs
    if "jobs" in build_template:
        for job_config in build_template["jobs"]:
            job_name = job_config["name"]
            notebook_path = job_config.get("notebook_path")
            job_cluster_id = job_config.get("cluster_id", cluster_id)
            
            if notebook_path and job_cluster_id:
                try:
                    # Create job (simplified - real implementation would need full job specification)
                    job_spec = {
                        "name": job_name,
                        "existing_cluster_id": job_cluster_id,
                        "notebook_task": {"notebook_path": notebook_path}
                    }
                    # Note: This would need actual job creation implementation
                    created_resources.append({"type": "job", "name": job_name, "notebook_path": notebook_path})
                except Exception as e:
                    print(f"Warning: Could not create job {job_name}: {e}")
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: Databricks resource creation took {creation_end - creation_start:.2f}s")
    
    # Generate unique resource ID
    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    resource_id = build_template.get("resource_id", f"databricks_resource_{test_name}_{test_timestamp}_{test_uuid}")
    
    # Create detailed resource data
    resource_data = {
        "resource_id": resource_id,
        "type": "databricks_resource",
        "test_name": test_name,
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"A Databricks resource for {test_name}",
        "status": "active",
        "created_resources": created_resources,
        "config": config,
        "cluster_id": cluster_id,
        "cluster_created_by_us": cluster_created_by_us,
        "is_shared_cluster": is_shared_cluster,
        "cluster_config_hash": cluster_config_hash,
        "databricks_manager": databricks_manager
    }
    
    print(f"Worker {os.getpid()}: Created Databricks resource {resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: Databricks fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    yield resource_data
    
    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up Databricks resource {resource_id}")
    databricks_manager.cleanup_databricks_resources(created_resources, resource_data)
