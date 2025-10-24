import os

# AI Agent task for Dynamic DAG Generation
User_Input = """
Our SaaS platform has 100+ customers and we're creating Airflow DAGs manually for each one. This doesn't scale and is a maintenance nightmare.

We need dynamic DAG generation where tenant pipeline configurations are stored in PostgreSQL and DAGs are auto-generated from that config.

Create a system where:

1. PostgreSQL has a tenant_pipeline_configs table with columns:
   - tenant_id, tenant_name, source_type (postgres/mysql/s3/api)
   - source_connection_info (JSONB with connection details)
   - transformation_rules (JSONB with SQL or Python transformations)
   - schedule (cron expression like '@daily', '@hourly')
   - enabled (boolean to turn pipelines on/off)
   - snowflake_schema (target schema name for this tenant)

2. Insert 5 sample tenant configurations:
   - Tenant A: PostgreSQL source, hourly schedule, ENABLED
   - Tenant B: MySQL source, daily schedule, ENABLED
   - Tenant C: S3 source, daily schedule, ENABLED
   - Tenant D: API source, daily schedule, ENABLED
   - Tenant E: PostgreSQL source, hourly schedule, DISABLED (should not create DAG)

3. Create a DAG factory Python file (in dags/dynamic_tenant_pipelines.py) that:
   - Reads the tenant_pipeline_configs table at DAG parse time
   - Generates one DAG per ENABLED tenant
   - DAG name format: tenant_{tenant_name}_pipeline
   - Each DAG should have tasks: validate_source → extract → transform → load_to_snowflake

4. Show that 4 DAGs appear in Airflow (one for each enabled tenant)

5. Demonstrate "hot reload": when a new tenant is added to the table or existing tenant is disabled, the DAGs update automatically

Create the:
- Database table with 5 sample configurations
- Dynamic DAG factory code
- Commit to branch: BRANCH_NAME
- PR: PR_NAME

The goal is that adding a new customer is just INSERT INTO tenant_pipeline_configs, not editing Python code.
"""

# Configuration will be generated dynamically by create_model_inputs function
