# Braintrust-only Snowflake test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import snowflake.connector
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Snowflake test validates external table implementation with S3.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with external table support
    custom_snowflake_config = {
        "resource_id": f"external_tables_{test_timestamp}_{test_uuid}",
        "database": f"DATA_LAKE_DB_{test_timestamp}_{test_uuid}",
        "schema": f"EXTERNAL_{test_timestamp}_{test_uuid}",
        "sql_file": None,  # No initial SQL file needed
    }

    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    return [snowflake_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    """
    from extract_test_configs import create_config_from_fixtures

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented external tables.
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes external table setup",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "External Stage Creation",
            "description": "Verify external stages created for S3",
            "status": "running",
            "Result_Message": "Checking for external stages...",
        },
        {
            "name": "External Table Creation",
            "description": "Verify external tables created",
            "status": "running",
            "Result_Message": "Validating external tables...",
        },
        {
            "name": "Query Functionality",
            "description": "Test SELECT queries on external tables",
            "status": "running",
            "Result_Message": "Testing query functionality...",
        },
        {
            "name": "Schema Handling",
            "description": "Verify flexible schema handling",
            "status": "running",
            "Result_Message": "Checking schema flexibility...",
        },
        {
            "name": "Partition Awareness",
            "description": "Verify partition columns for filtering",
            "status": "running",
            "Result_Message": "Checking partition setup...",
        },
        {
            "name": "File Format Configuration",
            "description": "Verify file format options set correctly",
            "status": "running",
            "Result_Message": "Validating file formats...",
        },
    ]

    try:
        # Step 1: Check agent execution
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed successfully"

        # Get Snowflake fixture
        snowflake_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "snowflake_resource"), None
        ) if fixtures else None

        if not snowflake_fixture:
            raise Exception("Snowflake fixture not found")

        resource_data = getattr(snowflake_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("Snowflake resource data not available")

        database_name = resource_data.get("database_name")
        schema_name = resource_data.get("schema_name")

        # Connect to Snowflake
        snowflake_conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=database_name,
            schema=schema_name,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        )
        snowflake_cur = snowflake_conn.cursor()

        try:
            # Step 2: Check for external stages
            print("🔍 Checking for external stages...", flush=True)
            
            snowflake_cur.execute(f"SHOW STAGES IN SCHEMA {database_name}.{schema_name}")
            stages = snowflake_cur.fetchall()
            
            # Look for stages with S3 or external in name
            external_stages = [s for s in stages if 'EXTERNAL' in str(s).upper() or 'S3' in str(s).upper()]
            
            if len(external_stages) >= 1:
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"✅ Found {len(external_stages)} external stage(s)"
            else:
                test_steps[1]["status"] = "partial"
                test_steps[1]["Result_Message"] = f"⚠️ Found {len(stages)} stage(s), but may not be external stages"

            # Step 3: Check for external tables
            print("🔍 Checking for external tables...", flush=True)
            
            snowflake_cur.execute(f"""
                SHOW TABLES IN SCHEMA {database_name}.{schema_name}
            """)
            all_tables = snowflake_cur.fetchall()
            
            # Check for external tables (look in table type or properties)
            snowflake_cur.execute(f"""
                SELECT table_name, table_type 
                FROM {database_name}.INFORMATION_SCHEMA.TABLES 
                WHERE table_schema = '{schema_name}'
            """)
            tables_info = snowflake_cur.fetchall()
            
            external_tables = [t for t in tables_info if 'EXTERNAL' in str(t[1]).upper()]
            
            if len(external_tables) >= 1:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ Found {len(external_tables)} external table(s): {[t[0] for t in external_tables]}"
            elif len(all_tables) >= 1:
                test_steps[2]["status"] = "partial"
                test_steps[2]["Result_Message"] = f"⚠️ Found {len(all_tables)} table(s), verifying if external..."
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "❌ No tables found"

            # Step 4: Test query functionality
            print("🔍 Testing query functionality...", flush=True)
            
            query_worked = False
            if len(all_tables) > 0:
                try:
                    # Try to query the first table
                    table_name = all_tables[0][1]  # Table name from SHOW TABLES
                    snowflake_cur.execute(f"SELECT * FROM {table_name} LIMIT 5")
                    results = snowflake_cur.fetchall()
                    query_worked = True
                    
                    test_steps[3]["status"] = "passed"
                    test_steps[3]["Result_Message"] = f"✅ Successfully queried external table, returned {len(results)} rows"
                except Exception as e:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = f"❌ Query failed: {str(e)}"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ No tables to query"

            # Step 5: Check schema handling (INFER_SCHEMA or flexible columns)
            print("🔍 Checking schema handling...", flush=True)
            
            schema_features = []
            
            # Check if any tables use INFER_SCHEMA
            for table_info in all_tables[:3]:  # Check first few tables
                table_name = table_info[1]
                try:
                    snowflake_cur.execute(f"DESCRIBE TABLE {table_name}")
                    columns = snowflake_cur.fetchall()
                    
                    # Check for VARIANT columns (flexible schema)
                    variant_cols = [c for c in columns if 'VARIANT' in str(c[1]).upper()]
                    if variant_cols:
                        schema_features.append("VARIANT columns for flexibility")
                    
                    if len(columns) > 0:
                        schema_features.append(f"schema defined with {len(columns)} columns")
                except:
                    pass
            
            if len(schema_features) > 0:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Schema handling implemented: {', '.join(schema_features)}"
            else:
                test_steps[4]["status"] = "partial"
                test_steps[4]["Result_Message"] = "⚠️ Schema handling may be basic"

            # Step 6: Check for partition columns
            print("🔍 Checking for partition columns...", flush=True)
            
            partition_found = False
            for table_info in all_tables[:3]:
                table_name = table_info[1]
                try:
                    snowflake_cur.execute(f"DESCRIBE TABLE {table_name}")
                    columns = snowflake_cur.fetchall()
                    
                    # Look for common partition column names
                    col_names = [c[0].lower() for c in columns]
                    if any(name in col_names for name in ['year', 'month', 'day', 'date', 'partition']):
                        partition_found = True
                        break
                except:
                    pass
            
            if partition_found:
                test_steps[5]["status"] = "passed"
                test_steps[5]["Result_Message"] = "✅ Partition columns detected (year/month/day)"
            else:
                test_steps[5]["status"] = "partial"
                test_steps[5]["Result_Message"] = "⚠️ No explicit partition columns found (may use metadata)"

            # Step 7: Check file format configuration
            print("🔍 Checking file format configuration...", flush=True)
            
            snowflake_cur.execute(f"SHOW FILE FORMATS IN SCHEMA {database_name}.{schema_name}")
            file_formats = snowflake_cur.fetchall()
            
            if len(file_formats) >= 1:
                test_steps[6]["status"] = "passed"
                test_steps[6]["Result_Message"] = f"✅ Found {len(file_formats)} file format(s) configured"
            else:
                test_steps[6]["status"] = "partial"
                test_steps[6]["Result_Message"] = "⚠️ No custom file formats (may use defaults)"

        finally:
            snowflake_cur.close()
            snowflake_conn.close()

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
