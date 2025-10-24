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
    This test validates Snowflake Data Sharing implementation.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture (provider account)
    custom_snowflake_config = {
        "resource_id": f"data_sharing_provider_{test_timestamp}_{test_uuid}",
        "database": f"ANALYTICS_PROVIDER_DB_{test_timestamp}_{test_uuid}",
        "schema": f"SHARED_DATA_{test_timestamp}_{test_uuid}",
        "sql_file": "provider_schema.sql",
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
    Validates Snowflake Data Sharing implementation.
    """
    test_steps = [
        {"name": "Agent Task Execution", "description": "AI Agent executes task", "status": "running", "Result_Message": "Checking agent execution..."},
        {"name": "Provider Database Setup", "description": "Verify provider database", "status": "running", "Result_Message": "Checking database..."},
        {"name": "Secure Views Creation", "description": "Verify secure views with row filtering", "status": "running", "Result_Message": "Checking secure views..."},
        {"name": "Share Object Creation", "description": "Verify Share object created", "status": "running", "Result_Message": "Checking shares..."},
        {"name": "Share Configuration", "description": "Verify views added to share", "status": "running", "Result_Message": "Checking share config..."},
        {"name": "Row-Level Security", "description": "Verify filtering by customer", "status": "running", "Result_Message": "Testing security..."},
        {"name": "Data Governance", "description": "Check audit/monitoring setup", "status": "running", "Result_Message": "Checking governance..."},
    ]

    try:
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ Agent completed successfully"

        # Get Snowflake fixture
        snowflake_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "snowflake_resource"), None
        ) if fixtures else None

        if not snowflake_fixture:
            raise Exception("Snowflake fixture not found")

        resource_data = getattr(snowflake_fixture, "_resource_data", None)
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
            # Step 2: Check provider database tables
            snowflake_cur.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
            all_tables = [row[1] for row in snowflake_cur.fetchall()]
            
            expected_tables = ['CAMPAIGN_PERFORMANCE', 'CUSTOMER_SEGMENTS', 'PRODUCT_USAGE_METRICS']
            found_tables = [t for t in expected_tables if t in all_tables]
            
            if len(found_tables) >= 2:
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"✅ Found {len(found_tables)} base tables: {found_tables}"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"❌ Only {len(found_tables)} base tables found"

            # Step 3: Check for secure views
            snowflake_cur.execute(f"SHOW VIEWS IN SCHEMA {database_name}.{schema_name}")
            all_views = [row[1] for row in snowflake_cur.fetchall()]
            
            secure_views = [v for v in all_views if 'SHARED_' in v or 'SECURE_' in v]
            
            if len(secure_views) >= 1:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ Found {len(secure_views)} secure view(s): {secure_views}"
            else:
                test_steps[2]["status"] = "partial"
                test_steps[2]["Result_Message"] = f"⚠️ Found {len(all_views)} views but names don't indicate secure views"

            # Step 4: Check for Share objects
            try:
                snowflake_cur.execute("SHOW SHARES")
                shares = snowflake_cur.fetchall()
                
                if len(shares) >= 1:
                    test_steps[3]["status"] = "passed"
                    test_steps[3]["Result_Message"] = f"✅ Found {len(shares)} Share object(s)"
                else:
                    test_steps[3]["status"] = "partial"
                    test_steps[3]["Result_Message"] = "⚠️ No Share objects found (may require special permissions)"
            except Exception as e:
                test_steps[3]["status"] = "partial"
                test_steps[3]["Result_Message"] = f"⚠️ Cannot check shares: {str(e)}"

            # Step 5: Check share configuration
            if len(secure_views) > 0:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = "✅ Secure views configured for sharing"
            else:
                test_steps[4]["status"] = "partial"
                test_steps[4]["Result_Message"] = "⚠️ Share configuration unclear"

            # Step 6: Test row-level security in views
            if len(all_views) > 0:
                try:
                    # Try to query a view and check if it has filtering logic
                    view_name = all_views[0]
                    snowflake_cur.execute(f"DESCRIBE VIEW {view_name}")
                    view_desc = snowflake_cur.fetchall()
                    
                    # Check if view definition includes CURRENT_USER or similar
                    snowflake_cur.execute(f"SELECT GET_DDL('VIEW', '{view_name}')")
                    view_ddl = snowflake_cur.fetchone()
                    
                    if view_ddl and ('CURRENT_USER' in str(view_ddl).upper() or 'CUSTOMER_ID' in str(view_ddl).upper()):
                        test_steps[5]["status"] = "passed"
                        test_steps[5]["Result_Message"] = "✅ Row-level security detected in view"
                    else:
                        test_steps[5]["status"] = "partial"
                        test_steps[5]["Result_Message"] = "⚠️ View exists but row-level security unclear"
                except Exception as e:
                    test_steps[5]["status"] = "partial"
                    test_steps[5]["Result_Message"] = f"⚠️ Cannot validate RLS: {str(e)}"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5]["Result_Message"] = "❌ No views to validate"

            # Step 7: Check for governance/monitoring
            governance_elements = []
            
            # Check for audit views
            audit_views = [v for v in all_views if 'AUDIT' in v or 'LOG' in v or 'MONITOR' in v]
            if audit_views:
                governance_elements.append(f"{len(audit_views)} audit views")
            
            if len(governance_elements) > 0:
                test_steps[6]["status"] = "passed"
                test_steps[6]["Result_Message"] = f"✅ Governance setup: {', '.join(governance_elements)}"
            else:
                test_steps[6]["status"] = "partial"
                test_steps[6]["Result_Message"] = "⚠️ No explicit governance views (can use Snowflake system views)"

        finally:
            snowflake_cur.close()
            snowflake_conn.close()

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {"score": score, "metadata": {"test_steps": test_steps}}
