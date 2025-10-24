# Braintrust-only MySQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import mysql.connector
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This MySQL test validates horizontal sharding implementation.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with monolithic multi-tenant data
    custom_mysql_config = {
        "resource_id": f"mysql_sharding_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"saas_app_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "tenants",
                        "columns": [
                            {"name": "id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "name", "type": "VARCHAR(100)", "not_null": True},
                            {"name": "plan_type", "type": "VARCHAR(50)"},
                            {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"},
                            {"name": "active", "type": "BOOLEAN", "default": "TRUE"},
                        ],
                        "data": [
                            {"name": f"Tenant_{i}", "plan_type": "premium" if i % 3 == 0 else "standard", "active": True}
                            for i in range(1, 21)  # 20 tenants
                        ],
                    },
                    {
                        "name": "users",
                        "columns": [
                            {"name": "id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "tenant_id", "type": "INT", "not_null": True},
                            {"name": "email", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "name", "type": "VARCHAR(100)"},
                            {"name": "role", "type": "VARCHAR(50)"},
                            {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"},
                        ],
                        "data": [
                            {"tenant_id": ((i-1) % 20) + 1, "email": f"user{i}@tenant{((i-1) % 20) + 1}.com", 
                             "name": f"User {i}", "role": "member"}
                            for i in range(1, 101)  # 100 users across tenants
                        ],
                    },
                    {
                        "name": "projects",
                        "columns": [
                            {"name": "id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "tenant_id", "type": "INT", "not_null": True},
                            {"name": "name", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "status", "type": "VARCHAR(50)"},
                            {"name": "created_by", "type": "INT"},
                            {"name": "updated_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"},
                        ],
                        "data": [
                            {"tenant_id": ((i-1) % 20) + 1, "name": f"Project {i}", 
                             "status": "active", "created_by": i}
                            for i in range(1, 51)  # 50 projects
                        ],
                    },
                    {
                        "name": "tasks",
                        "columns": [
                            {"name": "id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "project_id", "type": "INT", "not_null": True},
                            {"name": "title", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "description", "type": "TEXT"},
                            {"name": "status", "type": "VARCHAR(50)"},
                            {"name": "assigned_to", "type": "INT"},
                            {"name": "due_date", "type": "DATE"},
                        ],
                        "data": [
                            {"project_id": ((i-1) % 50) + 1, "title": f"Task {i}", 
                             "description": f"Task description {i}", "status": "pending", 
                             "assigned_to": ((i-1) % 100) + 1, "due_date": "2024-12-31"}
                            for i in range(1, 151)  # 150 tasks
                        ],
                    },
                ],
            }
        ],
    }

    mysql_fixture = MySQLFixture(custom_config=custom_mysql_config)
    return [mysql_fixture]


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
    Validates that the AI agent successfully implemented horizontal sharding.
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes sharding implementation",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "Routing Database Validation",
            "description": "Verify shard_map and shard_registry created",
            "status": "running",
            "Result_Message": "Checking routing database...",
        },
        {
            "name": "Shard Creation Validation",
            "description": "Verify 4 shards created",
            "status": "running",
            "Result_Message": "Validating shard creation...",
        },
        {
            "name": "Data Distribution Validation",
            "description": "Verify tenants distributed across shards",
            "status": "running",
            "Result_Message": "Checking data distribution...",
        },
        {
            "name": "Query Routing Validation",
            "description": "Verify routing functions work correctly",
            "status": "running",
            "Result_Message": "Testing query routing...",
        },
        {
            "name": "Rebalancing Mechanism Validation",
            "description": "Verify tenant migration procedure exists",
            "status": "running",
            "Result_Message": "Checking rebalancing mechanism...",
        },
        {
            "name": "Monitoring Views Validation",
            "description": "Verify monitoring views created",
            "status": "running",
            "Result_Message": "Validating monitoring infrastructure...",
        },
    ]

    try:
        # Step 1: Check agent execution
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed"
            # Mark remaining steps as failed
            for step in test_steps:
                if step["status"] == "running":
                    step["status"] = "failed"
                    step["Result_Message"] = "❌ Skipped due to agent failure"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed successfully"

        # Get MySQL fixture
        mysql_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "mysql_resource"), None
        ) if fixtures else None

        if not mysql_fixture:
            raise Exception("MySQL fixture not found")

        resource_data = getattr(mysql_fixture, "_resource_data", None)
        if not resource_data or not resource_data.get("created_resources"):
            raise Exception("MySQL resource data not available")

        db_name = resource_data["created_resources"][0]["name"]
        db_connection = mysql_fixture.get_connection(database=db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Verify routing database tables (flexible approach)
            print("🔍 Checking routing database...", flush=True)

            # Check for common routing table patterns
            db_cursor.execute("SHOW TABLES")
            all_tables = [table[0].lower() for table in db_cursor.fetchall()]

            # Look for routing-related tables (flexible naming)
            routing_tables = [t for t in all_tables if any(keyword in t for keyword in ['shard', 'routing', 'map', 'registry', 'tenant'])]

            # Check shard_map table (preferred name)
            db_cursor.execute("SHOW TABLES LIKE 'shard_map'")
            has_shard_map = db_cursor.fetchone() is not None

            # Check shard_registry table (preferred name)
            db_cursor.execute("SHOW TABLES LIKE 'shard_registry'")
            has_shard_registry = db_cursor.fetchone() is not None

            if has_shard_map and has_shard_registry:
                # Check if shard_map has mappings
                db_cursor.execute("SELECT COUNT(*) FROM shard_map")
                mapping_count = db_cursor.fetchone()[0]

                # Check if shard_registry has 4 shards
                db_cursor.execute("SELECT COUNT(*) FROM shard_registry")
                shard_count = db_cursor.fetchone()[0]

                if mapping_count >= 15 and shard_count >= 4:
                    test_steps[1]["status"] = "passed"
                    test_steps[1]["Result_Message"] = f"✅ Routing database created: {mapping_count} tenant mappings, {shard_count} shards registered"
                else:
                    test_steps[1]["status"] = "partial"
                    test_steps[1]["Result_Message"] = f"⚠️ Routing tables exist but incomplete: {mapping_count} mappings, {shard_count} shards"
            elif len(routing_tables) >= 1:
                # Alternative routing implementation detected
                test_steps[1]["status"] = "partial"
                test_steps[1]["Result_Message"] = f"⚠️ Alternative routing detected (tables: {', '.join(routing_tables[:3])}), expected shard_map/shard_registry"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ Routing database tables not found"

            # Step 3: Verify shard creation (check for databases or schemas named shard_*)
            print("🔍 Checking for shards...", flush=True)
            
            # Check for databases or schemas with shard naming
            db_cursor.execute("SHOW DATABASES LIKE 'shard_%'")
            shard_databases = db_cursor.fetchall()
            
            # Also check for schemas within current database
            db_cursor.execute("SHOW TABLES LIKE '%shard%'")
            shard_related_tables = db_cursor.fetchall()
            
            total_shards = len(shard_databases) + (1 if len(shard_related_tables) > 0 else 0)
            
            if total_shards >= 4:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ {total_shards} shards detected"
            elif total_shards >= 2:
                test_steps[2]["status"] = "partial"
                test_steps[2]["Result_Message"] = f"⚠️ Only {total_shards} shards found, expected 4"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "❌ Shards not properly created"

            # Step 4: Verify data distribution
            print("🔍 Checking data distribution...", flush=True)
            
            if has_shard_map:
                # Check distribution from shard_map
                db_cursor.execute("""
                    SELECT shard_id, COUNT(*) as tenant_count
                    FROM shard_map
                    GROUP BY shard_id
                    ORDER BY shard_id
                """)
                distribution = db_cursor.fetchall()
                
                if len(distribution) >= 2:
                    counts = [row[1] for row in distribution]
                    min_count = min(counts)
                    max_count = max(counts)
                    variance = max_count - min_count
                    
                    # Check if distribution is reasonably balanced (within 50% variance)
                    avg_count = sum(counts) / len(counts)
                    is_balanced = variance <= avg_count * 0.5
                    
                    test_steps[3]["status"] = "passed" if is_balanced else "partial"
                    test_steps[3]["Result_Message"] = f"{'✅' if is_balanced else '⚠️'} Data distributed across {len(distribution)} shards: {dict(distribution)}"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = "❌ Insufficient data distribution"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ Cannot validate distribution without shard_map"

            # Step 5: Verify query routing functions
            try:
                print("🔍 Checking query routing functions...", flush=True)

                # Check for routing functions
                db_cursor.execute("""
                    SELECT ROUTINE_NAME
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA = %s
                    AND (ROUTINE_NAME LIKE '%shard%' OR ROUTINE_NAME LIKE '%route%')
                    AND ROUTINE_TYPE = 'FUNCTION'
                """, (db_name,))
                routing_functions = db_cursor.fetchall()

                if len(routing_functions) >= 1:
                    test_steps[4]["status"] = "passed"
                    test_steps[4]["Result_Message"] = f"✅ Routing functions implemented: {', '.join([f[0] for f in routing_functions])}"
                else:
                    test_steps[4]["status"] = "partial"
                    test_steps[4]["Result_Message"] = "⚠️ No routing functions found (may use alternative routing method)"
            except Exception as e:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = f"❌ Error checking routing functions: {str(e)}"

            # Step 6: Verify rebalancing procedure
            try:
                print("🔍 Checking rebalancing procedures...", flush=True)

                db_cursor.execute("""
                    SELECT ROUTINE_NAME
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA = %s
                    AND (ROUTINE_NAME LIKE '%move%tenant%' OR ROUTINE_NAME LIKE '%rebalance%' OR ROUTINE_NAME LIKE '%migrate%')
                    AND ROUTINE_TYPE = 'PROCEDURE'
                """, (db_name,))
                rebalancing_procedures = db_cursor.fetchall()

                if len(rebalancing_procedures) >= 1:
                    test_steps[5]["status"] = "passed"
                    test_steps[5]["Result_Message"] = f"✅ Rebalancing procedures implemented: {', '.join([p[0] for p in rebalancing_procedures])}"
                else:
                    test_steps[5]["status"] = "partial"
                    test_steps[5]["Result_Message"] = "⚠️ No rebalancing procedures found (optional feature)"
            except Exception as e:
                test_steps[5]["status"] = "failed"
                test_steps[5]["Result_Message"] = f"❌ Error checking rebalancing procedures: {str(e)}"

            # Step 7: Verify monitoring views
            try:
                print("🔍 Checking monitoring views...", flush=True)

                db_cursor.execute("""
                    SELECT TABLE_NAME
                    FROM information_schema.VIEWS
                    WHERE TABLE_SCHEMA = %s
                    AND (TABLE_NAME LIKE '%distribution%' OR TABLE_NAME LIKE '%capacity%' OR TABLE_NAME LIKE '%shard%')
                """, (db_name,))
                monitoring_views = db_cursor.fetchall()

                if len(monitoring_views) >= 1:
                    test_steps[6]["status"] = "passed"
                    test_steps[6]["Result_Message"] = f"✅ Monitoring views created: {', '.join([v[0] for v in monitoring_views])}"
                else:
                    test_steps[6]["status"] = "partial"
                    test_steps[6]["Result_Message"] = "⚠️ No monitoring views found (recommended but optional)"
            except Exception as e:
                test_steps[6]["status"] = "failed"
                test_steps[6]["Result_Message"] = f"❌ Error checking monitoring views: {str(e)}"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score with partial credit (passed=1.0, partial=0.5, failed=0.0)
    score_sum = sum([
        1.0 if step["status"] == "passed" else (0.5 if step["status"] == "partial" else 0.0)
        for step in test_steps
    ])
    score = score_sum / len(test_steps)

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
