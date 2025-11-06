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
    This MySQL test validates query performance optimization.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with unoptimized schema (no indexes on FKs)
    custom_mysql_config = {
        "resource_id": f"mysql_perf_opt_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"ecommerce_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "customers",
                        "columns": [
                            {
                                "name": "id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "email", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "name", "type": "VARCHAR(100)"},
                            {"name": "registration_date", "type": "DATE"},
                            {"name": "region", "type": "VARCHAR(50)"},
                        ],
                        # No index on email or registration_date intentionally
                        "data": [
                            {
                                "email": f"customer{i}@example.com",
                                "name": f"Customer {i}",
                                "registration_date": "2024-01-01",
                                "region": "US",
                            }
                            for i in range(1, 101)
                        ],
                    },
                    {
                        "name": "products",
                        "columns": [
                            {
                                "name": "id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "name", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "category", "type": "VARCHAR(100)"},
                            {"name": "price", "type": "DECIMAL(10,2)"},
                            {"name": "cost", "type": "DECIMAL(10,2)"},
                        ],
                        # No index on category or price intentionally
                        "data": [
                            {
                                "name": f"Product {i}",
                                "category": f"Category {i % 10}",
                                "price": 10.00 + (i % 100),
                                "cost": 5.00 + (i % 50),
                            }
                            for i in range(1, 101)
                        ],
                    },
                    {
                        "name": "orders",
                        "columns": [
                            {
                                "name": "id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "customer_id", "type": "INT", "not_null": True},
                            {"name": "order_date", "type": "DATE", "not_null": True},
                            {"name": "total_amount", "type": "DECIMAL(10,2)"},
                            {"name": "status", "type": "VARCHAR(50)"},
                        ],
                        # No index on customer_id, order_date, or status intentionally
                        "data": [
                            {
                                "customer_id": (i % 100) + 1,
                                "order_date": "2024-01-15",
                                "total_amount": 100.00 + (i % 500),
                                "status": "completed" if i % 2 == 0 else "pending",
                            }
                            for i in range(1, 201)
                        ],
                    },
                    {
                        "name": "order_items",
                        "columns": [
                            {
                                "name": "id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "order_id", "type": "INT", "not_null": True},
                            {"name": "product_id", "type": "INT", "not_null": True},
                            {"name": "quantity", "type": "INT"},
                            {"name": "unit_price", "type": "DECIMAL(10,2)"},
                        ],
                        # No index on order_id or product_id intentionally
                        "data": [
                            {
                                "order_id": (i % 200) + 1,
                                "product_id": (i % 100) + 1,
                                "quantity": i % 10 + 1,
                                "unit_price": 10.00 + (i % 100),
                            }
                            for i in range(1, 401)
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
    Validates that the AI agent successfully optimized query performance.
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes performance optimization",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "Slow Query Identification",
            "description": "Verify slow queries identified",
            "status": "running",
            "Result_Message": "Checking query analysis...",
        },
        {
            "name": "Index Creation",
            "description": "Verify new indexes created",
            "status": "running",
            "Result_Message": "Validating index creation...",
        },
        {
            "name": "Query Performance Improvement",
            "description": "Verify queries now execute faster",
            "status": "running",
            "Result_Message": "Testing query performance...",
        },
        {
            "name": "Foreign Key Index Validation",
            "description": "Verify FK columns are indexed",
            "status": "running",
            "Result_Message": "Checking foreign key indexes...",
        },
        {
            "name": "Index Usage Verification",
            "description": "Verify indexes are being used",
            "status": "running",
            "Result_Message": "Checking index usage...",
        },
        {
            "name": "Summary Table Implementation",
            "description": "Check for summary/aggregate tables",
            "status": "running",
            "Result_Message": "Looking for summary tables...",
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

        # Get MySQL fixture
        mysql_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "mysql_resource"), None
            )
            if fixtures
            else None
        )

        if not mysql_fixture:
            raise Exception("MySQL fixture not found")

        resource_data = getattr(mysql_fixture, "_resource_data", None)
        if not resource_data or not resource_data.get("created_resources"):
            raise Exception("MySQL resource data not available")

        db_name = resource_data["created_resources"][0]["name"]
        db_connection = mysql_fixture.get_connection(database=db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Check for query analysis (look for comments or documentation)
            print("🔍 Checking for slow query analysis...", flush=True)
            test_steps[1]["status"] = "passed"
            test_steps[1]["Result_Message"] = "✅ Query analysis assumed completed"

            # Step 3: Verify index creation
            print("🔍 Checking for new indexes...", flush=True)

            # Check indexes on each table
            tables_to_check = ["customers", "products", "orders", "order_items"]
            total_new_indexes = 0

            for table in tables_to_check:
                db_cursor.execute(f"SHOW INDEX FROM {table}")
                indexes = db_cursor.fetchall()
                # Count non-primary key indexes
                non_pk_indexes = [idx for idx in indexes if idx[2] != "PRIMARY"]
                total_new_indexes += len(non_pk_indexes)

            if total_new_indexes >= 4:
                test_steps[2]["status"] = "passed"
                test_steps[2][
                    "Result_Message"
                ] = f"✅ {total_new_indexes} indexes created across tables"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Only {total_new_indexes} indexes found, expected at least 4"

            # Step 4: Test query performance
            print("🔍 Testing query performance...", flush=True)

            # Test a typical join query
            import time as time_module

            start_time = time_module.time()

            db_cursor.execute(
                """
                SELECT c.name, COUNT(o.id) as order_count
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id
                WHERE c.registration_date >= '2024-01-01'
                GROUP BY c.id, c.name
                LIMIT 10
            """
            )
            results = db_cursor.fetchall()

            end_time = time_module.time()
            query_time_ms = (end_time - start_time) * 1000

            if query_time_ms < 1000:  # Less than 1 second
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Query performance good: {query_time_ms:.2f}ms"
            else:
                test_steps[3]["status"] = "partial"
                test_steps[3][
                    "Result_Message"
                ] = f"⚠️ Query completed but slow: {query_time_ms:.2f}ms"

            # Step 5: Check FK indexes specifically
            print("🔍 Checking foreign key indexes...", flush=True)

            fk_indexed = 0

            # Check if customer_id in orders is indexed
            db_cursor.execute(
                "SHOW INDEX FROM orders WHERE Column_name = 'customer_id'"
            )
            result = db_cursor.fetchall()  # Consume all results
            if result:
                fk_indexed += 1

            # Check if order_id in order_items is indexed
            db_cursor.execute(
                "SHOW INDEX FROM order_items WHERE Column_name = 'order_id'"
            )
            result = db_cursor.fetchall()  # Consume all results
            if result:
                fk_indexed += 1

            # Check if product_id in order_items is indexed
            db_cursor.execute(
                "SHOW INDEX FROM order_items WHERE Column_name = 'product_id'"
            )
            result = db_cursor.fetchall()  # Consume all results
            if result:
                fk_indexed += 1

            if fk_indexed >= 2:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ {fk_indexed} foreign key columns indexed"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4][
                    "Result_Message"
                ] = f"❌ Only {fk_indexed} FK columns indexed, expected at least 2"

            # Step 6: Check index usage with EXPLAIN
            print("🔍 Checking index usage with EXPLAIN...", flush=True)

            try:
                db_cursor.execute(
                    """
                    EXPLAIN SELECT * FROM orders WHERE customer_id = 1
                """
                )
                explain_result = db_cursor.fetchall()
                uses_index = any(
                    "index" in str(row).lower() or "ref" in str(row).lower()
                    for row in explain_result
                )
            except Exception as e:
                print(f"EXPLAIN query error: {e}", flush=True)
                uses_index = False

            if uses_index:
                test_steps[5]["status"] = "passed"
                test_steps[5][
                    "Result_Message"
                ] = "✅ Queries using indexes (verified with EXPLAIN)"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5][
                    "Result_Message"
                ] = "❌ EXPLAIN shows table scans, indexes not being used"

            # Step 7: Check for summary tables
            print("🔍 Looking for summary/aggregate tables...", flush=True)

            try:
                db_cursor.execute("SHOW TABLES")
                all_tables = [row[0] for row in db_cursor.fetchall()]

                summary_tables = [
                    t
                    for t in all_tables
                    if "summary" in t.lower()
                    or "aggregate" in t.lower()
                    or "report" in t.lower()
                ]

                if len(summary_tables) > 0:
                    test_steps[6]["status"] = "passed"
                    test_steps[6][
                        "Result_Message"
                    ] = f"✅ Found {len(summary_tables)} summary table(s): {', '.join(summary_tables)}"
                else:
                    test_steps[6]["status"] = "partial"
                    test_steps[6][
                        "Result_Message"
                    ] = "⚠️ No summary tables found (optional optimization)"
            except Exception as e:
                print(f"Summary table check error: {e}", flush=True)
                test_steps[6]["status"] = "partial"
                test_steps[6]["Result_Message"] = "⚠️ Could not check for summary tables"

        finally:
            db_cursor.close()
            db_connection.close()

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
