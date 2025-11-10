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
    This MySQL test validates schema migration and data validation capabilities.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with source data that needs to be migrated to a new schema
    custom_mysql_config = {
        "resource_id": f"mysql_migration_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"source_system_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "sample_data",
                        "columns": [
                            {
                                "name": "id",
                                "type": "BIGINT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "category", "type": "VARCHAR(50)"},
                            {"name": "value", "type": "DECIMAL(15,4)"},
                            {"name": "description", "type": "TEXT"},
                            {
                                "name": "created_at",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                        ],
                        # Business metrics data suitable for star schema transformation
                        "data": [
                            {
                                "category": "Revenue",
                                "value": 125000.75,
                                "description": "Daily revenue from online sales",
                            },
                            {
                                "category": "Revenue",
                                "value": 98500.25,
                                "description": "Daily revenue from retail stores",
                            },
                            {
                                "category": "Customers",
                                "value": 1575.00,
                                "description": "New customer acquisitions",
                            },
                            {
                                "category": "Orders",
                                "value": 2450.00,
                                "description": "Total orders processed",
                            },
                            {
                                "category": "Inventory",
                                "value": 89700.75,
                                "description": "Current inventory value",
                            },
                            {
                                "category": "Marketing",
                                "value": 15000.00,
                                "description": "Daily marketing spend",
                            },
                            {
                                "category": "Support",
                                "value": 247.00,
                                "description": "Customer support tickets",
                            },
                            {
                                "category": "Performance",
                                "value": 98.5,
                                "description": "System uptime percentage",
                            },
                            {
                                "category": "Quality",
                                "value": 4.7,
                                "description": "Average product rating",
                            },
                            {
                                "category": "Operations",
                                "value": 156.25,
                                "description": "Operational efficiency score",
                            },
                            {
                                "category": "Revenue",
                                "value": 145000.50,
                                "description": "Weekend revenue spike",
                            },
                            {
                                "category": "Customers",
                                "value": 892.00,
                                "description": "Customer retention count",
                            },
                        ],
                    },
                    {
                        "name": "data_processing_log",
                        "columns": [
                            {
                                "name": "job_id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {
                                "name": "job_name",
                                "type": "VARCHAR(100)",
                                "not_null": True,
                            },
                            {
                                "name": "status",
                                "type": "ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
                            },
                            {"name": "start_time", "type": "TIMESTAMP"},
                            {"name": "end_time", "type": "TIMESTAMP"},
                            {"name": "records_processed", "type": "BIGINT"},
                        ],
                        # ETL job execution data suitable for fact table transformation
                        "data": [
                            {
                                "job_name": "daily_sales_extract",
                                "status": "COMPLETED",
                                "start_time": "2024-09-26 08:00:00",
                                "end_time": "2024-09-26 08:45:30",
                                "records_processed": 1250000,
                            },
                            {
                                "job_name": "hourly_customer_transform",
                                "status": "COMPLETED",
                                "start_time": "2024-09-26 09:00:00",
                                "end_time": "2024-09-26 09:15:20",
                                "records_processed": 75000,
                            },
                            {
                                "job_name": "product_catalog_load",
                                "status": "COMPLETED",
                                "start_time": "2024-09-26 10:00:00",
                                "end_time": "2024-09-26 10:35:45",
                                "records_processed": 500000,
                            },
                            {
                                "job_name": "inventory_sync",
                                "status": "COMPLETED",
                                "start_time": "2024-09-26 11:00:00",
                                "end_time": "2024-09-26 11:12:15",
                                "records_processed": 25000,
                            },
                            {
                                "job_name": "financial_reconciliation",
                                "status": "FAILED",
                                "start_time": "2024-09-26 07:30:00",
                                "end_time": "2024-09-26 07:45:15",
                                "records_processed": 0,
                            },
                            {
                                "job_name": "marketing_analytics",
                                "status": "COMPLETED",
                                "start_time": "2024-09-25 14:00:00",
                                "end_time": "2024-09-25 14:22:30",
                                "records_processed": 89500,
                            },
                            {
                                "job_name": "customer_segmentation",
                                "status": "PENDING",
                                "start_time": None,
                                "end_time": None,
                                "records_processed": None,
                            },
                        ],
                    },
                    {
                        "name": "job_metadata",
                        "columns": [
                            {
                                "name": "job_name",
                                "type": "VARCHAR(100)",
                                "primary_key": True,
                            },
                            {
                                "name": "job_type",
                                "type": "VARCHAR(50)",
                                "not_null": True,
                            },
                            {
                                "name": "job_category",
                                "type": "VARCHAR(50)",
                                "not_null": True,
                            },
                            {"name": "job_description", "type": "TEXT"},
                            {"name": "schedule_frequency", "type": "VARCHAR(20)"},
                            {"name": "priority", "type": "INT", "default": "5"},
                            {"name": "max_runtime_minutes", "type": "INT"},
                            {"name": "is_active", "type": "BOOLEAN", "default": "TRUE"},
                            {
                                "name": "created_date",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                        ],
                        # Job configuration and metadata for dimension table creation
                        "data": [
                            {
                                "job_name": "daily_sales_extract",
                                "job_type": "EXTRACT",
                                "job_category": "Sales",
                                "job_description": "Extract daily sales data from transactional systems",
                                "schedule_frequency": "DAILY",
                                "priority": 3,
                                "max_runtime_minutes": 60,
                                "is_active": True,
                            },
                            {
                                "job_name": "hourly_customer_transform",
                                "job_type": "TRANSFORM",
                                "job_category": "Customer",
                                "job_description": "Transform and cleanse customer data for analytics",
                                "schedule_frequency": "HOURLY",
                                "priority": 2,
                                "max_runtime_minutes": 30,
                                "is_active": True,
                            },
                            {
                                "job_name": "product_catalog_load",
                                "job_type": "LOAD",
                                "job_category": "Product",
                                "job_description": "Load product catalog data into warehouse",
                                "schedule_frequency": "DAILY",
                                "priority": 4,
                                "max_runtime_minutes": 45,
                                "is_active": True,
                            },
                            {
                                "job_name": "inventory_sync",
                                "job_type": "SYNC",
                                "job_category": "Inventory",
                                "job_description": "Synchronize inventory levels across systems",
                                "schedule_frequency": "HOURLY",
                                "priority": 1,
                                "max_runtime_minutes": 15,
                                "is_active": True,
                            },
                            {
                                "job_name": "financial_reconciliation",
                                "job_type": "VALIDATE",
                                "job_category": "Finance",
                                "job_description": "Reconcile financial data across systems",
                                "schedule_frequency": "DAILY",
                                "priority": 2,
                                "max_runtime_minutes": 30,
                                "is_active": True,
                            },
                            {
                                "job_name": "marketing_analytics",
                                "job_type": "ANALYZE",
                                "job_category": "Marketing",
                                "job_description": "Generate marketing performance analytics",
                                "schedule_frequency": "DAILY",
                                "priority": 5,
                                "max_runtime_minutes": 90,
                                "is_active": True,
                            },
                            {
                                "job_name": "customer_segmentation",
                                "job_type": "ANALYZE",
                                "job_category": "Customer",
                                "job_description": "Customer segmentation and behavioral analysis",
                                "schedule_frequency": "WEEKLY",
                                "priority": 4,
                                "max_runtime_minutes": 120,
                                "is_active": False,
                            },
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
    This function has access to all fixture data after setup.
    """
    from extract_test_configs import create_config_from_fixtures

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented a star schema migration.

    Expected behavior:
    - New data warehouse database created
    - Proper star schema with fact and dimension tables
    - Fact table (fact_etl_jobs) with metrics and foreign keys
    - Dimension tables (dim_jobs, dim_time, dim_status, dim_categories)
    - Star schema relationships and integrity constraints`
    - Analytics infrastructure (views, procedures)

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes star schema migration task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the star schema migration...",
        },
        {
            "name": "Data Warehouse Creation",
            "description": "Verify new data warehouse database was created",
            "status": "running",
            "Result_Message": "Validating data warehouse creation...",
        },
        {
            "name": "Star Schema Structure",
            "description": "Verify fact and dimension tables exist",
            "status": "running",
            "Result_Message": "Validating star schema structure...",
        },
        {
            "name": "Star Schema Relationships",
            "description": "Verify foreign key relationships and constraints",
            "status": "running",
            "Result_Message": "Validating star schema relationships...",
        },
        {
            "name": "Analytics Infrastructure",
            "description": "Verify views, procedures, and analytics capabilities",
            "status": "running",
            "Result_Message": "Validating analytics infrastructure...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Get MySQL fixture for validation
        mysql_fixture = None
        if fixtures:
            mysql_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mysql_resource"), None
            )

        if not mysql_fixture:
            raise Exception("MySQL fixture not found")

        # Get database connection
        resource_data = getattr(mysql_fixture, "_resource_data", None)
        if not resource_data or not resource_data.get("created_resources"):
            raise Exception("MySQL resource data not available")

        source_db_name = resource_data["created_resources"][0]["name"]
        if not source_db_name:
            raise Exception("Source database name not available")

        db_connection = (
            mysql_fixture.get_connection()
        )  # Connect without specific database
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Check if new data warehouse was created
            print(f"DEBUG: About to execute first query", flush=True)
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'data_warehouse'
            """
            )
            data_warehouse_exists = db_cursor.fetchone()[0] > 0
            print(
                f"DEBUG: First query completed, data_warehouse_exists={data_warehouse_exists}",
                flush=True,
            )

            if data_warehouse_exists:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = "✅ Data warehouse 'data_warehouse' created successfully"
                target_schema = "data_warehouse"
            else:
                # Also check for any new database that was created (might have different name)
                print(
                    f"DEBUG: About to execute warehouse query with source_db_name='{source_db_name}'",
                    flush=True,
                )
                db_cursor.execute(
                    """
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys', %s)
                    AND (schema_name LIKE '%warehouse%' OR schema_name LIKE '%star%' OR schema_name LIKE '%dim%')
                """,
                    (source_db_name,),
                )
                print(f"DEBUG: Warehouse query completed", flush=True)
                warehouse_schemas = db_cursor.fetchall()

                if warehouse_schemas:
                    test_steps[1]["status"] = "passed"
                    target_schema = warehouse_schemas[0][0]
                    if not target_schema:
                        raise Exception("Target schema name is empty")
                    test_steps[1][
                        "Result_Message"
                    ] = f"✅ Data warehouse '{target_schema}' created successfully"
                else:
                    # Check for any new schema at all
                    print(
                        f"DEBUG: About to execute any_new_schemas query with source_db_name='{source_db_name}'",
                        flush=True,
                    )
                    db_cursor.execute(
                        """
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys', %s)
                    """,
                        (source_db_name,),
                    )
                    print(f"DEBUG: Any new schemas query completed", flush=True)
                    any_new_schemas = db_cursor.fetchall()

                    if any_new_schemas:
                        test_steps[1]["status"] = "passed"
                        target_schema = any_new_schemas[0][0]
                        if not target_schema:
                            raise Exception("Target schema name is empty")
                        test_steps[1][
                            "Result_Message"
                        ] = f"✅ New database '{target_schema}' created"
                    else:
                        test_steps[1]["status"] = "failed"
                        test_steps[1][
                            "Result_Message"
                        ] = "❌ No new data warehouse database created"
                        return {"score": 0.2, "metadata": {"test_steps": test_steps}}

            # Step 3: Validate star schema structure
            # Check for fact and dimension tables
            if not target_schema:
                raise Exception("Target schema not properly defined")

            print(
                f"DEBUG: About to execute tables query with target_schema='{target_schema}'",
                flush=True,
            )
            db_cursor.execute(
                """
                SELECT table_name, table_rows 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """,
                (target_schema,),
            )
            print(f"DEBUG: Tables query completed", flush=True)
            all_tables = db_cursor.fetchall()
            table_names = [table[0] for table in all_tables]

            # Look for fact table
            fact_tables = [name for name in table_names if "fact" in name.lower()]

            # Look for dimension tables
            dim_tables = [name for name in table_names if "dim" in name.lower()]

            star_schema_score = 0
            star_schema_details = []

            if fact_tables:
                star_schema_score += 1
                star_schema_details.append(f"{len(fact_tables)} fact table(s)")

            if len(dim_tables) >= 3:  # Expecting at least 3 dimension tables
                star_schema_score += 1
                star_schema_details.append(f"{len(dim_tables)} dimension tables")
            elif len(dim_tables) >= 1:
                star_schema_details.append(
                    f"{len(dim_tables)} dimension tables (expected ≥3)"
                )

            if star_schema_score >= 2:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = (
                    f"✅ Star schema structure validated: {', '.join(star_schema_details)} "
                    f"(Tables: {', '.join(table_names)})"
                )
            elif (
                len(all_tables) >= 4
            ):  # At least have some tables that could be star schema
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = (
                    f"✅ Multi-table structure created: {len(all_tables)} tables "
                    f"(Tables: {', '.join(table_names)})"
                )
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = (
                    f"❌ Insufficient star schema: {', '.join(star_schema_details) if star_schema_details else 'no fact/dim tables'} "
                    f"(Found: {', '.join(table_names)})"
                )

            # Step 4: Validate star schema relationships and constraints
            relationship_checks = []

            # Check for foreign key constraints in fact table
            if fact_tables:
                fact_table = fact_tables[0]
                db_cursor.execute(
                    """
                    SELECT 
                        COLUMN_NAME,
                        REFERENCED_TABLE_NAME,
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE 
                    WHERE table_schema = %s 
                    AND table_name = %s
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """,
                    (target_schema, fact_table),
                )

                foreign_keys = db_cursor.fetchall()
                if foreign_keys:
                    relationship_checks.append(
                        f"{len(foreign_keys)} foreign keys in fact table"
                    )

                # Check if fact table has data
                try:
                    db_cursor.execute(
                        f"SELECT COUNT(*) FROM {target_schema}.{fact_table}"
                    )
                    fact_count = db_cursor.fetchone()[0]
                    if fact_count > 0:
                        relationship_checks.append(
                            f"{fact_count} records in fact table"
                        )
                except Exception:
                    pass

            # Check for surrogate keys in dimension tables
            surrogate_key_count = 0
            for dim_table in dim_tables:
                try:
                    # Look for columns ending with '_key' or '_id' that might be surrogate keys
                    db_cursor.execute(
                        """
                        SELECT COLUMN_NAME 
                        FROM information_schema.COLUMNS 
                        WHERE table_schema = %s 
                        AND table_name = %s
                        AND (COLUMN_NAME LIKE '%_key' OR COLUMN_NAME LIKE '%_id')
                        AND COLUMN_KEY = 'PRI'
                    """,
                        (target_schema, dim_table),
                    )

                    surrogate_keys = db_cursor.fetchall()
                    if surrogate_keys:
                        surrogate_key_count += 1
                except Exception:
                    pass

            if surrogate_key_count > 0:
                relationship_checks.append(
                    f"{surrogate_key_count} dimension tables with surrogate keys"
                )

            # Check for proper indexing on fact table
            if fact_tables:
                try:
                    db_cursor.execute(
                        f"SHOW INDEX FROM {target_schema}.{fact_tables[0]}"
                    )
                    indexes = db_cursor.fetchall()
                    non_primary_indexes = [
                        idx for idx in indexes if idx[2] != "PRIMARY"
                    ]
                    if non_primary_indexes:
                        relationship_checks.append(
                            f"{len(non_primary_indexes)} indexes on fact table"
                        )
                except Exception:
                    pass

            # Evaluate relationship checks
            if len(relationship_checks) >= 3:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Star schema relationships validated: {', '.join(relationship_checks)}"
            elif len(relationship_checks) >= 1:
                test_steps[3]["status"] = "passed"
                test_steps[3][
                    "Result_Message"
                ] = f"✅ Basic relationships found: {', '.join(relationship_checks)}"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = "❌ No star schema relationships or constraints found"

            # Step 5: Check for analytics infrastructure
            analytics_components = []

            # Check for analytical views in the new schema
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.views 
                WHERE table_schema = %s
            """,
                (target_schema,),
            )

            view_count = db_cursor.fetchone()[0]
            if view_count > 0:
                analytics_components.append(f"{view_count} analytical views")

            # Check for stored procedures for data warehouse maintenance
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.routines 
                WHERE routine_schema = %s 
                AND routine_type = 'PROCEDURE'
            """,
                (target_schema,),
            )

            procedure_count = db_cursor.fetchone()[0]
            if procedure_count > 0:
                analytics_components.append(f"{procedure_count} maintenance procedures")

            # Check for proper indexing across all tables
            db_cursor.execute(
                """
                SELECT COUNT(DISTINCT index_name) 
                FROM information_schema.statistics 
                WHERE table_schema = %s 
                AND index_name != 'PRIMARY'
            """,
                (target_schema,),
            )

            index_count = db_cursor.fetchone()[0]
            if index_count >= len(all_tables):  # At least one index per table
                analytics_components.append(f"{index_count} performance indexes")

            # Check for time dimension data (if dim_time table exists)
            time_dim_tables = [name for name in table_names if "time" in name.lower()]
            if time_dim_tables:
                try:
                    db_cursor.execute(
                        f"SELECT COUNT(*) FROM {target_schema}.{time_dim_tables[0]}"
                    )
                    time_records = db_cursor.fetchone()[0]
                    if time_records >= 30:  # At least a month of time dimension data
                        analytics_components.append(
                            f"time dimension with {time_records} records"
                        )
                except Exception:
                    pass

            # Check for audit/lineage columns in tables
            audit_columns_found = 0
            for table_name in table_names[:3]:  # Check first 3 tables
                try:
                    db_cursor.execute(
                        """
                        SELECT COUNT(*) 
                        FROM information_schema.COLUMNS 
                        WHERE table_schema = %s 
                        AND table_name = %s
                        AND (COLUMN_NAME LIKE '%created%' OR COLUMN_NAME LIKE '%updated%' 
                             OR COLUMN_NAME LIKE '%source%' OR COLUMN_NAME LIKE '%lineage%')
                    """,
                        (target_schema, table_name),
                    )

                    audit_cols = db_cursor.fetchone()[0]
                    if audit_cols > 0:
                        audit_columns_found += 1
                except Exception:
                    pass

            if audit_columns_found >= 2:
                analytics_components.append("audit/lineage tracking")

            # Evaluate analytics infrastructure
            if len(analytics_components) >= 3:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Analytics infrastructure implemented: {', '.join(analytics_components)}"
            elif len(analytics_components) >= 1:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Basic analytics infrastructure: {', '.join(analytics_components)}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = "❌ No analytics infrastructure found"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
