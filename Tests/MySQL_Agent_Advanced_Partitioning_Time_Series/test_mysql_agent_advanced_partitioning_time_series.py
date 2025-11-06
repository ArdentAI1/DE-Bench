# Braintrust-only MySQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import datetime
import mysql.connector
from typing import Any, Dict, List, Optional
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_month_start(
    months_ago: int, hour: Optional[int] = 0, minute: Optional[int] = 0
) -> datetime.datetime:
    """
    Get the first day of the target month

    :param int months_ago: The number of months ago to get the first day of
    :param Optional[int] hour: The hour of the day
    :param Optional[int] minute: The minute of the hour
    :return: The first day of the target month
    """
    today = datetime.datetime.now()
    # Calculate the first day of the target month
    year = today.year
    month = today.month - months_ago
    while month <= 0:
        month += 12
        year -= 1
    return datetime.datetime(year, month, 1, hour, minute, 0)


def get_month_mid(
    months_ago: int,
    day: Optional[int] = 15,
    hour: Optional[int] = 12,
    minute: Optional[int] = 0,
) -> datetime.datetime:
    """
    Get the middle day of the target month

    :param int months_ago: The number of months ago to get the middle day of
    :param Optional[int] day: The day of the month
    :param Optional[int] hour: The hour of the day
    :param Optional[int] minute: The minute of the hour
    :return: The middle day of the target month
    """
    today = datetime.datetime.now()
    year = today.year
    month = today.month - months_ago
    while month <= 0:
        month += 12
        year -= 1
    return datetime.datetime(year, month, day, hour, minute, 0)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This MySQL test validates advanced partitioning for time-series analytics.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Initialize MySQL fixture with comprehensive time-series sensor data
    custom_mysql_config = {
        "resource_id": f"mysql_partitioning_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"timeseries_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "sensor_readings",
                        "columns": [
                            {
                                "name": "reading_id",
                                "type": "BIGINT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "sensor_id", "type": "INT", "not_null": True},
                            {
                                "name": "reading_timestamp",
                                "type": "DATETIME",
                                "not_null": True,
                            },
                            {"name": "temperature", "type": "DECIMAL(5,2)"},
                            {"name": "humidity", "type": "DECIMAL(5,2)"},
                            {"name": "pressure", "type": "DECIMAL(8,2)"},
                            {"name": "location", "type": "VARCHAR(100)"},
                        ],
                        # Time-series sensor data spanning multiple months for partitioning demo
                        "data": [
                            # Dynamically generate reading_timestamps based on current date
                            # This block generates timestamps for current month and previous months
                            # to simulate time-series data for partitioning
                            # Current Month Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(0, 8, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 22.5,
                                "humidity": 65.2,
                                "pressure": 1013.25,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(0, 8, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 23.1,
                                "humidity": 62.8,
                                "pressure": 1012.80,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(0, 8, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 21.8,
                                "humidity": 68.5,
                                "pressure": 1014.10,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_mid(
                                    0, 15, 14, 30
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 25.2,
                                "humidity": 58.3,
                                "pressure": 1015.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_mid(
                                    0, 15, 14, 30
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 24.8,
                                "humidity": 60.1,
                                "pressure": 1014.90,
                                "location": "Building_A_Floor_2",
                            },
                            # Previous Month Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(1, 9, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 19.5,
                                "humidity": 72.1,
                                "pressure": 1016.20,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(1, 9, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 20.2,
                                "humidity": 69.8,
                                "pressure": 1015.75,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(1, 9, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 18.9,
                                "humidity": 75.2,
                                "pressure": 1017.30,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 104,
                                "reading_timestamp": get_month_mid(
                                    1, 15, 16, 45
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 17.3,
                                "humidity": 78.5,
                                "pressure": 1018.10,
                                "location": "Building_C_Floor_1",
                            },
                            {
                                "sensor_id": 105,
                                "reading_timestamp": get_month_mid(
                                    1, 15, 16, 45
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 18.1,
                                "humidity": 76.2,
                                "pressure": 1017.85,
                                "location": "Building_C_Floor_2",
                            },
                            # Two Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(
                                    2, 10, 15
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 15.8,
                                "humidity": 82.3,
                                "pressure": 1019.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(
                                    2, 10, 15
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 16.5,
                                "humidity": 80.1,
                                "pressure": 1018.90,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(
                                    2, 10, 15
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 14.9,
                                "humidity": 84.7,
                                "pressure": 1020.20,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 104,
                                "reading_timestamp": get_month_mid(
                                    2, 15, 12, 30
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 13.2,
                                "humidity": 87.1,
                                "pressure": 1021.15,
                                "location": "Building_C_Floor_1",
                            },
                            {
                                "sensor_id": 105,
                                "reading_timestamp": get_month_mid(
                                    2, 15, 12, 30
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 14.0,
                                "humidity": 85.8,
                                "pressure": 1020.75,
                                "location": "Building_C_Floor_2",
                            },
                            # Three Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(3, 7, 20).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 12.5,
                                "humidity": 89.2,
                                "pressure": 1022.30,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(3, 7, 20).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 13.1,
                                "humidity": 87.5,
                                "pressure": 1021.85,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(3, 7, 20).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 11.8,
                                "humidity": 91.3,
                                "pressure": 1023.10,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 106,
                                "reading_timestamp": get_month_mid(
                                    3, 15, 15, 40
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 10.9,
                                "humidity": 93.1,
                                "pressure": 1024.25,
                                "location": "Building_D_Floor_1",
                            },
                            {
                                "sensor_id": 107,
                                "reading_timestamp": get_month_mid(
                                    3, 15, 15, 40
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 11.6,
                                "humidity": 91.8,
                                "pressure": 1023.75,
                                "location": "Building_D_Floor_2",
                            },
                            # Four Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(4, 6, 30).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 11.5,
                                "humidity": 92.2,
                                "pressure": 1024.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(4, 6, 30).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 12.1,
                                "humidity": 90.5,
                                "pressure": 1023.90,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(4, 6, 30).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 10.8,
                                "humidity": 94.3,
                                "pressure": 1025.20,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 108,
                                "reading_timestamp": get_month_mid(
                                    4, 15, 18, 50
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 9.9,
                                "humidity": 96.1,
                                "pressure": 1026.15,
                                "location": "Building_E_Floor_1",
                            },
                            {
                                "sensor_id": 109,
                                "reading_timestamp": get_month_mid(
                                    4, 15, 18, 50
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 10.6,
                                "humidity": 94.8,
                                "pressure": 1025.75,
                                "location": "Building_E_Floor_2",
                            },
                            # Five Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(5, 5, 45).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 10.5,
                                "humidity": 93.3,
                                "pressure": 1025.55,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(5, 5, 45).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 11.1,
                                "humidity": 91.6,
                                "pressure": 1025.00,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(5, 5, 45).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 9.8,
                                "humidity": 95.4,
                                "pressure": 1026.30,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 110,
                                "reading_timestamp": get_month_mid(
                                    5, 15, 20, 25
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 8.9,
                                "humidity": 97.2,
                                "pressure": 1027.25,
                                "location": "Building_F_Floor_1",
                            },
                            {
                                "sensor_id": 111,
                                "reading_timestamp": get_month_mid(
                                    5, 15, 20, 25
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 9.6,
                                "humidity": 95.9,
                                "pressure": 1026.75,
                                "location": "Building_F_Floor_2",
                            },
                            # Six Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(6, 4, 50).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 9.5,
                                "humidity": 94.4,
                                "pressure": 1027.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(6, 4, 50).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 10.1,
                                "humidity": 92.7,
                                "pressure": 1026.90,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(6, 4, 50).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 8.8,
                                "humidity": 96.5,
                                "pressure": 1028.20,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 112,
                                "reading_timestamp": get_month_mid(
                                    6, 15, 22, 10
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 7.9,
                                "humidity": 98.3,
                                "pressure": 1029.15,
                                "location": "Building_G_Floor_1",
                            },
                            {
                                "sensor_id": 113,
                                "reading_timestamp": get_month_mid(
                                    6, 15, 22, 10
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 8.6,
                                "humidity": 97.0,
                                "pressure": 1028.75,
                                "location": "Building_G_Floor_2",
                            },
                            # Seven Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(7, 3, 55).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 8.5,
                                "humidity": 95.5,
                                "pressure": 1029.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(7, 3, 55).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 9.1,
                                "humidity": 93.8,
                                "pressure": 1028.90,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(7, 3, 55).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 7.8,
                                "humidity": 97.6,
                                "pressure": 1030.20,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 114,
                                "reading_timestamp": get_month_mid(
                                    7, 15, 23, 40
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 6.9,
                                "humidity": 99.4,
                                "pressure": 1031.15,
                                "location": "Building_H_Floor_1",
                            },
                            {
                                "sensor_id": 115,
                                "reading_timestamp": get_month_mid(
                                    7, 15, 23, 40
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 7.6,
                                "humidity": 98.1,
                                "pressure": 1030.75,
                                "location": "Building_H_Floor_2",
                            },
                            # Eight Months Ago Data
                            {
                                "sensor_id": 101,
                                "reading_timestamp": get_month_start(8, 3, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 7.5,
                                "humidity": 96.5,
                                "pressure": 1031.45,
                                "location": "Building_A_Floor_1",
                            },
                            {
                                "sensor_id": 102,
                                "reading_timestamp": get_month_start(8, 3, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 8.1,
                                "humidity": 94.8,
                                "pressure": 1030.90,
                                "location": "Building_A_Floor_2",
                            },
                            {
                                "sensor_id": 103,
                                "reading_timestamp": get_month_start(8, 3, 0).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "temperature": 6.8,
                                "humidity": 98.6,
                                "pressure": 1032.20,
                                "location": "Building_B_Floor_1",
                            },
                            {
                                "sensor_id": 116,
                                "reading_timestamp": get_month_mid(
                                    8, 15, 23, 59
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 5.9,
                                "humidity": 100.4,
                                "pressure": 1033.15,
                                "location": "Building_I_Floor_1",
                            },
                            {
                                "sensor_id": 117,
                                "reading_timestamp": get_month_mid(
                                    8, 15, 23, 59
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "temperature": 6.6,
                                "humidity": 99.1,
                                "pressure": 1032.75,
                                "location": "Building_I_Floor_2",
                            },
                        ],
                    }
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
    Validates that the AI agent successfully implemented advanced partitioning for time-series data.

    Expected behavior:
    - Partitioned sensor_readings table created
    - Multiple partitions created for different months
    - Sample time-series data inserted across partitions
    - Indexes created for optimal query performance
    - Stored procedure for partition management created
    - Query performance optimizations demonstrated

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
            "description": "AI Agent executes partitioning implementation",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the partitioning task...",
        },
        {
            "name": "Partitioned Table Creation",
            "description": "Verify sensor_readings table with partitioning",
            "status": "running",
            "Result_Message": "Validating partitioned table structure...",
        },
        {
            "name": "Partition Structure Validation",
            "description": "Verify multiple partitions exist and are properly configured",
            "status": "running",
            "Result_Message": "Validating partition configuration...",
        },
        {
            "name": "Data Distribution Validation",
            "description": "Verify data is properly distributed across partitions",
            "status": "running",
            "Result_Message": "Validating data distribution across partitions...",
        },
        {
            "name": "Performance Optimizations",
            "description": "Verify indexes and stored procedures for partition management",
            "status": "running",
            "Result_Message": "Validating performance optimizations...",
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

        db_name = resource_data["created_resources"][0]["name"]
        db_connection = mysql_fixture.get_connection(database=db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Validate partitioned table exists
            db_cursor.execute("SHOW TABLES LIKE 'sensor_readings'")
            table_exists = db_cursor.fetchone()

            if not table_exists:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ sensor_readings table not found"
                return {"score": 0.2, "metadata": {"test_steps": test_steps}}

            # Check if table is partitioned
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.partitions 
                WHERE table_schema = %s 
                AND table_name = 'sensor_readings' 
                AND partition_name IS NOT NULL
            """,
                (db_name,),
            )

            partition_count = db_cursor.fetchone()[0]

            if partition_count > 0:
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ Partitioned sensor_readings table found with {partition_count} partitions"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = "❌ sensor_readings table exists but is not partitioned"
                return {"score": 0.2, "metadata": {"test_steps": test_steps}}

            # Step 3: Validate partition structure
            db_cursor.execute(
                """
                SELECT partition_name, partition_expression, partition_description
                FROM information_schema.partitions 
                WHERE table_schema = %s 
                AND table_name = 'sensor_readings'
                AND partition_name IS NOT NULL
                ORDER BY partition_ordinal_position
            """,
                (db_name,),
            )

            partitions = db_cursor.fetchall()

            if len(partitions) >= 3:  # At least 3-4 months of partitions
                test_steps[2]["status"] = "passed"
                partition_names = [p[0] for p in partitions]
                test_steps[2]["Result_Message"] = (
                    f"✅ Partition structure validated: {len(partitions)} partitions "
                    f"({', '.join(partition_names[:3])}{'...' if len(partitions) > 3 else ''})"
                )
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Insufficient partitions: only {len(partitions)} found, expected at least 3"

            # Step 4: Validate data distribution
            db_cursor.execute("SELECT COUNT(*) FROM sensor_readings")
            total_records = db_cursor.fetchone()[0]

            if total_records >= 20:  # At least the initial sensor data (20 records)
                # Check data distribution across partitions
                db_cursor.execute(
                    """
                    SELECT 
                        p.partition_name,
                        p.table_rows
                    FROM information_schema.partitions p
                    WHERE p.table_schema = %s 
                    AND p.table_name = 'sensor_readings'
                    AND p.partition_name IS NOT NULL
                    AND p.table_rows > 0
                    ORDER BY p.partition_ordinal_position
                """,
                    (db_name,),
                )

                partitions_with_data = db_cursor.fetchall()

                if len(partitions_with_data) >= 2:  # Data in multiple partitions
                    test_steps[3]["status"] = "passed"
                    test_steps[3]["Result_Message"] = (
                        f"✅ Data distributed across partitions: {total_records} total records "
                        f"in {len(partitions_with_data)} partitions"
                    )
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = (
                        f"❌ Data not properly distributed: {total_records} records "
                        f"in only {len(partitions_with_data)} partitions"
                    )
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3][
                    "Result_Message"
                ] = f"❌ Insufficient sample data: only {total_records} records"

            # Step 5: Check for performance optimizations
            optimizations_found = 0
            optimization_details = []

            # Check for indexes
            db_cursor.execute("SHOW INDEX FROM sensor_readings")
            indexes = db_cursor.fetchall()
            index_names = [idx[2] for idx in indexes if idx[2] != "PRIMARY"]

            if index_names:
                optimizations_found += 1
                optimization_details.append(f"{len(index_names)} indexes")

            # Check for stored procedures (partition management)
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.routines 
                WHERE routine_schema = %s 
                AND routine_type = 'PROCEDURE'
            """,
                (db_name,),
            )

            procedure_count = db_cursor.fetchone()[0]
            if procedure_count > 0:
                optimizations_found += 1
                optimization_details.append(f"{procedure_count} stored procedures")

            # Check for additional tables (summary/aggregation tables)
            db_cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name != 'sensor_readings'
            """,
                (db_name,),
            )

            additional_tables = db_cursor.fetchone()[0]
            if additional_tables > 0:
                optimizations_found += 1
                optimization_details.append(f"{additional_tables} additional tables")

            if optimizations_found >= 2:
                test_steps[4]["status"] = "passed"
                test_steps[4][
                    "Result_Message"
                ] = f"✅ Performance optimizations implemented: {', '.join(optimization_details)}"
            else:
                test_steps[4]["status"] = (
                    "failed" if optimizations_found == 0 else "passed"
                )
                test_steps[4][
                    "Result_Message"
                ] = f"{'❌' if optimizations_found == 0 else '✅'} Limited optimizations: {', '.join(optimization_details) if optimization_details else 'none found'}"

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
