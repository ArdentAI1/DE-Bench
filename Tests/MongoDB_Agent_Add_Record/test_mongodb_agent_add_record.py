# Braintrust-only MongoDB test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
from typing import List, Dict, Any
from Configs.MongoConfig import syncMongoClient
from Fixtures.base_fixture import DEBenchFixture


current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This is the main entry point for the Braintrust system.
    
    NOW LOADS DATA FROM S3 BSON (like Snowflake tests)
    """
    from Fixtures.MongoDB.mongo_resources import MongoDBFixture

    # Initialize MongoDB fixture with S3 config
    # Note: database name comes from BSON dump, not from config (unlike Snowflake)
    custom_mongo_config = {
        "resource_id": f"mongodb_add_record_{test_timestamp}_{test_uuid}",
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "Mongodb_Seeds/mongodb_add_record_test.bson.gz",
            "aws_key_id": "env:AWS_ACCESS_KEY",
            "aws_secret_key": "env:AWS_SECRET_KEY",
        },
    }

    mongo_fixture = MongoDBFixture(custom_config=custom_mongo_config)
    return [mongo_fixture]


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
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully added a record to MongoDB.

    This function creates test steps, performs validation, and returns detailed results
    for the Braintrust evaluation system.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "MongoDB Record Addition",
            "description": "Verify that AI agent added 'John Doe' record to MongoDB",
            "status": "running",
            "Result_Message": "Checking if 'John Doe' record was added to MongoDB collection...",
        }
    ]

    success = False

    try:
        # Use fixture to get database connection for validation
        mongo_fixture = None
        if fixtures:
            mongo_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mongo_resource"), None
            )

        if mongo_fixture:
            # Get the database that was created (from S3 restore)
            resource_data = getattr(mongo_fixture, "_resource_data", None)
            if not resource_data:
                raise Exception("MongoDB resource data not available")
            
            db_name = resource_data["database"]  # The BENCH_DB_xxx name
            db = mongo_fixture.get_database(db_name)
        else:
            # Fallback to direct connection if no fixtures provided
            db = syncMongoClient["agent_test_database"]

        # The collection name from the BSON dump
        collection = db["users"]  # BSON dump uses "users" as collection name
        record = collection.find_one({"name": "John Doe", "age": 30})

        if record is not None:
            # Verify the record contents match expectations
            if record["name"] == "John Doe" and record["age"] == 30:
                test_steps[0]["status"] = "passed"
                test_steps[0]["Result_Message"] = (
                    "✅ AI agent successfully added John Doe record with correct values: "
                    f"name='{record['name']}', age={record['age']}"
                )
                success = True
            else:
                test_steps[0]["status"] = "failed"
                test_steps[0]["Result_Message"] = (
                    f"❌ Record found but values incorrect. Expected: name='John Doe', age=30. "
                    f"Found: name='{record.get('name')}', age={record.get('age')}"
                )
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = (
                "❌ John Doe record was not found in MongoDB collection. "
                "AI agent may not have executed the MongoDB insertion correctly."
            )

    except Exception as e:
        test_steps[0]["status"] = "failed"
        test_steps[0]["Result_Message"] = f"❌ Database validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
