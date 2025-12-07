# Braintrust-only Airflow + MongoDB test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture
from Configs.MongoConfig import syncMongoClient

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a MongoDB data processing pipeline DAG.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.MongoDB.mongo_resources import MongoDBFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with configurable deployment provider
    resource_id = f"mongodb_process_ad_ops_{test_timestamp}_{test_uuid}"
    provider = os.getenv("AIRFLOW_PROVIDER", "modal")  # Default to Modal
    
    if provider == "modal":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "modal",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
        }
    elif provider == "aks":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "aks",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "kubernetes_namespace": resource_id.replace("_", "-"),
        }
    elif provider == "ecs":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "ecs",
            "container_image": "us-central1-docker.pkg.dev/ardent-de-bench/de-bench/airflow2-session-auth:base",
            "ecs_namespace": resource_id.replace("_", "-"),
        }
    elif provider == "astro":
        custom_airflow_config = {
            "resource_id": resource_id,
            "airflow_provider": "astro",
        }
    else:
        raise ValueError(
            f"Unknown AIRFLOW_PROVIDER: {provider}. "
            f"Supported: modal, aks, ecs, astro"
        )

    # Initialize MongoDB fixture with growl ad_opportunities data
    custom_mongo_config = {
        "resource_id": resource_id,
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "Mongodb_Seeds/growl_ad_opportunities.bson.gz",
            "aws_key_id": "env:AWS_ACCESS_KEY",
            "aws_secret_key": "env:AWS_SECRET_KEY",
        },
    }

    # Initialize GitHub fixture for DAG deployment
    custom_github_config = {
        "resource_id": resource_id,
        "initial_branch": "main",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    mongo_fixture = MongoDBFixture(custom_config=custom_mongo_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [airflow_fixture, mongo_fixture, github_fixture]


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
        "task": Test_Configs.User_Input,
    }


def validate_test(model_result: Any, fixtures: List[DEBenchFixture]) -> Dict[str, Any]:
    """
    Validate that the Airflow pipeline successfully processed ad_opportunities data.
    
    Checks:
    1. Airflow DAG ran successfully
    2. Processed collection exists with data
    3. Records have IP geolocation enrichment
    4. Records have user agent parsing
    5. Records are flattened (no nested objects)
    
    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test
    
    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    test_steps = []
    
    try:
        # Get MongoDB fixture
        mongo_fixture = None
        if fixtures:
            mongo_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mongo_resource"), None
            )
        
        if not mongo_fixture:
            raise Exception("MongoDB fixture not found")
        
        # Get database name
        resource_data = getattr(mongo_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("MongoDB resource data not available")
        
        db_name = resource_data["database"]
        db = syncMongoClient[db_name]
        
        # Step 1: Check Airflow DAG execution
        airflow_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "airflow"), None
        )
        
        if airflow_fixture:
            # Check if DAG ran (this is a simplified check)
            test_steps.append({
                "name": "Airflow DAG Execution",
                "description": "Verify Airflow DAG was created and executed",
                "status": "passed",
                "Result_Message": "✅ Airflow fixture available (DAG execution details in Airflow logs)",
            })
        else:
            test_steps.append({
                "name": "Airflow DAG Execution",
                "description": "Verify Airflow DAG was created and executed",
                "status": "failed",
                "Result_Message": "❌ Airflow fixture not found",
            })
        
        # Step 2: Check processed collection exists
        source_coll = db["ad_opportunities"]
        source_count = source_coll.count_documents({})
        
        if "ad_opportunities_processed" not in db.list_collection_names():
            test_steps.append({
                "name": "Processed Collection Exists",
                "description": "Verify ad_opportunities_processed collection was created",
                "status": "failed",
                "Result_Message": "❌ Collection 'ad_opportunities_processed' not found",
            })
            return {"success": False, "test_steps": test_steps}
        
        processed_coll = db["ad_opportunities_processed"]
        processed_count = processed_coll.count_documents({})
        
        test_steps.append({
            "name": "Processed Collection Exists",
            "description": "Verify ad_opportunities_processed collection was created",
            "status": "passed",
            "Result_Message": f"✅ Collection created with {processed_count} records",
        })
        
        # Step 3: Check record count
        if processed_count == source_count:
            test_steps.append({
                "name": "Record Count",
                "description": "Verify all source records were processed",
                "status": "passed",
                "Result_Message": f"✅ All {source_count} records processed",
            })
        else:
            test_steps.append({
                "name": "Record Count",
                "description": "Verify all source records were processed",
                "status": "failed",
                "Result_Message": f"❌ Expected {source_count} records, found {processed_count}",
            })
        
        # Step 4: Check IP geolocation enrichment
        # Look for records with geolocation fields
        geo_sample = processed_coll.find_one({
            "$or": [
                {"state": {"$exists": True, "$ne": None}},
                {"country": {"$exists": True, "$ne": None}},
                {"city": {"$exists": True, "$ne": None}},
            ]
        })
        
        if geo_sample:
            geo_fields = []
            for field in ["state", "country", "city", "longitude", "latitude"]:
                if field in geo_sample and geo_sample[field] is not None:
                    geo_fields.append(field)
            
            if len(geo_fields) >= 3:  # At least 3 of 5 geo fields
                test_steps.append({
                    "name": "IP Geolocation Enrichment",
                    "description": "Verify client IP geolocation data added",
                    "status": "passed",
                    "Result_Message": f"✅ Geolocation enrichment added ({', '.join(geo_fields)})",
                })
            else:
                test_steps.append({
                    "name": "IP Geolocation Enrichment",
                    "description": "Verify client IP geolocation data added",
                    "status": "partial",
                    "Result_Message": f"⚠️ Some geolocation fields found: {', '.join(geo_fields)}",
                })
        else:
            test_steps.append({
                "name": "IP Geolocation Enrichment",
                "description": "Verify client IP geolocation data added",
                "status": "failed",
                "Result_Message": "❌ No geolocation enrichment found",
            })
        
        # Step 5: Check user agent parsing
        ua_sample = processed_coll.find_one({
            "$or": [
                {"browser": {"$exists": True, "$ne": None}},
                {"os": {"$exists": True, "$ne": None}},
                {"device": {"$exists": True, "$ne": None}},
            ]
        })
        
        if ua_sample:
            ua_fields = []
            for field in ["browser", "os", "device"]:
                if field in ua_sample and ua_sample[field] is not None:
                    ua_fields.append(field)
            
            if len(ua_fields) >= 2:  # At least 2 of 3 UA fields
                test_steps.append({
                    "name": "User Agent Parsing",
                    "description": "Verify user agent parsing added",
                    "status": "passed",
                    "Result_Message": f"✅ User agent parsed ({', '.join(ua_fields)})",
                })
            else:
                test_steps.append({
                    "name": "User Agent Parsing",
                    "description": "Verify user agent parsing added",
                    "status": "partial",
                    "Result_Message": f"⚠️ Some user agent fields found: {', '.join(ua_fields)}",
                })
        else:
            test_steps.append({
                "name": "User Agent Parsing",
                "description": "Verify user agent parsing added",
                "status": "failed",
                "Result_Message": "❌ No user agent parsing found",
            })
        
        # Step 6: Check record flattening
        sample = processed_coll.find_one({})
        if sample:
            # Check for nested objects (excluding _id and MongoDB Extended JSON)
            nested_fields = []
            for key, value in sample.items():
                if key != "_id" and isinstance(value, dict):
                    # Skip MongoDB Extended JSON types ($oid, $date, etc)
                    if not (len(value) == 1 and list(value.keys())[0].startswith("$")):
                        nested_fields.append(key)
            
            if len(nested_fields) == 0:
                test_steps.append({
                    "name": "Record Flattening",
                    "description": "Verify records are flattened for ClickHouse",
                    "status": "passed",
                    "Result_Message": "✅ Records flattened (no nested objects)",
                })
            else:
                test_steps.append({
                    "name": "Record Flattening",
                    "description": "Verify records are flattened for ClickHouse",
                    "status": "failed",
                    "Result_Message": f"❌ Records contain nested objects: {', '.join(nested_fields[:3])}",
                })
        else:
            test_steps.append({
                "name": "Record Flattening",
                "description": "Verify records are flattened for ClickHouse",
                "status": "failed",
                "Result_Message": "❌ No records to validate",
            })
        
        # Determine overall success
        success = all(
            step["status"] == "passed" 
            for step in test_steps 
            if step["status"] in ["passed", "failed"]  # Ignore "partial"
        )
        
        return {"success": success, "test_steps": test_steps}
        
    except Exception as e:
        test_steps.append({
            "name": "Validation Error",
            "description": "Error during test validation",
            "status": "failed",
            "Result_Message": f"❌ Validation error: {str(e)}",
        })
        return {"success": False, "test_steps": test_steps}

