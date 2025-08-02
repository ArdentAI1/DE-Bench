# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs
from model.Configure_Model import remove_model_configs
import os
import importlib
import pytest
from pymongo.errors import CollectionInvalid
import time


# Import from the Functions directory
from Environment.Docker.DockerSetup import load_docker
from Configs.MongoConfig import syncMongoClient


current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.mongodb
@pytest.mark.code_writing
@pytest.mark.database
@pytest.mark.parametrize("mongo_resource", [{
    "resource_id": "test_mongo_resource",
    "databases": [
        {
            "name": "test_database",
            "collections": [
                {
                    "name": "test_collection",
                    "data": []
                }
            ]
        }
    ]
}], indirect=True)
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
def test_mongodb_agent_add_record(request, mongo_resource, supabase_account_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))

    request.node.user_properties.append(("user_query", Test_Configs.User_Input))

    test_steps = [
        {
            "name": "Record Creation",
            "description": "Adding a new record to MongoDB",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    try:
        # we can add an option to do local runs with this container
        # container = load_docker(input_directory=input_dir)

        # MongoDB setup is now handled by the fixture
        
        # Set up model configs using the configuration from Test_Configs
        config_results = set_up_model_configs(Configs=Test_Configs.Configs,custom_info={
            "publicKey": supabase_account_resource["publicKey"],
            "secretKey": supabase_account_resource["secretKey"],
        })

        # SECTION 2: RUN THE MODEL
        # Run the model which should add the record
        start_time = time.time()
        model_result = run_model(
            container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs,extra_information = {
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # SECTION 3: VERIFY THE OUTCOMES
        # we then check the record was added
        db = syncMongoClient["test_database"]
        collection = db["test_collection"]
        record = collection.find_one({"name": "John Doe", "age": 30})

        if record is not None:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Record was successfully added to MongoDB"
            assert record["name"] == "John Doe", "Name in record does not match"
            assert record["age"] == 30, "Age in record does not match"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "Record was not found in MongoDB"
            raise AssertionError("Record was not found in MongoDB")
        

    finally:
        try:
            # MongoDB cleanup is now handled by the fixture

            # Remove model configs
            if config_results:
                remove_model_configs(
                    Configs=Test_Configs.Configs, 
                    custom_info={
                        **config_results,  # Spread all the config results
                        "publicKey": supabase_account_resource["publicKey"],
                        "secretKey": supabase_account_resource["secretKey"],
                    }
                )

        except Exception as e:
            pass  # Cleanup error handled silently
