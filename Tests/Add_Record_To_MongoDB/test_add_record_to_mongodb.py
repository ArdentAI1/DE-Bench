# Import from the Model directory
from model.Run_Model import run_model
import os
import importlib
import pytest
from pymongo.errors import CollectionInvalid

from Configs.ArdentConfig import Ardent_Client

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
def test_add_mongodb_record():
    input_dir = os.path.dirname(os.path.abspath(__file__))


    #SECTION 1: SETUP THE TEST

    #we can add an option to do local runs with this container
    #container = load_docker(input_directory=input_dir)

    #we need to add the database in mongo and the collection into the database.

    try:
        db = syncMongoClient['test_database']
        db.create_collection('test_collection')
    except CollectionInvalid as e:
        print(f"Collection exists, dropping and recreating: {e}")
        db.drop_collection('test_collection')
        db.create_collection('test_collection')
    except Exception as e:
        raise Exception(f"Unexpected Error: {e}")
        
    #we then sync Ardent to the new stuff created
    valid_config = {
        "connection_string": os.getenv("MONGODB_URI"),
        "databases": [{
            "name": "test_database",
            "collections": [
                {"name": "test_collection"}
            ]
        }]
    }

    result = Ardent_Client.set_config(
        config_type="mongodb",
        **valid_config
    )





    #SECTION 2: RUN THE MODEL
    # Run the model which should add the record
    run_model(container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs)

    #SECTION 3: VERIFY THE OUTCOMES





    #we then check the record was added
    collection = db['test_collection']
    record = collection.find_one({"name": "John Doe", "age": 30})
    assert record is not None, "Record was not found in MongoDB"
    assert record["name"] == "John Doe", "Name in record does not match"
    assert record["age"] == 30, "Age in record does not match"
    #assert True


    #now clean up
    db.drop_collection('test_collection')
    syncMongoClient.drop_database('test_database')
    
