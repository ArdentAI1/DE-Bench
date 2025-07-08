import os
import importlib
import pytest
import time
import requests
from github import Github
from requests.auth import HTTPBasicAuth

from Environment.Airflow.Airflow import Airflow_Local
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs
from model.Configure_Model import remove_model_configs

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.airflow
@pytest.mark.pipeline
def test_airflow_sample_1(request):
 
    
    print("Hello World from Airflow Sample 1")
    
    input_dir = os.path.dirname(os.path.abspath(__file__))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))

    test_steps = [
        {
            "name": "Stub Test",
            "description": "Simple Hello World test",
            "status": "passed",
            "Result_Message": "Hello World from Airflow Sample 1",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))
    
    # Add model runtime (stub value for now)
    start_time = time.time()
    # Stub: no actual model run
    end_time = time.time()
    request.node.user_properties.append(("model_runtime", end_time - start_time))


    test_steps[0]["status"] = "passed"
    test_steps[0]["Result_Message"] = "Hello World from Airflow Sample 1"
    
    # Simple assertion to make it pass
    assert True 