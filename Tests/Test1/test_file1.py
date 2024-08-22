# src/Tests/Test1_Subfolder/test_file1.py

# Import from the Model directory
from Model.Run_Model import run_model
import os
import importlib


# Import from the Functions directory
from Functions.Docker.DockerSetup import load_docker

current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def test_some_function():

    input_dir = os.path.dirname(os.path.abspath(__file__))
    container = load_docker(input_directory=input_dir)


    run_model(container=container, task=Test_Configs.User_Input)

    #simulate the creation and running of a hello world script
   


    #now we validate the output


    #now we shut down the container

    container.stop()

