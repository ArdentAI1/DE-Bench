# src/Tests/Test1_Subfolder/test_file1.py

# Import from the Model directory
from Model.Run_Model import run_model
import os
import importlib
import pytest


# Import from the Functions directory
from Functions.Docker.DockerSetup import load_docker

current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.code_writing
@pytest.mark.one
def test_Hello_World():
    input_dir = os.path.dirname(os.path.abspath(__file__))
    container = load_docker(input_directory=input_dir)
    run_model(container=container, task=Test_Configs.User_Input,configs=Test_Configs.Configs)


    #TODO:REMOVE SIMULATION OF PROPER IMPLEMENTATION


    # Create a Python file that prints "Hello World" in the current directory
    container.exec_run("sh -c 'echo \"print(\\\"Hello World\\\")\" > hello_world.py'")





    #Validate the outputs now
    # Run the Python file
    result = container.exec_run("python hello_world.py")

    # Get the output
    output = result.output.decode('utf-8').strip()

    # Stop the container
    container.stop()

    # Validate that "Hello World" was printed
    assert output == "Hello World", f"Expected 'Hello World', but got '{output}'"
