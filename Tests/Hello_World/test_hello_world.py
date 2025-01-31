# src/Tests/Test1_Subfolder/test_file1.py

# Import from the Model directory

import os
import importlib
import pytest
import sys

from model.Run_Model import run_model
from Functions.Docker.DockerSetup import load_docker



# Import from the Functions directory

#This calculates the path for the Test_Configs.py relative to the current file.
#This is used to import the Test_Configs.py file from the correct directory and allows copy paste instead of hardcoding

current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)



# Add this debug line at the top of the file after imports



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

    print(output)

    # Stop the container
    container.stop()

    # Validate that "Hello World" was printed
    assert output == "Hello World", f"Expected 'Hello World', but got '{output}'"

    print("Working on test 1")


    





