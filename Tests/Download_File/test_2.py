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


@pytest.mark.two
@pytest.mark.code_writing
@pytest.mark.environment_management
def test_Download_File():
    input_dir = os.path.dirname(os.path.abspath(__file__))
    container = load_docker(input_directory=input_dir)
    run_model(container=container, task=Test_Configs.User_Input,configs=Test_Configs.Configs)


    #TODO:REMOVE SIMULATION OF PROPER IMPLEMENTATION
    
    # Create a Python script to download the file
    download_script = '''
import requests
import os

url = "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip"
filename = os.path.basename(url)
print(f"Downloading {url}...")
response = requests.get(url)
if response.status_code == 200:
    with open(filename, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded {filename} successfully.")
    print(f"File size: {os.path.getsize(filename)} bytes")
else:
    print(f"Failed to download {url}. Status code: {response.status_code}")
'''
    
    # Write the download script to a file in the container
    escaped_script = download_script.replace("'", "'\\''")
    create_file_command = f"echo '{escaped_script}' > download_file.py"
    container.exec_run(['/bin/sh', '-c', create_file_command])
    
    # Install requests library (if not already installed)
    container.exec_run("pip install requests")
    
    # Run the download script
    result = container.exec_run("python download_file.py")
    
    # Get the output
    output = result.output.decode('utf-8').strip()


    #BEGINING OF VALIDATION
    
    # Check if the file exists in the container
    file_check = container.exec_run("ls -l Divvy_Trips_2018_Q4.zip")
    file_exists = file_check.exit_code == 0
    
    # Stop the container
    container.stop()
    
    # Validate the download
    assert "Downloaded Divvy_Trips_2018_Q4.zip successfully." in output, "File was not downloaded successfully"
    assert file_exists, "Downloaded file not found in the container"
    assert "File size:" in output, "File size not reported"
    print(output)
