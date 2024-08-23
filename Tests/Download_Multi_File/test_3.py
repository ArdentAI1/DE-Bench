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


@pytest.mark.three
@pytest.mark.code_writing
@pytest.mark.environment_management
def test_Download_Multi_File():
    input_dir = os.path.dirname(os.path.abspath(__file__))
    container = load_docker(input_directory=input_dir)
    run_model(container=container, task=Test_Configs.User_Input,configs=Test_Configs.Configs)


    #TODO:REMOVE SIMULATION OF PROPER IMPLEMENTATION

    # Create a Python script to download and process multiple files
    download_script = '''
import requests
import os
import zipfile

urls = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip"
]

def download_and_unzip(url):
    filename = os.path.basename(url)
    print(f"Downloading {url}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {filename} successfully.")
        print(f"File size: {os.path.getsize(filename)} bytes")
        
        # Unzip the file
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall()
        print(f"Unzipped {filename}")
        
        # Remove the zip file
        os.remove(filename)
        print(f"Removed {filename}")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")

for url in urls:
    download_and_unzip(url)

# List all CSV files
csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
print("CSV files:")
for csv_file in csv_files:
    print(f"- {csv_file}")
'''

    # Write the download script to a file in the container
    escaped_script = download_script.replace("'", "'\\''")
    create_file_command = f"echo '{escaped_script}' > download_multi_file.py"
    container.exec_run(['/bin/sh', '-c', create_file_command])

    # Install required libraries
    container.exec_run("pip install requests")

    # Run the download script
    result = container.exec_run("python download_multi_file.py")

    # Get the output
    output = result.output.decode('utf-8').strip()


    #BEGINING OF VALIDATION

    # Check if CSV files exist in the container
    file_check = container.exec_run(["/bin/sh", "-c", "ls -l *.csv"])
    csv_files_exist = file_check.exit_code == 0

    # Get the list of CSV files
    csv_files = container.exec_run(["/bin/sh", "-c", "ls *.csv"]).output.decode('utf-8').strip().split('\n')

    # Stop the container
    container.stop()

    

    expected_files = [
        "Divvy_Trips_2018_Q4.csv",
        "Divvy_Trips_2019_Q1.csv",
        "Divvy_Trips_2019_Q2.csv",
        "Divvy_Trips_2019_Q3.csv"
    ]
    for expected_file in expected_files:
        assert any(expected_file in csv_file for csv_file in csv_files), f"{expected_file} not found in the container"

    print("All tests passed successfully!")