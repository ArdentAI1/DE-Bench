# src/Tests/Test1_Subfolder/test_file1.py

# Import from the Model directory
from model.Run_Model import run_model
import os
import importlib
import pytest
import boto3


# Import from the Functions directory
from Functions.Docker.DockerSetup import load_docker
from Functions.AWS.AWS_Client import s3_client

current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


@pytest.mark.three
@pytest.mark.code_writing
@pytest.mark.environment_management
@pytest.mark.AWS
@pytest.mark.S3
@pytest.mark.Data_Env_Search
def test_Download_File():
    input_dir = os.path.dirname(os.path.abspath(__file__))
    container = load_docker(input_directory=input_dir)
    run_model(container=container, task=Test_Configs.User_Input,configs=Test_Configs.Configs)



    aws_access_key_id = os.getenv('ACCESS_KEY_ID_AWS')
    aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY_AWS')
    aws_region = 'us-east-1'


    upload_script = f'''
import boto3
import os

s3_client = boto3.client(
    's3',
    aws_access_key_id='{aws_access_key_id}',
    aws_secret_access_key='{aws_secret_access_key}',
    region_name='{aws_region}'
)
bucket_name = 'de-bench-testing'
file_path = 'test.pdf'

print(f"Uploading {{file_path}} to S3 bucket {{bucket_name}}...")
try:
    s3_client.upload_file(file_path, bucket_name, file_path)
    print(f"Uploaded {{file_path}} successfully.")
except Exception as e:
    print(f"Failed to upload {{file_path}}. Error: {{e}}")
'''
        
    # Write the upload script to a file in the container
    escaped_script = upload_script.replace("'", "'\\''")
    create_file_command = f"echo '{escaped_script}' > upload_file.py"
    container.exec_run(['/bin/sh', '-c', create_file_command])
    
    # Install boto3 library (if not already installed)
    container.exec_run("pip install boto3")
    
    # Run the upload script
    result = container.exec_run("python upload_file.py")
    
    # Get the output
    output = result.output.decode('utf-8').strip()

    # Validate if the file is in S3
    # You may need to check the file from outside the container or use a similar script to verify
    try:
        response = s3_client.head_object(Bucket='de-bench-testing', Key='test.pdf')
        file_exists_in_s3 = response is not None
        if file_exists_in_s3:
            s3_client.delete_object(Bucket='de-bench-testing', Key='test.pdf')
            print("File was successfully uploaded, verified, and deleted from S3.")
    except Exception as e:
        file_exists_in_s3 = False
    
    # Stop the container
    #container.stop()
    
    # Validate the upload
    assert file_exists_in_s3, "Uploaded file not found in the S3 bucket"
    print("File was successfully uploaded and verified in S3.")
   