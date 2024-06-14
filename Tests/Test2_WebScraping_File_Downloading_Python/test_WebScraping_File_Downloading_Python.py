# content of test_sample.py
import docker
import os
import time
import sys
from functools import partial
from dotenv import load_dotenv
import importlib
import pytest

load_dotenv()
sys.path.append(os.getenv('YOUR_ROOT_DIR'))
current_dir = os.path.dirname(os.path.abspath(__file__))

from model import model_store


# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"

Test_Configs = importlib.import_module(module_path)

def inc(x):
    return x + 1


def access_shell(container, command = None):
    # Start an interactive shell session in the container
    if command is None:
        return
    exec_id = container.exec_run(
        cmd=command)

    # Print output from the command execution
    return exec_id.output.decode('utf-8').strip()
       

   

@pytest.mark.two
def test_file_downloading():

    

    # Create a Docker client object
    client = docker.from_env()

    host_dir = os.path.abspath(f"Tests/{os.path.basename(current_dir)}/Test_Environment")
    container_root = "/app"


    # Run a container from the "python 3.9 slim" image (detach=True for background)
    container = client.containers.run(
        "python:3.9-slim",
        command="tail -f /dev/null",
        working_dir=container_root,
        detach=True,  # Run container in the background
        volumes={
            host_dir: {"bind": container_root, "mode": "rw"}
        }
    )

    time.sleep(10)

    # Get the container ID (for log retrieval)
    container_id = container.id

    print(f"Container started with ID: {container_id}")

    #this is where you add the model to do stuff
    #the model has direct access to Test Environment as a volume so it can interact with the files as if it was a new clean environment
    #the model has access_shell to run commands in the container directly


    #import the model here

    shell_access_var = partial(access_shell, container)

    model_store.run_model(user_input=Test_Configs.User_Input,code_environment_location = "/Test_Environment",shell_access=shell_access_var)


    returned = access_shell(container, "echo hello world")

    print(returned)

    #result = access_shell(container)



    # Retrieve and print container logs
    logs = container.logs()
    print(f"Container logs:\n{logs}")

    container.stop()
    container.remove()
    print(f"Container with ID {container_id} stopped and removed.")

    assert inc(3) == 4