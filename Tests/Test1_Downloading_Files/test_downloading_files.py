import docker
import os
import time
import sys
from functools import partial
from dotenv import load_dotenv
import importlib
import pytest


# Load environment variables from .env file
load_dotenv()

# Add the root directory to sys.path
root_dir = os.getenv('YOUR_ROOT_DIR')
if root_dir:
    sys.path.append(root_dir)
    print(f"Appended {root_dir} to sys.path")
else:
    print("YOUR_ROOT_DIR not set in .env file")
    sys.exit(1)

# Import the model_store module
from model import model_store

# Determine the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"

try:
    # Import Test_Configs module dynamically
    Test_Configs = importlib.import_module(module_path)
    print(f"Successfully imported {module_path}")
except ModuleNotFoundError:
    print(f"Failed to import module: {module_path}")
    sys.exit(1)


def inc(x):
    return x + 1

@pytest.mark.one
def access_shell(container, command=None):
    # Start an interactive shell session in the container
    if command is None:
        return
    exec_id = container.exec_run(cmd=command)

    # Print output from the command execution
    return exec_id.output.decode('utf-8').strip()


def test_file_downloading():
    # Create a Docker client object
    client = docker.from_env()

    # Construct host directory path dynamically
    host_dir = os.path.abspath(f"Tests/{parent_dir_name}/Test_Environment")
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

    # Access shell function with container context
    shell_access_var = partial(access_shell, container)

    # Example usage of model_store with Test_Configs
    model_store.run_model(user_input=Test_Configs.User_Input, code_environment_location="/Test_Environment",
                          shell_access=shell_access_var)

    # Example command execution in the container
    returned = access_shell(container, "echo hello world")
    print(returned)

    # Retrieve and print container logs
    logs = container.logs()
    print(f"Container logs:\n{logs}")

    # Stop and remove the container
    container.stop()
    container.remove()
    print(f"Container with ID {container_id} stopped and removed.")

    # Assertion example
    assert inc(3) == 4



