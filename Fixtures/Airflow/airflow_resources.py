"""
This module provides a pytest fixture for creating isolated Airflow instances using Docker Compose.
"""
import os
import shutil
import subprocess
import tempfile
import time

import pytest
import requests
from pathlib import Path
from python_on_whales import DockerClient

from Environment.Airflow.Airflow import Airflow_Local
VALIDATE_ASTRO_INSTALL = "Please check if the Astro CLI is installed and in PATH."


@pytest.fixture(scope="function")
def airflow_resource(request):
    """
    A function-scoped fixture that creates unique Airflow instances for each test.
    Each test gets its own isolated Airflow environment using docker-compose.
    """
    # verify the required astro envars are set
    required_envars = ["ASTRO_WORKSPACE_ID", "ASTRO_ACCESS_TOKEN"]
    if missing_envars := [envar for envar in required_envars if not os.getenv(envar)]:
        raise ValueError(f"The following envars are not set: {missing_envars}")
    
    # make sure the astro cli is installed
    astro_version = _parse_astro_version()

    start_time = time.time()
    test_name = request.node.name
    unique_id = f"{test_name}_{int(time.time())}"
    print(f"Worker {os.getpid()}: Starting airflow_resource for {test_name}")

    # TODO: verify template for Airflow resource
    # A function-scoped fixture that creates Airflow resource in Astronomer based on template.
    # Template structure: {"resource_id": "id", "databases": [{"name": "db", "collections": [{"name": "col", "data": []}]}]}    
    build_template = request.param

    
    # Create Airflow resource
    print(f"Worker {os.getpid()}: Creating Airflow resource for {test_name}")
    creation_start = time.time()
    
    # run terminal commands to create the airflow resource in astronomer
    # login to astronomer
    _run_and_validate_subprocess(["astro", "login", "--token", os.getenv("ASTRO_ACCESS_TOKEN")], "login to Astro")
    
    # create a deployment
    test_dir = _create_dir_and_astro_project(unique_id)

    # TODO: upload the dag
    # TODO: trigger the dag
    # TODO: wait for the dag to finish
    # TODO: delete the deployment
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: Airflow resource creation took {creation_end - creation_start:.2f}s")


def _parse_astro_version() -> str:
    """
    Runs the `astro version` command to check if the Astro CLI is installed and returns the version number.

    :raises EnvironmentError: If the Astro CLI is not installed or not in PATH, or if the version cannot be parsed.
    :return: The version number of the Astro CLI as a string.
    :rtype: str
    """
    import re
    try:
        # run a simple astro version command to check if astro cli is installed
        version = subprocess.run(["astro", "version"], check=True, capture_output=True)
        # parse the version out after checking it ran successfully
        if version.returncode != 0:
            raise subprocess.CalledProcessError(version.returncode, "astro version")
        # use regex to extract the version number from the output
        version_pattern = re.compile(r"(\d+\.\d+\.\d+)")
        match = version_pattern.search(version.stdout.decode('utf-8'))
        if not match:
            raise EnvironmentError("Could not parse Astro CLI version from output")
        astro_version = match.group(1)
        # print the version number
        print(f"Worker {os.getpid()}: Astro CLI version: {astro_version}")
        return astro_version
    except Exception as e:
        raise e(
            "The Astro CLI is not installed or not in PATH. Please install it "
            "from https://docs.astronomer.io/cli/installation"
        ) from e


def _run_and_validate_subprocess(command: list[str], process_description: str, check: bool = True, capture_output: bool = True, return_output: bool = False) -> subprocess.CompletedProcess:
    """
    Helper function to run a subprocess command and validate the return code.

    :param command: The command to run.
    :param process_description: The description of the process, used for error messages.
    :param check: Whether to check the return code.
    :param capture_output: Whether to capture the output.
    :param return_output: Whether to return the output.
    :return: The completed process.
    :rtype: subprocess.CompletedProcess
    """
    try:
        process = subprocess.run(command, check=check, capture_output=capture_output)
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)
        if return_output:
            return process.stdout.decode('utf-8')
        else:
            return process
    except Exception as e:
        raise e(
            f"Failed to {process_description}. {VALIDATE_ASTRO_INSTALL}"
        ) from e


def _create_dir_and_astro_project(unique_id: str) -> Path:
    """
    Creates a directory and an Astro project in it.

    :param unique_id: The unique id for the test.
    :return: The path to the created directory.
    :rtype: Path
    """
    # Use a temp directory inside the project root for Docker compatibility
    project_root = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).parent
    tmp_root = os.path.join(project_root, "tmp_airflow_tests")
    os.makedirs(tmp_root, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix=f"airflow_test_{unique_id}", dir=tmp_root)
    # create a dags directory
    dags_dir = os.path.join(temp_dir, "dags")
    os.makedirs(dags_dir, exist_ok=True)
    print(f"Worker {os.getpid()}: Created temp directory for Airflow: {temp_dir}")
    # cd into the temp directory and run astro project init
    os.chdir(temp_dir)

    astro_project = _run_and_validate_subprocess(["astro", "dev", "init"], "initialize Astro project", return_output=True)
    print(f"Worker {os.getpid()}: Astro project initialized: {astro_project}")
    return Path(temp_dir)


    # resource_id = build_template.get("resource_id", f"airflow_resource_{test_name}_{int(time.time())}")
    
    # # Create detailed resource data
    # resource_data = {
    #     "resource_id": resource_id,
    #     "type": "airflow_resource",
    #     "test_name": test_name,
    #     "creation_time": time.time(),
    #     "worker_pid": os.getpid(),
    #     "creation_duration": creation_end - creation_start,
    #     "description": f"An Airflow resource for {test_name}",
    #     "status": "active",
    #     "created_resources": created_resources
    # }
    
    # print(f"Worker {os.getpid()}: Created Airflow resource {resource_id}")
    
    # fixture_end_time = time.time()
    # print(f"Worker {os.getpid()}: Airflow fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    # yield resource_data
    
    # # Cleanup after test completes
    # print(f"Worker {os.getpid()}: Cleaning up Airflow resource {resource_id}")
    # try:
    #     # TODO: Implement cleaning up Airflow resources in Astronomer
    #     # Clean up created resources in reverse order
    #     for resource in reversed(created_resources):
    #         # TODO: Implement deleting an Airflow deployment in Astronomer
    #         pass
    #     print(f"Worker {os.getpid()}: Airflow resource {resource_id} cleaned up successfully")
    # except Exception as e:
    #     print(f"Worker {os.getpid()}: Error cleaning up Airflow resource: {e}")




    
    
    # Use a temp directory inside the project root for Docker compatibility
#     project_root = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).parent
#     tmp_root = os.path.join(project_root, "tmp_airflow_tests")
#     os.makedirs(tmp_root, exist_ok=True)
#     temp_dir = tempfile.mkdtemp(prefix=f"airflow_test_{test_name}_", dir=tmp_root)
#     print(f"Worker {os.getpid()}: Created temp directory for Airflow: {temp_dir}")
    
#     # Copy the Airflow environment to the temp directory
#     airflow_source_dir = os.path.join(project_root, "Environment", "Airflow")
#     airflow_test_dir = os.path.join(temp_dir, "airflow")
#     shutil.copytree(airflow_source_dir, airflow_test_dir)
    
#     # Create a unique project name for this test instance
#     project_name = f"airflow_{test_name}_{int(time.time())}"
    
#     # Modify the docker-compose.yml to use unique project name and ports
#     docker_compose_path = os.path.join(airflow_test_dir, "docker-compose.yml")
    
#     # Read the original docker-compose.yml
#     with open(docker_compose_path, 'r') as f:
#         compose_content = f.read()
    
#     # Create a modified version with unique project name and ports
#     # We'll use environment variables to make it unique and avoid port conflicts
#     import random
#     base_port = 8888
#     webserver_port = base_port + random.randint(0, 100)  # Random port offset
    
#     # Replace the port mapping in the docker-compose content
#     import re
#     # Replace the webserver port mapping
#     compose_content = re.sub(
#         r'ports:\s*\n\s*-\s*"8888:8080"',
#         f'ports:\n      - "{webserver_port}:8080"',
#         compose_content
#     )
    
#     modified_compose_content = f"""# Modified docker-compose for test isolation
# # Project: {project_name}
# # Test: {test_name}
# # Worker PID: {os.getpid()}
# # Webserver Port: {webserver_port}

# {compose_content}
# """
    
#     # Write the modified docker-compose.yml
#     with open(docker_compose_path, 'w') as f:
#         f.write(modified_compose_content)
    
#     # Set environment variables for this test instance
#     original_env = os.environ.copy()
#     os.environ["COMPOSE_PROJECT_NAME"] = project_name
#     os.environ["AIRFLOW_PROJ_DIR"] = airflow_test_dir
    
#     # Create a custom Airflow_Local instance for this test
#     class TestAirflowLocal(Airflow_Local):
#         def __init__(self, test_dir, project_name, webserver_port):
#             self.Airflow_DIR = test_dir
#             self.project_name = project_name
#             self.webserver_port = webserver_port
#             self.Airflow_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
#             self.Airflow_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")
#             self.Airflow_HOST = os.getenv("AIRFLOW_HOST", "localhost")
#             self.Airflow_PORT = str(webserver_port)
#             self.Airflow_BASE_URL = f"http://{self.Airflow_HOST}:{self.Airflow_PORT}"

#         def Wait_For_Airflow_To_Be_Ready(self):
#             """
#             Wait a bit for Airflow to be fully ready
#             """
#             time.sleep(5)
#             for i in range(6):
#                 print(f"Waiting for Airflow to be ready... Attempt {i+1}/5")
#                 try:
#                     response = requests.get(
#                         f"{self.Airflow_BASE_URL}/health",
#                         auth=requests.auth.HTTPBasicAuth(self.Airflow_USERNAME, self.Airflow_PASSWORD),
#                         timeout=10
#                     )
#                     if response.status_code == 200:
#                         print(f"Airflow is ready at {self.Airflow_BASE_URL}")
#                         break
#                 except requests.exceptions.RequestException as e:
#                     print(f"Error connecting to Airflow: {e}")
#                     time.sleep(10)
        
#         def Start_Airflow(self, public_expose=False):
#             # Set the absolute path for AIRFLOW_PROJ_DIR
#             os.environ["AIRFLOW_PROJ_DIR"] = self.Airflow_DIR
#             os.environ["COMPOSE_PROJECT_NAME"] = self.project_name

#             # Boot up the docker instance for compose
#             docker = DockerClient(
#                 compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
#             )

#             # Docker compose up to get it to start
#             docker.compose.up(detach=True)

#             # Wait for the airflow webserver to start
#             print(f"Worker {os.getpid()}: Waiting for Airflow to start for {test_name}...")
#             self.Wait_For_Airflow_To_Be_Ready()

#             if public_expose:
#                 print("TODO LATER")

#         def Stop_Airflow(self):
#             try:
#                 # load the docker compose as docker
#                 docker = DockerClient(
#                     compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
#                 )

#                 # docker down with force removal
#                 docker.compose.down(volumes=True, remove_orphans=True, remove_images="all")
#                 print(f"Worker {os.getpid()}: Successfully stopped & removed containers and volumes for {self.project_name}")
#             except Exception as e:
#                 print(f"Worker {os.getpid()}: Error in Stop_Airflow: {e}")
#                 # Try alternative cleanup
#                 try:
#                     # Use docker directly to force remove containers
#                     import subprocess
#                     result = subprocess.run(
#                         ["docker", "compose", "down", "--rmi" "all", "--volumes", "--remove-orphans"],
#                         capture_output=True, text=True
#                     )
#                     if result.stdout.strip():
#                         print(f"Worker {os.getpid()}: removed containers, volumes, and orphans for {self.project_name}")
#                 except Exception as alt_e:
#                     print(f"Worker {os.getpid()}: Alternative cleanup failed: {alt_e}")
    
#     # Create the test-specific Airflow instance
#     airflow_instance = TestAirflowLocal(airflow_test_dir, project_name, webserver_port)
    
#     creation_start = time.time()
    
#     try:
#         # Start Airflow for this test
#         print(f"Worker {os.getpid()}: Starting Airflow instance for {test_name}")
#         airflow_instance.Start_Airflow()
        
#         creation_end = time.time()
#         print(f"Worker {os.getpid()}: Airflow instance creation took {creation_end - creation_start:.2f}s")
        
#         # Create detailed resource data
#         resource_data = {
#             "resource_id": project_name,
#             "type": "airflow_resource",
#             "test_name": test_name,
#             "creation_time": time.time(),
#             "worker_pid": os.getpid(),
#             "creation_duration": creation_end - creation_start,
#             "description": f"An Airflow instance for {test_name}",
#             "status": "active",
#             "airflow_instance": airflow_instance,
#             "temp_dir": temp_dir,
#             "project_name": project_name,
#             "base_url": airflow_instance.Airflow_BASE_URL,
#             "username": airflow_instance.Airflow_USERNAME,
#             "password": airflow_instance.Airflow_PASSWORD
#         }
        
#         print(f"Worker {os.getpid()}: Created Airflow resource {project_name}")
        
#         fixture_end_time = time.time()
#         print(f"Worker {os.getpid()}: Airflow fixture setup took {fixture_end_time - start_time:.2f}s total")
        
#         yield resource_data
        
#     except Exception as e:
#         print(f"Worker {os.getpid()}: Error creating Airflow resource: {e}")
#         # Even if creation failed, try to clean up any partial resources
#         try:
#             if 'airflow_instance' in locals():
#                 airflow_instance.Stop_Airflow()
#         except:
#             pass
#         raise
#     finally:
#         # Cleanup after test completes - ensure this always runs
#         print(f"Worker {os.getpid()}: Cleaning up Airflow resource {project_name}")
        
#         # Always try to stop containers first
#         try:
#             if 'airflow_instance' in locals():
#                 print(f"Worker {os.getpid()}: Stopping Airflow containers for {project_name}")
#                 airflow_instance.Stop_Airflow()
#                 print(f"Worker {os.getpid()}: Successfully stopped Airflow containers")
#         except Exception as e:
#             print(f"Worker {os.getpid()}: Error stopping Airflow containers: {e}")
#             # Try alternative cleanup method
#             try:
#                 docker = DockerClient()
#                 # Force remove containers with the project name
#                 docker.compose.down(volumes=True, remove_orphans=True)
#                 print(f"Worker {os.getpid()}: Force cleaned up containers using alternative method")
#             except Exception as alt_e:
#                 print(f"Worker {os.getpid()}: Alternative cleanup also failed: {alt_e}")
        
#         # Always try to clean up temp directory
#         try:
#             if os.path.exists(temp_dir):
#                 shutil.rmtree(temp_dir)
#                 print(f"Worker {os.getpid()}: Removed temp directory: {temp_dir}")
#         except Exception as e:
#             print(f"Worker {os.getpid()}: Error removing temp directory: {e}")
        
#         # Always restore original environment
#         try:
#             os.environ.clear()
#             os.environ.update(original_env)
#         except Exception as e:
#             print(f"Worker {os.getpid()}: Error restoring environment: {e}")
        
#         print(f"Worker {os.getpid()}: Airflow resource {project_name} cleanup completed") 