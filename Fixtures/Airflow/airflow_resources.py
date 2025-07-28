"""
This module provides a pytest fixture for creating isolated Airflow instances using Docker Compose.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Union

import github
from github import Github

import pytest

from .Airflow import Airflow_Local

VALIDATE_ASTRO_INSTALL = "Please check if the Astro CLI is installed and in PATH."


@pytest.fixture(scope="function")
def airflow_resource(request):
    """
    A function-scoped fixture that creates unique Airflow instances for each test.
    Each test gets its own isolated Airflow environment using docker-compose.
    """
    # verify the required astro envars are set
    resource_id = "airflow_resource"
    required_envars = [
        "ASTRO_WORKSPACE_ID",
        "ASTRO_ACCESS_TOKEN",
        "AIRFLOW_GITHUB_TOKEN",
        "AIRFLOW_REPO",
        "ASTRO_CLOUD_PROVIDER",
        "ASTRO_REGION",
    ]
    if missing_envars := [envar for envar in required_envars if not os.getenv(envar)]:
        raise ValueError(f"The following envars are not set: {missing_envars}")  # noqa

    # make sure the astro cli is installed
    _parse_astro_version()

    start_time = time.time()
    test_name = request.node.name
    unique_id = f"{test_name}_{int(time.time())}"
    print(f"Worker {os.getpid()}: Starting airflow_resource for {test_name}")

    # Create Airflow resource
    print(f"Worker {os.getpid()}: Creating Airflow resource for {test_name}")
    creation_start = time.time()
    created_resources = []

    # run terminal commands to create the airflow resource in astronomer
    # login to astronomer
    _run_and_validate_subprocess(
        ["astro", "login", "--token-login", os.getenv("ASTRO_ACCESS_TOKEN")],
        "login to Astro",
    )

    test_dir = _create_dir_and_astro_project(unique_id)
    astro_deployment_id = _create_deployment_in_astronomer(unique_id)

    try:
         # check and update the github secrets
        _check_and_update_gh_secrets(
            deployment_id=astro_deployment_id,
            deployment_name=unique_id,
            astro_access_token=os.environ["ASTRO_ACCESS_TOKEN"],
        )

        created_resources.append(astro_deployment_id)
        api_url = "https://" + _run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "inspect",
                "--deployment-name",
                unique_id,
                "--key",
                "metadata.airflow_api_url",
            ],
            "getting Astro deployment API URL",
            return_output=True,
        )
        base_url = api_url[: api_url.find("/api/v1")]

        # create a token for the airflow resource
        api_token = _run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "token",
                "create",
                "--description",
                f"{test_name} API access for deployment {unique_id}",
                "--name",
                f"{unique_id} API access",
                "--role",
                "DEPLOYMENT_ADMIN",
                "--expiration",
                "30",
                "--deployment-id",
                astro_deployment_id,
                "--clean-output",
            ],
            "creating Astro deployment API token",
            return_output=True,
        )

        # create a user in the airflow deployment (ardent needs username and password for the Airflowconfig)
        _create_user_in_airflow_deployment(unique_id)

        creation_end = time.time()
        print(
            f"Worker {os.getpid()}: Airflow resource creation took {creation_end - creation_start:.2f}s"
        )

        # Create detailed resource data
        resource_data = {
            "resource_id": resource_id,
            "type": "airflow_resource",
            "test_name": test_name,
            "creation_time": time.time(),
            "worker_pid": os.getpid(),
            "creation_duration": creation_end - creation_start,
            "description": f"An Airflow resource for {test_name}",
            "status": "active",
            "project_name": test_dir.stem,
            "base_url": base_url,
            "deployment_id": astro_deployment_id,
            "deployment_name": unique_id,
            "api_url": api_url,
            "api_token": api_token,
            "api_headers": {"Authorization": f"Bearer {api_token}", "Cache-Control": "no-cache"},
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "airflow_instance": Airflow_Local(
                airflow_dir=test_dir, host=base_url, api_token=api_token, api_url=api_url
            ),
            "created_resources": created_resources,
        }

        print(f"Worker {os.getpid()}: Created Airflow resource {resource_id}")

        fixture_end_time = time.time()
        print(
            f"Worker {os.getpid()}: Airflow fixture setup took {fixture_end_time - start_time:.2f}s total"
        )
        yield resource_data
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in Airflow fixture: {e}")
        raise e from e
    finally:
        # clean up the airflow resource after the test completes
        print(f"Worker {os.getpid()}: Cleaning up Airflow resource {resource_id}")
        cleanup_airflow_resource(test_name, resource_id, created_resources, test_dir)


def _parse_astro_version() -> None:
    """
    Runs the `astro version` command to check if the Astro CLI is installed and returns the version number.

    :raises EnvironmentError: If the Astro CLI is not installed or not in PATH, or if the version cannot be parsed.
    :rtype: None
    """
    try:
        # run a simple astro version command to check if astro cli is installed
        version = subprocess.run(["astro", "version"], check=True, capture_output=True)
        # parse the version out after checking it ran successfully
        if version.returncode != 0:
            raise subprocess.CalledProcessError(version.returncode, "astro version")
        # use regex to extract the version number from the output
        version_pattern = re.compile(r"(\d+\.\d+\.\d+)")
        match = version_pattern.search(version.stdout.decode("utf-8"))
        if not match:
            raise EnvironmentError("Could not parse Astro CLI version from output")
        astro_version = match.group(1)
        # print the version number
        print(f"Worker {os.getpid()}: Astro CLI version: {astro_version}")
    except Exception as e:
        print(
            "The Astro CLI is not installed or not in PATH. Please install it "
            "from https://docs.astronomer.io/cli/installation"
        )
        raise e from e


def _run_and_validate_subprocess(
    command: list[str],
    process_description: str,
    check: bool = True,
    capture_output: bool = True,
    return_output: bool = False,
    input_text: str = None,
) -> Union[subprocess.CompletedProcess, str]:
    """
    Helper function to run a subprocess command and validate the return code.

    :param command: The command to run.
    :param process_description: The description of the process, used for error messages.
    :param check: Whether to check the return code.
    :param capture_output: Whether to capture the output.
    :param return_output: Whether to return the output.
    :param input_text: Text to send to stdin if the command expects input.
    :return: The completed process, or the command output if `return_output` is True.
    :rtype: Union[subprocess.CompletedProcess, str]
    """
    try:
        if input_text:
            # If input is needed, use Popen to handle interactive input
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = process.communicate(input=input_text)
            if process.returncode != 0:
                print(stderr)
                raise subprocess.CalledProcessError(
                    process.returncode, command, stdout, stderr
                )
            if return_output:
                return stdout
            else:
                return subprocess.CompletedProcess(
                    command, process.returncode, stdout, stderr
                )
        else:
            process = subprocess.run(
                command, check=check, capture_output=capture_output
            )
            if process.returncode != 0:
                print(process.stderr.decode("utf-8"))
                raise subprocess.CalledProcessError(process.returncode, command)
            if return_output:
                return process.stdout.decode("utf-8").rstrip("\n")
            else:
                return process
    except Exception as e:
        print(f"Worker {os.getpid()}: Error running {process_description}: {e}")
        raise e from e


def _check_and_update_gh_secrets(deployment_id: str, deployment_name: str, astro_access_token: str) -> None:
    """
    Checks if the GitHub secrets exists, deletes them if they do, and creates new ones with the given
        deployment ID and name.

    :param str deployment_id: The ID of the deployment.
    :param str deployment_name: The name of the deployment.
    :rtype: None
    """
    gh_secrets = {
        "ASTRO_DEPLOYMENT_ID": deployment_id,
        "ASTRO_DEPLOYMENT_NAME": deployment_name,
        "ASTRO_ACCESS_TOKEN": astro_access_token,
    }
    airflow_github_repo = os.getenv("AIRFLOW_REPO")
    g = Github(os.getenv("AIRFLOW_GITHUB_TOKEN"))
    if "github.com" in airflow_github_repo:
        # Extract owner/repo from URL
        parts = airflow_github_repo.split("/")
        airflow_github_repo = f"{parts[-2]}/{parts[-1]}"
    repo = g.get_repo(airflow_github_repo)
    try:
        for secret, value in gh_secrets.items():
            try:
                if repo.get_secret(secret):
                    print(f"Worker {os.getpid()}: {secret} already exists, deleting...")
                    repo.delete_secret(secret)
                print(f"Worker {os.getpid()}: Creating {secret}...")
            except github.GithubException as e:
                if e.status == 404:
                    print(f"Worker {os.getpid()}: {secret} does not exist, creating...")
                else:
                    print(f"Worker {os.getpid()}: Error checking secret {secret}: {e}")
                    raise e
            repo.create_secret(secret, value)
            print(f"Worker {os.getpid()}: {secret} created successfully.")
    except Exception as e:
        print(f"Worker {os.getpid()}: Error checking and updating GitHub secrets: {e}")
        raise e from e


def _create_deployment_in_astronomer(deployment_name: str) -> str:
    """
    Creates a deployment in Astronomer.

    :param deployment_name: The name of the deployment to create.
    :raises EnvironmentError: If the deployment ID cannot be parsed from the output.
    :return: The ID of the created deployment.
    :rtype: str
    """
    try:
        # Run the command to create a deployment in Astronomer
        response = _run_and_validate_subprocess(
            [
                "astro", "deployment", "create",
                "--workspace-id", os.getenv("ASTRO_WORKSPACE_ID"),
                "--name", deployment_name,
                "--runtime-version", os.getenv("ASTRO_RUNTIME_VERSION", "13.1.0"),
                "--development-mode", "enable",
                "--cloud-provider", os.getenv("ASTRO_CLOUD_PROVIDER"),
                "--region", os.getenv("ASTRO_REGION", "us-east-1"),
                "--wait",
            ],
            "creating Astronomer deployment",
            return_output=True,
        )
        # Parse the output to get the newly created deployment ID
        deployment_id_pattern = re.compile(r"(?<=deployments/)([^/]+)(?=/overview)")
        match = deployment_id_pattern.search(response)
        if not match:
            raise EnvironmentError("Could not parse deployment ID from output")
        deployment_id = match.group(1)
        print(f"Worker {os.getpid()}: Created Astronomer deployment: {deployment_id}")
        return deployment_id
    except Exception as e:
        print(f"Worker {os.getpid()}: Error creating Astronomer deployment: {e}")
        raise e from e


def _create_dir_and_astro_project(unique_id: str) -> Path:
    """
    Creates a directory and an Astro project in it.

    :param unique_id: The unique id for the test.
    :return: The path to the created directory.
    :rtype: Path
    """
    # Use a temp directory inside the project root for Docker compatibility
    project_root = Path(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ).parent
    tmp_root = os.path.join(project_root, "tmp_airflow_tests")
    os.makedirs(tmp_root, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix=f"airflow_test_{unique_id}", dir=tmp_root)
    # create a dags directory
    dags_dir = os.path.join(temp_dir, "dags")
    os.makedirs(dags_dir, exist_ok=True)
    print(f"Worker {os.getpid()}: Created temp directory for Airflow: {temp_dir}")
    # cd into the temp directory and run astro project init
    os.chdir(temp_dir)
    temp_dir = Path(temp_dir)

    astro_project = _run_and_validate_subprocess(
        ["astro", "dev", "init", "-n", temp_dir.stem],
        "initialize Astro project",
        return_output=True,
        input_text="y",
    )
    print(f"Worker {os.getpid()}: Astro project initialized: {astro_project}")
    return temp_dir


def _create_user_in_airflow_deployment(deployment_name: str) -> None:
    """
    Helper method to create a user in the Airflow deployment using Astronomer CLI and environment variables in Airflow.

    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :rtype: None
    """
    username = os.getenv("AIRFLOW_USERNAME", "airflow")
    password = os.getenv("AIRFLOW_PASSWORD", "airflow")
    user_creation_commands = [
        [
            "astro", "deployment", "variable", "create",
            "_AIRFLOW_WWW_USER_CREATE=true",
            "--deployment-name", deployment_name
        ],
        [
            "astro", "deployment", "variable", "create",
            f"_AIRFLOW_WWW_USER_USERNAME={username}",
            "--deployment-name", deployment_name
        ],
        [
            "astro", "deployment", "variable", "create",
            f"_AIRFLOW_WWW_USER_PASSWORD={password}",
            "--deployment-name", deployment_name, "-s"
        ],
        [
            "astro", "deployment", "variable", "create",
            "AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth",
            "--deployment-name", deployment_name,
        ],
    ]
    for command in user_creation_commands:
        _ = _run_and_validate_subprocess(command, "creating user in Airflow deployment")
    

def cleanup_airflow_resource(
    test_name: str,
    resource_id: str,
    created_resources: list[str],
    test_dir: Optional[Path] = None,
):
    """
    Cleans up an Airflow resource, including the temp directory and the created resources in Astronomer.

    :param test_name: The name of the test.
    :param resource_id: The ID of the resource.
    :param created_resources: The list of created resources.
    :param test_dir: The path to the test directory.
    """
    if test_dir and test_dir.exists():
        try:
            shutil.rmtree(test_dir)
            print(
                f"Worker {os.getpid()}: Removed {test_name}'s temp directory: {test_dir}"
            )
        except Exception as e:
            print(f"Worker {os.getpid()}: Error removing temp directory: {e}")

    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up Airflow resource {resource_id}")
    try:
        # Clean up created resources in reverse order
        for resource in reversed(created_resources):
            _ = _run_and_validate_subprocess(
                ["astro", "deployment", "delete", resource, "-f"],
                "delete Astronomer deployment",
                check=True,
            )
        print(
            f"Worker {os.getpid()}: Airflow resource {resource_id} cleaned up successfully"
        )
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up Airflow resource: {e}")
