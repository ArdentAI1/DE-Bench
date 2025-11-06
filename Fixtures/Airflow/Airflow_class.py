"""
Unified Airflow Manager Class

This module provides a comprehensive Airflow management class that combines:
- Resource management and deployment operations
- Airflow instance interactions and monitoring
- GitHub integration and DAG management
- Docker and container operations
- Session and cache management

This replaces the separate airflow_resources.py and Airflow.py modules to provide
better resource sharing and dependency management.
"""

import copy
import base64
import fcntl
import functools
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple

import github
import requests
from dotenv import load_dotenv
from git import GitCommandError, InvalidGitRepositoryError, Repo
from github import Github
from python_on_whales import DockerClient

from Fixtures.Databricks.cache_manager import CacheManager
from braintrust import traced
from utils.parallel import map_func
from utils.processes import run_and_validate_subprocess

# Constants
VALIDATE_ASTRO_INSTALL = "Please check if the Astro CLI is installed and in PATH."
load_dotenv()


class AirflowManager:
    """
    Unified Airflow Manager that handles both resource management and Airflow operations.

    This class combines functionality for:
    - Managing Astro deployments (create, hibernate, wake up)
    - Interacting with Airflow API (DAGs, tasks, logs)
    - GitHub integration and DAG management
    - Docker operations and requirements management
    - Cache and session management
    """

    # Pattern for test runner deployment names
    TEST_RUNNER_PATTERN = "de_bench_test_runner"
    TEST_RUNNER_REGEX = re.compile(rf"^{TEST_RUNNER_PATTERN}_(\d+)$")

    def __init__(
        self,
        airflow_dir: Optional[Path] = None,
        host: Optional[str] = None,
        api_token: Optional[str] = None,
        api_url: Optional[str] = None,
        cache_manager: Optional[CacheManager] = None,
        resource_id: Optional[str] = None,
        require_astro: bool = True,
    ):
        """
        Initialize the AirflowManager with all necessary configurations.

        Args:
            airflow_dir: Directory for Airflow project files
            host: Airflow host URL
            api_token: API token for Airflow authentication
            api_url: Airflow API URL
            max_retries: Maximum number of retries for operations
            cache_manager: Shared cache manager instance
            resource_id: Unique identifier for this resource
            require_astro: If False, skip Astro environment validation (for Kubernetes mode)
        """
        # Core instance variables
        self.airflow_dir = airflow_dir.absolute() if airflow_dir else None
        self.host = host
        self.api_token = api_token
        self.api_url = api_url
        self.cache_manager = cache_manager
        self.resource_id = resource_id

        # API headers for Airflow requests
        self.api_headers = self.set_api_headers(require_astro=require_astro)

        # Deployment tracking
        self.deployment_id = None
        self.deployment_name = None
        self.test_resources = []
        self.secret_suffix = None  # Track the suffix used for GitHub secrets

        # Environment validation (skip for Kubernetes mode)
        self._validate_environment(require_astro=require_astro)

    def _validate_environment(self, require_astro: bool = True):
        """
        Validate required environment variables and installations.

        Args:
            require_astro: If False, skip Astro-specific validation (for Kubernetes mode)
        """
        # If not requiring Astro (e.g., Kubernetes mode), skip validation
        if not require_astro:
            required_envars = [
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "AZURE_TENANT_ID",
                "AZURE_SUBSCRIPTION_ID",
                "AZURE_ACR_NAME",
                "AKS_RESOURCE_GROUP",
                "AKS_CLUSTER_NAME",
                "AZURE_CREDENTIALS",
                "DE_BENCH_AKS_IMAGE_NAME",
                "DE_BENCH_AKS_CLUSTER_NAME",
                "DE_BENCH_AKS_RESOURCE_GROUP",
                "USE_KUBERNETES_AIRFLOW",
            ]

        else:
            required_envars = [
                "ASTRO_WORKSPACE_ID",
                "AIRFLOW_GITHUB_TOKEN",
                "AIRFLOW_REPO",
                "ASTRO_CLOUD_PROVIDER",
                "ASTRO_REGION",
            ]

        if missing_envars := [
            envar for envar in required_envars if not os.getenv(envar)
        ]:
            raise ValueError(f"The following envars are not set: {missing_envars}")

        if require_astro and not os.getenv("ASTRO_ACCESS_TOKEN") and not os.getenv("ASTRO_API_TOKEN"):
            raise ValueError("Either ASTRO_ACCESS_TOKEN or ASTRO_API_TOKEN must be set")

        # Validate Astro CLI installation
        if require_astro:
            self._parse_astro_version()

    def set_api_headers(self, require_astro: bool = True) -> Dict[str, str]:
        """
        Set the API headers for Airflow requests.

        :param bool require_astro: If True, use Astro authentication, otherwise use basic authentication
        :return: The API headers as a dictionary to use for subsequent requests
        :rtype: Dict[str, str]
        """
        if require_astro:
            return (
                {
                    "Authorization": f"Bearer {self.api_token}",
                    "Cache-Control": "no-cache",
                }
                if self.api_token
                else {}
            )
        # Running for Kubernetes mode, use basic authentication
        return {
            "Authorization": f"Basic {base64.b64encode(f'{os.getenv('AIRFLOW_USERNAME', 'admin')}:{os.getenv('AIRFLOW_PASSWORD', 'admin')}'.encode()).decode()}",
        }

    # ===== UTILITY METHODS =====

    def retry_astro_command(max_retries: int = 3):
        """
        Retry decorator for astro CLI commands with exponential backoff.
        Includes automatic workspace switching fallback for workspace context errors.

        Args:
            max_retries: Maximum number of retries (default 3, so 4 total attempts)

        Returns:
            Decorated function
        """

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None

                for attempt in range(max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except (
                        subprocess.CalledProcessError,
                        EnvironmentError,
                        ValueError,
                    ) as e:
                        last_exception = e
                        error_message = str(e).lower()

                        # Check stderr/stdout if it's a CalledProcessError
                        if hasattr(e, "stderr") and e.stderr:
                            error_message += " " + str(e.stderr).lower()
                        if hasattr(e, "stdout") and e.stdout:
                            error_message += " " + str(e.stdout).lower()

                        # Check for workspace context error and try to fix it
                        if (
                            "workspace context not set" in error_message
                            or "failed to find a valid workspace" in error_message
                        ):
                            try:
                                workspace_id = os.getenv("ASTRO_WORKSPACE_ID")
                                if workspace_id:
                                    print(
                                        f"Worker {os.getpid()}: Workspace context error detected, switching to workspace {workspace_id}"
                                    )
                                    run_and_validate_subprocess(
                                        ["astro", "workspace", "switch", workspace_id],
                                        "switching astro workspace",
                                    )
                                    print(
                                        f"Worker {os.getpid()}: Successfully switched to workspace {workspace_id}, retrying command..."
                                    )
                                    # Try the command again immediately after switching workspace
                                    try:
                                        return func(*args, **kwargs)
                                    except Exception:
                                        # If it still fails, continue with normal retry logic
                                        pass
                                else:
                                    print(
                                        f"Worker {os.getpid()}: Workspace context error detected but ASTRO_WORKSPACE_ID not found"
                                    )
                            except Exception as workspace_error:
                                print(
                                    f"Worker {os.getpid()}: Failed to switch workspace: {workspace_error}"
                                )

                        if attempt == max_retries:
                            print(
                                f"Worker {os.getpid()}: {func.__name__} failed after {max_retries + 1} attempts: {e}"
                            )
                            raise

                        wait_time = 2**attempt
                        print(
                            f"Worker {os.getpid()}: {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                        )
                        print(
                            f"Worker {os.getpid()}: Retrying {func.__name__} in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)

                if last_exception:
                    raise last_exception

            return wrapper

        return decorator

    # ===== ASTRO CLI AND AUTHENTICATION METHODS =====

    @retry_astro_command(max_retries=3)
    def _ensure_astro_login(self) -> None:
        """
        Ensure Astro is logged in using file-based coordination to prevent multiple logins in parallel.
        """
        if os.getenv("ASTRO_API_TOKEN"):
            print(f"Worker {os.getpid()}: Astro API token found, skipping login")
            return None

        lock_file_path = os.path.join(tempfile.gettempdir(), "astro_login.lock")

        with open(lock_file_path, "w") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

                # Check if already logged in
                try:
                    result = subprocess.run(
                        [
                            "astro",
                            "deployment",
                            "list",
                            "--workspace-id",
                            os.getenv("ASTRO_WORKSPACE_ID"),
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if result.returncode == 0:
                        print(f"Worker {os.getpid()}: Astro already logged in")
                        return None
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass

                astro_token = os.getenv("ASTRO_API_TOKEN")
                if not astro_token:
                    raise ValueError("ASTRO_API_TOKEN not found in .env file")

                print(f"Worker {os.getpid()}: Logging into Astro for test session")
                run_and_validate_subprocess(
                    ["astro", "login", "--token-login", astro_token],
                    "login to Astro (session-wide)",
                )
                print(f"Worker {os.getpid()}: Successfully logged into Astro")

            except (IOError, OSError):
                print(
                    f"Worker {os.getpid()}: Waiting for another process to complete Astro login..."
                )
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                    print(
                        f"Worker {os.getpid()}: Astro login completed by another process"
                    )
                    return None
                except Exception as e:
                    print(f"Worker {os.getpid()}: Error waiting for Astro login: {e}")
                    raise

    @retry_astro_command(max_retries=3)
    def _parse_astro_version(self) -> None:
        """
        Runs the `astro version` command to check if the Astro CLI is installed.
        """
        try:
            version = subprocess.run(
                ["astro", "version"], check=True, capture_output=True
            )
            if version.returncode != 0:
                raise subprocess.CalledProcessError(version.returncode, "astro version")

            version_pattern = re.compile(r"(\d+\.\d+\.\d+)")
            match = version_pattern.search(version.stdout.decode("utf-8"))
            if not match:
                raise EnvironmentError("Could not parse Astro CLI version from output")

            astro_version = match.group(1)
            print(f"Worker {os.getpid()}: Astro CLI version: {astro_version}")
        except Exception as e:
            print(
                "The Astro CLI is not installed or not in PATH. Please install it "
                "from https://docs.astronomer.io/cli/installation"
            )
            raise e from e

    # ===== DEPLOYMENT MANAGEMENT METHODS =====

    @traced(name="_create_deployment_in_astronomer")
    @retry_astro_command(max_retries=3)
    def _create_deployment_in_astronomer(
        self, deployment_name: str, wait: Optional[bool] = True
    ) -> Optional[str]:
        """
        Creates a deployment in Astronomer.

        Args:
            deployment_name: The name of the deployment to create
            wait: Whether to wait for deployment to be created

        Returns:
            The ID of the created deployment, or None if wait is False
        """
        try:
            response = run_and_validate_subprocess(
                [
                    "astro",
                    "deployment",
                    "create",
                    "--workspace-id",
                    os.getenv("ASTRO_WORKSPACE_ID"),
                    "--name",
                    deployment_name,
                    "--runtime-version",
                    os.getenv("ASTRO_RUNTIME_VERSION", "13.1.0"),
                    "--development-mode",
                    "enable",
                    "--cloud-provider",
                    os.getenv("ASTRO_CLOUD_PROVIDER"),
                    "--region",
                    os.getenv("ASTRO_REGION", "us-east-1"),
                    "--scheduler-size",
                    "small",
                    "--wait" if wait else "",
                ],
                "creating Astronomer deployment",
                return_output=True,
            )

            if not wait:
                return None

            deployment_id_pattern = re.compile(r"deployments/([a-zA-Z0-9]+)")
            match = deployment_id_pattern.search(response)
            if not match:
                raise EnvironmentError("Could not parse deployment ID from output")

            deployment_id = match.group(1)
            print(
                f"Worker {os.getpid()}: Created Astronomer deployment: {deployment_id}"
            )

            # Set required variables immediately on creation (never on wake)
            self._create_variables_in_airflow_deployment(deployment_name)

            # Applying variables can trigger a restart; wait for healthy
            self._validate_deployment_status(
                deployment_name=deployment_name, expected_status="healthy"
            )

            return deployment_id
        except Exception as e:
            print(f"Worker {os.getpid()}: Error creating Astronomer deployment: {e}")
            raise e from e

    @traced(name="_check_deployment_status")
    @retry_astro_command(max_retries=3)
    def _check_deployment_status(self, deployment_name: str) -> str:
        """
        Helper method to check the status of a deployment in Astronomer.

        Args:
            deployment_name: The name of the Airflow deployment

        Returns:
            The status of the deployment
        """
        status = run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "inspect",
                "--deployment-name",
                deployment_name,
                "--key",
                "metadata.status",
            ],
            "getting Astro deployment status",
            return_output=True,
        )

        print(f"Worker {os.getpid()}: Deployment {deployment_name} status: {status}")
        return status

    @traced(name="_validate_deployment_status")
    def _validate_deployment_status(
        self, deployment_name: str, expected_status: str
    ) -> None:
        """
        Validates the status of a deployment in Astronomer.

        Args:
            deployment_name: The name of the deployment
            expected_status: The expected status of the deployment
        """
        start_time = time.time()
        print(
            f"Worker {os.getpid()}: Waiting for deployment '{deployment_name}' to have status '{expected_status}'..."
        )

        for _ in range(30):
            status = self._check_deployment_status(deployment_name)
            if status.lower() == expected_status.lower():
                end_time = time.time()
                print(
                    f"Worker {os.getpid()}: Deployment {deployment_name} is {expected_status} "
                    f"after {end_time - start_time:.2f}s"
                )
                return
            time.sleep(10)

        raise TimeoutError(
            f"Deployment {deployment_name} did not become {expected_status} in time."
        )

    @traced(name="_wake_up_deployment")
    @retry_astro_command(max_retries=3)
    def _wake_up_deployment(self, deployment_name: str) -> None:
        """
        Helper method to wake up a deployment in Astronomer.

        Args:
            deployment_name: The name of the deployment to wake up
        """
        wake_up_result = run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "wake-up",
                "--deployment-name",
                deployment_name,
                "-f",
            ],
            "waking up deployment",
        )

        if wake_up_result:
            self._validate_deployment_status(
                deployment_name=deployment_name, expected_status="healthy"
            )
        else:
            print(f"Unable to wake up deployment {deployment_name}: {wake_up_result}")
            raise EnvironmentError(f"Unable to wake up deployment {deployment_name}")

    @retry_astro_command(max_retries=3)
    def _hibernate_deployment(self, deployment_name: str) -> None:
        """
        Helper method to hibernate a deployment in Astronomer.

        Args:
            deployment_name: The name of the deployment to hibernate
        """
        print(f"Worker {os.getpid()}: Hibernating deployment {deployment_name}...")
        hibernating_result = run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "hibernate",
                "--deployment-name",
                deployment_name,
                "-f",
            ],
            "hibernating deployment",
        )

        if hibernating_result:
            print(
                f"Worker {os.getpid()}: Deployment {deployment_name} hibernated successfully."
            )
        else:
            print(
                f"Unable to hibernate deployment {deployment_name}: {hibernating_result}"
            )
            raise EnvironmentError(f"Unable to hibernate deployment {deployment_name}")

    @traced(name="fetch_astro_deployments")
    @retry_astro_command(max_retries=3)
    def fetch_astro_deployments(self) -> List[Dict[str, str]]:
        """
        Helper method to find deployments in Astronomer.

        Returns:
            List of astro deployments with their name, id, and status
        """
        astro_deployments: List[Dict[str, str]] = []

        astro_workspace_id = os.getenv("ASTRO_WORKSPACE_ID")

        kwargs = {
            "command": [
                "astro",
                "deployment",
                "list",
                "--workspace-id",
                astro_workspace_id,
            ],
            "process_description": "listing deployments in Astronomer",
            "return_output": True,
        }
        deployment_command_output = run_and_validate_subprocess(**kwargs)

        deployments = deployment_command_output.split("\n")
        if index := next(
            (i for i, line in enumerate(deployments) if "NAME" in line), None
        ):
            deployments = deployments[index + 1 :]

        deployments = [
            deployment.split() for deployment in deployments if deployment.strip()
        ]
        deployments = [
            deployment
            for deployment in deployments
            if len(deployment) > 5 and deployment[0] != "NAME"
        ]
        deployments = {deployment[0]: deployment[5] for deployment in deployments}

        # Filter deployments to only include those matching the test runner pattern
        test_runner_deployments = {}
        for deployment_name, deployment_id in deployments.items():
            if self.TEST_RUNNER_REGEX.match(deployment_name):
                test_runner_deployments[deployment_name] = deployment_id

        def build_deployment_info(deployment_item):
            """Helper function to build deployment info with status check"""
            deployment_name, deployment_id = deployment_item

            status = "UNKNOWN"

            try:
                status = self._check_deployment_status(deployment_name)
            except Exception as e:
                print(f"Worker {os.getpid()}: Error checking deployment status: {e}")

            # Extract runner number from deployment name
            match = self.TEST_RUNNER_REGEX.match(deployment_name)
            runner_number = int(match.group(1)) if match else None

            return {
                "deployment_name": deployment_name,
                "deployment_id": deployment_id,
                "status": status,
                "runner_number": runner_number,
            }

        # Use parallel processing to check all deployment statuses
        astro_deployments = map_func(
            build_deployment_info, test_runner_deployments.items()
        )

        print(
            f"Worker {os.getpid()}: found {len(test_runner_deployments)} test runner deployments."
        )
        return astro_deployments

    # ===== PROJECT AND DIRECTORY MANAGEMENT =====

    @retry_astro_command(max_retries=3)
    def _create_dir_and_astro_project(self, unique_id: str) -> Path:
        """
        Creates a directory and an Astro project in it.

        Args:
            unique_id: The unique id for the test

        Returns:
            The path to the created directory
        """
        project_root = Path(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).parent
        tmp_root = os.path.join(project_root, "tmp_airflow_tests")
        os.makedirs(tmp_root, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix=f"airflow_test_{unique_id}", dir=tmp_root)

        dags_dir = os.path.join(temp_dir, "dags")
        os.makedirs(dags_dir, exist_ok=True)
        print(f"Worker {os.getpid()}: Created temp directory for Airflow: {temp_dir}")

        os.chdir(temp_dir)
        temp_dir = Path(temp_dir)

        astro_project = run_and_validate_subprocess(
            ["astro", "dev", "init", "-n", temp_dir.stem],
            "initialize Astro project",
            return_output=True,
            input_text="y",
        )
        print(f"Worker {os.getpid()}: Astro project initialized: {astro_project}")
        return temp_dir

    def get_airflow_dags_from_github(self):
        """
        Clone the GitHub repository and copy DAG files to the Airflow dags directory.
        Uses environment variables from .env file.
        """
        github_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        repo_url = os.getenv("AIRFLOW_REPO")
        dag_path = os.getenv("AIRFLOW_DAG_PATH", "dags/")

        print(f"Current working directory: {os.getcwd()}")
        print(f"Airflow directory: {self.airflow_dir}")
        print(f"Destination DAG path: {os.path.join(self.airflow_dir, 'dags')}")

        if not github_token:
            raise ValueError(
                "The AIRFLOW_GITHUB_TOKEN environment variable is not set."
            )
        if not repo_url:
            raise ValueError("The AIRFLOW_REPO environment variable is not set.")

        if repo_url.startswith("https://"):
            repo_url = repo_url.replace("https://", f"https://{github_token}@")
        else:
            raise ValueError("The AIRFLOW_REPO URL must start with 'https://'.")

        git_repo_path = os.path.join(self.airflow_dir, "GitRepo")
        destination_dag_path = os.path.join(self.airflow_dir, "dags")

        print(f"Creating/checking destination DAG directory: {destination_dag_path}")
        if not os.path.exists(destination_dag_path):
            print(f"Creating directory: {destination_dag_path}")
            os.makedirs(destination_dag_path, mode=0o755)
        else:
            print(f"Directory already exists: {destination_dag_path}")
            try:
                os.chmod(destination_dag_path, 0o755)
                print(f"Set directory permissions to 0o755")
            except (PermissionError, OSError) as e:
                print(f"Could not change directory permissions: {e}")
                pass

        # Verify directory is writable
        try:
            test_file = os.path.join(destination_dag_path, ".test_write")
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            print(f"Directory {destination_dag_path} is writable")
        except Exception as e:
            print(f"Warning: Directory {destination_dag_path} is not writable: {e}")
            try:
                os.system(f"chmod -R 755 '{destination_dag_path}'")
                print(f"Attempted to fix permissions on {destination_dag_path}")
            except Exception as chmod_e:
                print(f"Could not fix permissions: {chmod_e}")

        try:
            is_valid_repo = False
            if os.path.exists(git_repo_path):
                try:
                    contents = os.listdir(git_repo_path)
                    if len(contents) == 1 and contents[0] == ".gitkeep":
                        pass
                    else:
                        repo = Repo(git_repo_path)
                        is_valid_repo = True

                        origin = repo.remotes.origin
                        if repo_url != origin.url:
                            origin.set_url(repo_url)

                        repo.git.reset("--hard", "origin/main")
                        pull_info = origin.pull()

                        source_dag_path = os.path.join(git_repo_path, dag_path)
                except (InvalidGitRepositoryError, GitCommandError) as e:
                    for item in os.listdir(git_repo_path):
                        if item != ".gitkeep":
                            item_path = os.path.join(git_repo_path, item)
                            if os.path.isfile(item_path):
                                os.unlink(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                    is_valid_repo = False

            if not is_valid_repo:
                os.makedirs(git_repo_path, exist_ok=True)

                for item in os.listdir(git_repo_path):
                    if item != ".gitkeep":
                        item_path = os.path.join(git_repo_path, item)
                        if os.path.isfile(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)

                gitkeep_exists = os.path.exists(os.path.join(git_repo_path, ".gitkeep"))
                if gitkeep_exists:
                    os.rename(
                        os.path.join(git_repo_path, ".gitkeep"),
                        os.path.join(self.airflow_dir, ".gitkeep_temp"),
                    )

                try:
                    Repo.clone_from(repo_url, git_repo_path)
                finally:
                    if gitkeep_exists:
                        os.rename(
                            os.path.join(self.airflow_dir, ".gitkeep_temp"),
                            os.path.join(git_repo_path, ".gitkeep"),
                        )

        except GitCommandError as e:
            raise RuntimeError(f"Git operation failed: {e}")

        source_dag_path = os.path.join(git_repo_path, dag_path)

        if not os.path.exists(source_dag_path):
            raise FileNotFoundError(
                f"The specified DAG path {source_dag_path} does not exist in the repository."
            )

        # Copy DAG files to the Airflow DAGs directory
        for item in os.listdir(source_dag_path):
            s = os.path.join(source_dag_path, item)
            d = os.path.join(destination_dag_path, item)

            if not os.path.exists(s):
                print(f"Warning: Source path does not exist: {s}")
                continue

            if not os.access(s, os.R_OK):
                print(f"Warning: Source path is not readable: {s}")
                continue

            if os.path.isdir(s):
                if os.path.exists(d):
                    shutil.rmtree(d)
                shutil.copytree(s, d)
            else:
                try:
                    os.chmod(destination_dag_path, 0o755)
                except (PermissionError, OSError):
                    pass

                try:
                    print(f"Copying from: {s}")
                    print(f"Copying to: {d}")

                    if os.path.exists(d):
                        print(f"Destination file exists, attempting to remove: {d}")
                        removed = False

                        try:
                            os.chmod(d, 0o666)
                            os.remove(d)
                            removed = True
                            print(f"Successfully removed existing file: {d}")
                        except (PermissionError, OSError) as e:
                            print(f"Normal remove failed: {e}")

                        if not removed:
                            try:
                                result = os.system(f"rm -f '{d}'")
                                if result == 0:
                                    removed = True
                                    print(
                                        f"Successfully removed file using rm command: {d}"
                                    )
                                else:
                                    print(f"rm command failed with exit code: {result}")
                            except Exception as rm_e:
                                print(f"rm command failed: {rm_e}")

                        if not removed:
                            try:
                                os.system(f"chmod 777 '{d}'")
                                os.remove(d)
                                removed = True
                                print(f"Successfully removed file after chmod: {d}")
                            except Exception as chmod_e:
                                print(f"chmod + remove failed: {chmod_e}")

                        if not removed:
                            print(
                                f"Warning: Could not remove existing file {d}, skipping..."
                            )
                            continue

                    dest_dir = os.path.dirname(d)
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir, mode=0o755)
                    else:
                        try:
                            os.chmod(dest_dir, 0o755)
                        except (PermissionError, OSError):
                            pass

                    print(f"Attempting to copy file using shutil.copy2...")
                    try:
                        shutil.copy2(s, d)
                        print(f"Successfully copied file using shutil.copy2")
                    except Exception as copy_error:
                        print(f"shutil.copy2 failed: {copy_error}")
                        try:
                            with open(s, "rb") as fsrc:
                                with open(d, "wb") as fdst:
                                    fdst.write(fsrc.read())
                            print(f"Successfully copied file using manual method")
                        except Exception as manual_error:
                            print(f"Manual copy also failed: {manual_error}")
                            raise

                    try:
                        os.chmod(d, 0o644)
                        print(f"Set file permissions to 0o644")
                    except (PermissionError, OSError) as perm_error:
                        print(f"Could not set file permissions: {perm_error}")
                        pass

                except Exception as e:
                    print(f"Unexpected error copying {s} to {d}: {e}")
                    continue

    # ===== AIRFLOW API INTERACTION METHODS =====
    @traced(name="wait_for_airflow_to_be_ready")
    def wait_for_airflow_to_be_ready(
        self, wait_time_in_minutes: Optional[int] = 0
    ) -> bool:
        """
        Strict readiness gate for Airflow webserver and API.

        - Requires HTTP 200 for both /health and /api/v1/dags
        - Requires "healthy" in /health body
        - Logs short bodies for debugging
        - Includes a short stabilization recheck
        """
        time.sleep(60 * wait_time_in_minutes)

        def _log(label, resp):
            try:
                preview = (resp.text or "")[:160].replace("\n", " ")
            except Exception:
                preview = "<no-body>"
            print(f"{label}: {resp.status_code} {preview}")

        retries = 10
        while retries > 0:
            print(f"Checking Airflow readiness... {retries} retries left")
            try:
                health = requests.get(
                    f"{self.host.rstrip('/')}/health",
                    headers=self.api_headers,
                    timeout=5,
                )
                dags = requests.get(
                    f"{self.host.rstrip('/')}/api/v1/dags",
                    headers=self.api_headers,
                    timeout=5,
                )

                # Strict: 200 AND body mentions 'healthy'
                health_ok = (health.status_code == 200) and (
                    "healthy" in (health.text or "").lower()
                )
                # Strict: 200 only; 302 is typically a login redirect -> NOT ready
                api_ok = dags.status_code == 200

                if not health_ok or not api_ok:
                    _log("health", health)
                    _log("dags", dags)

                if health_ok and api_ok:
                    # Stabilization window
                    time.sleep(10)
                    health2 = requests.get(
                        f"{self.host.rstrip('/')}/health",
                        headers=self.api_headers,
                        timeout=5,
                    )
                    dags2 = requests.get(
                        f"{self.host.rstrip('/')}/api/v1/dags",
                        headers=self.api_headers,
                        timeout=5,
                    )
                    health2_ok = (health2.status_code == 200) and (
                        "healthy" in (health2.text or "").lower()
                    )
                    api2_ok = dags2.status_code == 200
                    if health2_ok and api2_ok:
                        print("✅ Airflow webserver and API are stable and ready")
                        return True
                    else:
                        print("⚠️ Flap during stabilization")
                        _log("health2", health2)
                        _log("dags2", dags2)
            except (
                requests.ConnectTimeout,
                requests.ConnectionError,
                requests.Timeout,
            ) as e:
                print(f"Connection error during readiness: {e}")
            except requests.RequestException as e:
                print(f"Request error during readiness: {e}")

            retries -= 1
            time.sleep(30)

        print("Airflow webserver is NOT ready after retries")
        return False

    def verify_airflow_dag_exists(
        self, dag_id: str, max_wait_minutes: Optional[int] = 8
    ) -> bool:
        """
        Verify if a DAG exists using the dag_id in Airflow via API call.
        Also checks for import errors and returns False immediately if the DAG has import errors.

        Args:
            dag_id: The ID of the DAG to check for
            max_wait_minutes: Maximum time to wait in minutes

        Returns:
            True if the DAG exists and has no import errors, False otherwise
        """
        wait_time_seconds = 20
        max_wait_seconds = max_wait_minutes * 60
        max_retries = max_wait_seconds // wait_time_seconds

        for attempt in range(max_retries):
            print(
                f"Attempt {attempt + 1}/{max_retries}: Checking for DAG '{dag_id}'..."
            )

            # Check for import errors first
            try:
                import_errors = self.get_dag_import_errors()
                for error in import_errors:
                    error_filename = error.get("filename", "")
                    # Check if the import error is related to our DAG
                    if dag_id in error_filename or dag_id in error.get(
                        "stack_trace", ""
                    ):
                        print(
                            f"❌ DAG '{dag_id}' has import error: {error.get('stack_trace', 'Unknown error')}"
                        )
                        return False
            except Exception as e:
                print(f"⚠️ Warning: Could not check import errors: {e}")

            dag_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}",
                headers=self.api_headers,
            )

            if dag_response.status_code == 200:
                print(f"✅ DAG '{dag_id}' found!")
                return True
            elif dag_response.status_code == 404:
                print(f"⏳ DAG '{dag_id}' not found yet (status: 404)")

                if attempt % 6 == 5:
                    self._list_available_dags()
            else:
                print(
                    f"⚠️ Unexpected response (status: {dag_response.status_code}): {dag_response.text}"
                )

            if attempt == max_retries - 1:
                print(
                    f"❌ DAG '{dag_id}' not found after {max_retries} attempts "
                    f"({max_retries * wait_time_seconds / 60:.1f} minutes)"
                )
                self._list_available_dags()
                raise Exception(
                    f"DAG '{dag_id}' not found after max retries. Check DAG deployment and syntax."
                )

            print(f"Waiting {wait_time_seconds} seconds before next attempt...")
            time.sleep(wait_time_seconds)

        return False

    def _list_available_dags(self):
        """Helper method to list all available DAGs for debugging purposes."""
        try:
            print("🔍 Listing all available DAGs for debugging...")
            dags_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags",
                headers=self.api_headers,
            )

            if dags_response.status_code == 200:
                dags_data = dags_response.json()
                dags = dags_data.get("dags", [])

                if dags:
                    print(f"📋 Found {len(dags)} DAGs in Airflow:")
                    for dag in dags[:10]:
                        dag_id = dag.get("dag_id", "Unknown")
                        is_paused = dag.get("is_paused", True)
                        status = "⏸️ Paused" if is_paused else "▶️ Active"
                        print(f"  - {dag_id} ({status})")

                    if len(dags) > 10:
                        print(f"  ... and {len(dags) - 10} more DAGs")
                else:
                    print("📭 No DAGs found in Airflow")
            else:
                print(
                    f"❌ Failed to list DAGs (status: {dags_response.status_code}): {dags_response.text}"
                )

        except Exception as e:
            print(f"❌ Error listing DAGs: {e}")

    def unpause_and_trigger_airflow_dag(
        self, dag_id: str, max_retries: Optional[int] = 5
    ) -> Optional[str]:
        """
        Unpause a DAG using the dag_id in Airflow via API call.

        Args:
            dag_id: The ID of the DAG to unpause
            max_retries: Maximum number of retries, defaults to 5

        Returns:
            The dag_run_id if triggered successfully, else None
        """
        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1}/{max_retries}: Checking for DAG...")

            unpause_response = requests.patch(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}",
                headers=self.api_headers,
                json={"is_paused": False},
            )

            if unpause_response.status_code != 200:
                print(f"Failed to unpause DAG: {unpause_response.text}")
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to unpause DAG: {unpause_response.text}")
                time.sleep(10)
                continue

            print(f"DAG unpaused successfully. Triggering DAG...")

            trigger_response = requests.post(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns",
                headers=self.api_headers,
                json={"conf": {}},
            )

            if trigger_response.status_code == 200:
                dag_run_id = trigger_response.json()["dag_run_id"]
                print(f"DAG triggered successfully! Run ID: {dag_run_id}")
                return dag_run_id
            else:
                print(f"Failed to trigger DAG: {trigger_response.text}")
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to trigger DAG: {trigger_response.text}")
                time.sleep(10)
                continue

        return None

    def verify_dag_id_ran(
        self, dag_id: str, dag_run_id: str, max_retries: int = 10
    ) -> bool:
        """
        Verify if a DAG has been executed.

        Args:
            dag_id: The ID of the DAG to check for
            dag_run_id: The ID of the DAG run to check for
            max_retries: Maximum number of retries, defaults to 10

        Returns:
            True if the DAG has been executed, False otherwise
        """
        print(f"Monitoring DAG run {dag_run_id} for completion...")

        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1}/{max_retries}: Checking for DAG run...")
            status_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                headers=self.api_headers,
            )

            if status_response.status_code == 200:
                state = status_response.json()["state"]
                print(f"DAG run state: {state}")
                if state in ["success", "failed", "error"]:
                    print(f"DAG ran to completion with state: {state}")
                    return True
                else:
                    print(f"DAG run state: {state}")
                    time.sleep(60)
                    continue

        return self.check_dag_task_instances(dag_id, dag_run_id)

    def get_task_instance_logs(self, dag_id: str, dag_run_id: str, task_id: str) -> str:
        """
        Get the logs for a task instance.

        Args:
            dag_id: The ID of the DAG
            dag_run_id: The ID of the DAG run
            task_id: The ID of the task

        Returns:
            The logs for the task instance
        """
        print(f"Retrieving logs for task '{task_id}'")
        task_instance_response = requests.get(
            f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
            headers=self.api_headers,
        )

        if task_instance_response.status_code != 200:
            raise Exception(
                f"Failed to retrieve task instance details: {task_instance_response.text}"
            )

        print(f"Task instance response: {task_instance_response.text}")
        task_instance_data = task_instance_response.json()
        try_number = task_instance_data.get("try_number", 1)
        print(f"Fetching logs for task '{task_id}' with try number: {try_number}")

        task_logs_response = requests.get(
            f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
            headers=self.api_headers,
        )

        if task_logs_response.status_code != 200:
            raise Exception(f"Failed to retrieve task logs: {task_logs_response.text}")

        print(f"Task logs received for task '{task_id}' with try number: {try_number}")
        return task_logs_response.text

    def get_dag_source_code(self, dag_id: str, github_manager=None) -> dict:
        """
        Get the DAG source code and details from Airflow and GitHub.

        Args:
            dag_id: The ID of the DAG to retrieve source for
            github_manager: Optional GitHub manager

        Returns:
            Dictionary containing DAG details and source code
        """
        print(f"🔍 Retrieving DAG source code for: {dag_id}")

        dag_details_response = requests.get(
            f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/details",
            headers=self.api_headers,
        )

        dag_info = {
            "dag_id": dag_id,
            "source_code": None,
            "github_files": {},
            "file_path": None,
            "dag_details": None,
            "error": None,
        }

        if dag_details_response.status_code == 200:
            dag_details = dag_details_response.json()
            dag_info["dag_details"] = dag_details
            dag_info["file_path"] = dag_details.get("fileloc")
            print(f"📄 DAG file location: {dag_details.get('fileloc', 'Unknown')}")
        else:
            dag_info["error"] = (
                f"Failed to retrieve DAG details: {dag_details_response.text}"
            )
            print(f"❌ {dag_info['error']}")

        if github_manager:
            print(
                "📝 GitHub source code available in agent_code_snapshot (captured during test)"
            )
            dag_info["github_note"] = (
                "Source code available in agent_code_snapshot from test metadata"
            )

        return dag_info

    def get_dag_import_errors(self) -> list:
        """
        Get all DAG import errors from Airflow.

        Returns:
            List of import errors
        """
        print("🔍 Retrieving DAG import errors...")

        import_errors_response = requests.get(
            f"{self.host.rstrip('/')}/api/v1/importErrors",
            headers=self.api_headers,
        )

        if import_errors_response.status_code == 200:
            import_errors = import_errors_response.json().get("import_errors", [])
            print(f"📋 Found {len(import_errors)} import errors")
            return import_errors
        else:
            print(f"❌ Failed to retrieve import errors: {import_errors_response.text}")
            return []

    def get_all_task_logs_for_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """
        Get logs for all tasks in a DAG run.

        Args:
            dag_id: The ID of the DAG
            dag_run_id: The ID of the DAG run

        Returns:
            Dictionary mapping task_id to logs
        """
        print(f"🔍 Retrieving all task logs for DAG run: {dag_run_id}")

        task_instances_response = requests.get(
            f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            headers=self.api_headers,
        )

        all_logs = {}

        if task_instances_response.status_code == 200:
            task_instances = task_instances_response.json().get("task_instances", [])

            for task_instance in task_instances:
                task_id = task_instance["task_id"]
                try_number = task_instance.get("try_number", 1)
                state = task_instance.get("state", "unknown")

                try:
                    logs = self.get_task_instance_logs(dag_id, dag_run_id, task_id)
                    all_logs[task_id] = {
                        "logs": logs,
                        "state": state,
                        "try_number": try_number,
                        "start_date": task_instance.get("start_date"),
                        "end_date": task_instance.get("end_date"),
                        "duration": task_instance.get("duration"),
                    }
                except Exception as e:
                    all_logs[task_id] = {
                        "logs": f"Error retrieving logs: {str(e)}",
                        "state": state,
                        "try_number": try_number,
                        "error": str(e),
                    }
        else:
            print(
                f"❌ Failed to retrieve task instances: {task_instances_response.text}"
            )

        return all_logs

    def get_comprehensive_dag_info(
        self, dag_id: str, dag_run_id: str = None, github_manager=None
    ) -> dict:
        """
        Get comprehensive information about a DAG including source code, errors, and logs.

        Args:
            dag_id: The ID of the DAG
            dag_run_id: Optional DAG run ID for execution logs
            github_manager: Optional GitHub manager

        Returns:
            Comprehensive DAG information
        """
        print(f"📊 Gathering comprehensive DAG information for: {dag_id}")

        comprehensive_info = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "timestamp": time.time(),
            "dag_source": self.get_dag_source_code(dag_id, github_manager),
            "import_errors": self.get_dag_import_errors(),
            "task_logs": {},
            "dag_run_info": None,
        }

        if dag_run_id:
            comprehensive_info["task_logs"] = self.get_all_task_logs_for_dag_run(
                dag_id, dag_run_id
            )

            dag_run_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                headers=self.api_headers,
            )

            if dag_run_response.status_code == 200:
                comprehensive_info["dag_run_info"] = dag_run_response.json()

        return comprehensive_info

    def get_dag_tasks(self, dag_id: str, max_retries: Optional[int] = 5) -> list:
        """
        Get all tasks for a specific DAG.

        Args:
            dag_id: The ID of the DAG to get tasks for
            max_retries: Maximum number of retries, defaults to 5

        Returns:
            List of task dictionaries containing task information
        """
        print(f"🔍 Retrieving tasks for DAG: {dag_id}")

        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1}/{max_retries}: Getting DAG tasks...")

            tasks_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/tasks",
                headers=self.api_headers,
            )

            if tasks_response.status_code == 200:
                tasks_data = tasks_response.json()
                tasks = tasks_data.get("tasks", [])
                print(f"✅ Retrieved {len(tasks)} tasks for DAG '{dag_id}'")
                return tasks
            elif tasks_response.status_code == 404:
                print(f"❌ DAG '{dag_id}' not found (status: 404)")
                if attempt == max_retries - 1:
                    raise Exception(f"DAG '{dag_id}' not found")
            else:
                print(
                    f"⚠️ Unexpected response (status: {tasks_response.status_code}): {tasks_response.text}"
                )
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to retrieve tasks for DAG '{dag_id}': {tasks_response.text}"
                    )

            if attempt < max_retries - 1:
                print("Waiting 10 seconds before retry...")
                time.sleep(10)

        return []

    def check_dag_task_instances(
        self, dag_id: str, dag_run_id: str, max_retries: Optional[int] = 5
    ) -> bool:
        """
        Check if all tasks in a DAG have been executed.

        Args:
            dag_id: The ID of the DAG
            dag_run_id: The ID of the DAG run
            max_retries: Maximum number of retries, defaults to 5

        Returns:
            True if tasks have been executed, False otherwise
        """
        for attempt in range(max_retries):
            print(
                f"Attempt {attempt + 1}/{max_retries}: Checking for DAG task instances..."
            )
            task_instances_response = requests.get(
                f"{self.host.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?limit=100",
                headers=self.api_headers,
            )

            if task_instances_response.status_code == 200:
                task_instances = task_instances_response.json()["task_instances"]
                if task_instances_response.json()["total_entries"] == 0:
                    print("Rechecking task instances...")
                    time.sleep(30)
                    continue
                elif task_instances_response.json()["total_entries"] >= 1:
                    for task_instance in task_instances:
                        if task_instance["state"] in [
                            "success",
                            "failed",
                            "error",
                            "up_for_retry",
                        ]:
                            print(
                                f"Task instance {task_instance['task_id']} completed successfully"
                            )
                            return True
                        else:
                            print(
                                f"Task instance {task_instance['task_id']} is still running"
                            )
                            continue
                else:
                    print(f"Task instances found: {len(task_instances)}")
                    time.sleep(30)
                    continue
                time.sleep(30)
                continue

        raise Exception("DAG task instances timed out")

    # ===== DOCKER AND REQUIREMENTS MANAGEMENT =====

    def cleanup_airflow_directories(self):
        """
        Clean up all Airflow directories while preserving .gitkeep files.
        Also resets the database to clear all DAG history and metadata.
        """
        try:
            directories = {
                "dags": os.path.join(self.airflow_dir, "dags"),
                "git_repo": os.path.join(self.airflow_dir, "GitRepo"),
                "config": os.path.join(self.airflow_dir, "config"),
                "logs": os.path.join(self.airflow_dir, "logs"),
                "plugins": os.path.join(self.airflow_dir, "plugins"),
            }

            cache_path = os.path.join(self.airflow_dir, ".requirements_cache")
            if os.path.exists(cache_path):
                print("Removing requirements cache...")
                os.remove(cache_path)

            permanent_requirements_path = os.path.join(
                self.airflow_dir, "Requirements", "requirements.txt"
            )
            if os.path.exists(permanent_requirements_path):
                print("Resetting requirements.txt file...")
                with open(permanent_requirements_path, "w") as f:
                    pass

            for dir_name, dir_path in directories.items():
                if os.path.exists(dir_path):
                    try:
                        for item in os.listdir(dir_path):
                            if item != ".gitkeep":
                                item_path = os.path.join(dir_path, item)
                                if os.path.isfile(item_path):
                                    os.unlink(item_path)
                                elif os.path.isdir(item_path):
                                    shutil.rmtree(item_path)

                        gitkeep_path = os.path.join(dir_path, ".gitkeep")
                        if not os.path.exists(gitkeep_path):
                            with open(gitkeep_path, "w") as f:
                                pass

                    except Exception as e:
                        print(f"Error while cleaning {dir_name} directory: {e}")
                else:
                    print(f"{dir_name} directory does not exist: {dir_path}")

            return True
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False

    def update_airflow_requirements(self):
        """
        Check if requirements.txt has changed and rebuild Airflow containers only if necessary.

        Returns:
            True if containers were rebuilt, False otherwise
        """
        requirements_path = os.path.join(
            self.airflow_dir, "GitRepo", "Requirements", "requirements.txt"
        )
        permanent_requirements_path = os.path.join(
            self.airflow_dir, "Requirements", "requirements.txt"
        )
        cache_path = os.path.join(self.airflow_dir, ".requirements_cache")

        if not os.path.exists(requirements_path):
            print("No requirements.txt found, skipping requirements update")
            return False

        with open(requirements_path, "r") as f:
            requirements_content = f.read().strip()
            if not requirements_content:
                print("Requirements file is empty, skipping update")
                return False

        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                cached_content = f.read().strip()
                if cached_content == requirements_content:
                    print("Requirements unchanged, skipping rebuild")
                    return False

        print("Requirements have changed, rebuilding Airflow containers...")

        if os.path.exists(os.path.dirname(permanent_requirements_path)):
            with open(requirements_path, "r") as src:
                content = src.read()
                with open(permanent_requirements_path, "w") as dst:
                    dst.write(content)

        with open(cache_path, "w") as f:
            f.write(requirements_content)

        if not os.path.exists(self.airflow_dir / "docker-compose.yml"):
            print(
                f"docker-compose.yml not found in {self.airflow_dir}. Please ensure it exists."
            )
            return False

        docker = DockerClient(
            compose_files=[os.path.join(self.airflow_dir, "docker-compose.yml")]
        )

        print("Docker client context:")
        print(f"Docker client context: {docker.context}")

        try:
            print("Stopping existing containers...")
            docker.compose.down(volumes=True)

            time.sleep(10)

            print("Building containers with no cache...")
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    os.path.join(self.airflow_dir, "docker-compose.yml"),
                    "build",
                    "--no-cache",
                ],
                check=True,
            )

            print("Starting containers...")
            docker.compose.up(detach=True)

            print("Waiting for services to be ready...")
            time.sleep(30)

            print("Airflow containers rebuilt and restarted with new requirements")
            return True

        except Exception as e:
            print(f"Error during container rebuild: {e}")
            try:
                docker.compose.up(detach=True)
            except:
                pass
            raise

    # ===== RESOURCE MANAGEMENT AND FIXTURE METHODS =====

    @classmethod
    def create_resource(
        cls, request, build_template: dict, shared_cache_manager: CacheManager
    ) -> "AirflowManager":
        """
        Create an AirflowManager instance for test fixtures.

        Args:
            request: pytest request object
            build_template: Template configuration for the resource
            shared_cache_manager: Shared cache manager instance

        Returns:
            Configured AirflowManager instance
        """
        resource_id = build_template["resource_id"]
        start_time = time.time()

        print(f"Worker {os.getpid()}: Starting airflow_resource for {resource_id}")

        # Create manager instance
        manager = cls(cache_manager=shared_cache_manager, resource_id=resource_id)

        # Ensure Astro login
        manager._ensure_astro_login()

        # Create project directory
        test_dir = manager._create_dir_and_astro_project(resource_id)
        manager.airflow_dir = test_dir

        try:
            # Try to allocate hibernating deployment from cache
            print(
                f"Worker {os.getpid()}: Attempting to allocate hibernating deployment for {resource_id}"
            )

            if deployment_info := shared_cache_manager.allocate_astronomer_deployment(
                resource_id, os.getpid()
            ):
                astro_deployment_id = deployment_info["deployment_id"]
                astro_deployment_name = deployment_info["deployment_name"]
                print(
                    f"Worker {os.getpid()}: Allocated hibernating deployment: {astro_deployment_name}"
                )
                manager._wake_up_deployment(astro_deployment_name)
                # validate proper environment variables
            else:
                print(
                    f"Worker {os.getpid()}: No hibernating deployments available, creating new deployment: {resource_id}"
                )
                astro_deployment_id = manager._create_deployment_in_astronomer(
                    resource_id
                )
                astro_deployment_name = resource_id

            # Update instance state
            manager.deployment_id = astro_deployment_id
            manager.deployment_name = astro_deployment_name
            manager.test_resources.append((astro_deployment_name, shared_cache_manager))

            import hashlib

            hash_suffix = hashlib.sha256(astro_deployment_name.encode()).hexdigest()[:8]
            manager.secret_suffix = hash_suffix

            # Get deployment info and set up API access
            fresh_deployment_id = manager._get_deployment_id_by_name(
                astro_deployment_name
            )
            if not fresh_deployment_id:
                raise EnvironmentError(
                    f"Could not find deployment ID for deployment {astro_deployment_name}"
                )

            print(
                f"Worker {os.getpid()}: Using fresh deployment ID {fresh_deployment_id} for {astro_deployment_name}"
            )

            # validate proper environment variables

            # Set up API connection details
            api_url = "https://" + run_and_validate_subprocess(
                [
                    "astro",
                    "deployment",
                    "inspect",
                    "--deployment-name",
                    astro_deployment_name,
                    "--key",
                    "metadata.airflow_api_url",
                ],
                "getting Astro deployment API URL",
                return_output=True,
            )
            base_url = api_url[: api_url.find("/api/v1")]
            api_token = os.getenv("ASTRO_API_TOKEN")

            # Update manager with connection details
            manager.host = base_url
            manager.api_token = api_token
            manager.api_url = api_url
            manager.api_headers = {
                "Authorization": f"Bearer {api_token}",
                "Cache-Control": "no-cache",
            }

            # Validate Airflow is ready
            manager.wait_for_airflow_to_be_ready()

            creation_end = time.time()
            print(
                f"Worker {os.getpid()}: Airflow resource creation took {creation_end - start_time:.2f}s"
            )

            return manager

        except Exception as e:
            print(f"Worker {os.getpid()}: Error in Airflow fixture: {e}")
            # Cleanup on error
            manager.cleanup_resource(test_dir)
            raise e from e

    @traced(name="_get_deployment_id_by_name")
    @retry_astro_command(max_retries=3)
    def _get_deployment_id_by_name(self, deployment_name: str) -> Optional[str]:
        """
        Helper function to get deployment ID by deployment name from Astronomer.

        Args:
            deployment_name: The name of the deployment

        Returns:
            The deployment ID if found, None otherwise
        """
        try:
            deployment_id = run_and_validate_subprocess(
                [
                    "astro",
                    "deployment",
                    "inspect",
                    "--deployment-name",
                    deployment_name,
                    "--key",
                    "metadata.deployment_id",
                ],
                f"getting deployment ID for {deployment_name}",
                return_output=True,
            )

            if deployment_id and deployment_id.strip():
                deployment_id = deployment_id.strip()
                print(
                    f"Worker {os.getpid()}: Found deployment ID {deployment_id} for deployment {deployment_name}"
                )
                return deployment_id
            else:
                print(
                    f"Worker {os.getpid()}: No deployment ID returned for deployment {deployment_name}"
                )
                return None

        except Exception as e:
            print(
                f"Worker {os.getpid()}: Error fetching deployment ID for {deployment_name}: {e}"
            )
            return None

    @traced(name="_create_variables_in_airflow_deployment")
    @retry_astro_command(max_retries=3)
    def _create_variables_in_airflow_deployment(self, deployment_name: str) -> None:
        """
        Helper method to create variables in the Airflow deployment.

        Args:
            deployment_name: The name of the Airflow deployment
        """
        username = os.getenv("AIRFLOW_USERNAME", "airflow")
        password = os.getenv("AIRFLOW_PASSWORD", "airflow")

        user_creation_commands = [
            [
                "astro",
                "deployment",
                "variable",
                "create",
                "_AIRFLOW_WWW_USER_CREATE=true",
                "--deployment-name",
                deployment_name,
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                f"_AIRFLOW_WWW_USER_USERNAME={username}",
                "--deployment-name",
                deployment_name,
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                f"_AIRFLOW_WWW_USER_PASSWORD={password}",
                "--deployment-name",
                deployment_name,
                "-s",
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                "AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth",
                "--deployment-name",
                deployment_name,
            ],
        ]

        if slack_app_url := os.getenv("SLACK_APP_URL"):
            user_creation_commands.append(
                [
                    "astro",
                    "deployment",
                    "variable",
                    "create",
                    f"SLACK_APP_URL={slack_app_url}",
                    "--deployment-name",
                    deployment_name,
                    "-s",
                ]
            )

        for command in user_creation_commands:
            variable_name = command[4].split("=")[0]
            try:
                _ = run_and_validate_subprocess(
                    command, f"creating/updating variable {variable_name}"
                )
                print(
                    f"Worker {os.getpid()}: Successfully created/updated variable {variable_name}"
                )
            except subprocess.CalledProcessError as e:
                error_output = e.stderr.decode("utf-8") if e.stderr else str(e)
                if (
                    "already exists" in error_output.lower()
                    or "conflict" in error_output.lower()
                ):
                    print(
                        f"Worker {os.getpid()}: Variable {variable_name} already exists, skipping creation"
                    )
                else:
                    print(
                        f"Worker {os.getpid()}: Error creating variable {variable_name}: {error_output}"
                    )

                    if variable_name in [
                        "_AIRFLOW_WWW_USER_CREATE",
                        "AIRFLOW__API__AUTH_BACKENDS",
                    ]:
                        try:
                            update_command = command.copy()
                            update_command[3] = "update"
                            _ = run_and_validate_subprocess(
                                update_command, f"updating variable {variable_name}"
                            )
                            print(
                                f"Worker {os.getpid()}: Successfully updated variable {variable_name}"
                            )
                        except Exception as update_error:
                            print(
                                f"Worker {os.getpid()}: Failed to update variable {variable_name}: {update_error}"
                            )

    @traced(name="cleanup_airflow_resource")
    @retry_astro_command(max_retries=3)
    def cleanup_resource(self, test_dir: Optional[Path] = None):
        """
        Cleans up an Airflow resource, including temp directory and deployments.

        Args:
            test_dir: The path to the test directory
        """
        if test_dir and test_dir.exists():
            try:
                shutil.rmtree(test_dir)
                print(
                    f"Worker {os.getpid()}: Removed {self.resource_id}'s temp directory: {test_dir}"
                )
            except Exception as e:
                print(f"Worker {os.getpid()}: Error removing temp directory: {e}")

        print(f"Worker {os.getpid()}: Cleaning up Airflow resource {self.resource_id}")
        try:
            for resource_info in reversed(self.test_resources):
                deployment_name, cache_manager = resource_info

                print(
                    f"Worker {os.getpid()}: Deleting deployment created by test: {deployment_name}"
                )
                _ = run_and_validate_subprocess(
                    ["astro", "deployment", "delete", "-n", deployment_name, "-f"],
                    "delete Astronomer deployment",
                    check=True,
                )
                self._create_deployment_in_astronomer(deployment_name, wait=False)
                self._hibernate_deployment(deployment_name)

                new_id = self._get_deployment_id_by_name(deployment_name)
                cache_manager.release_astronomer_deployment(
                    deployment_name, os.getpid(), new_id
                )
                print(
                    f"Worker {os.getpid()}: Deployment {deployment_name} - hibernated successfully."
                )

            print(
                f"Worker {os.getpid()}: Airflow resource {self.resource_id} cleaned up successfully"
            )
        except Exception as e:
            print(f"Worker {os.getpid()}: Error cleaning up Airflow resource: {e}")
