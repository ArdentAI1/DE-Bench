"""
This module provides a pytest fixture for managing GitHub operations in Airflow tests.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Union
from .github_manager import GitHubManager

import github
from github import Github

import pytest


@pytest.fixture(scope="function")
def github_resource(request):
    """
    A function-scoped fixture that provides a GitHub manager for test operations.
    Each test gets its own GitHub manager instance.
    """
    # Verify required environment variables
    required_envars = [
        "AIRFLOW_GITHUB_TOKEN",
        "AIRFLOW_REPO",
    ]
    
    if missing_envars := [envar for envar in required_envars if not os.getenv(envar)]:
        raise ValueError(f"The following envars are not set: {missing_envars}")
    
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting github_resource for {test_name}")
    
    # Create GitHub manager
    access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
    repo_url = os.getenv("AIRFLOW_REPO")
    
    github_manager = GitHubManager(access_token, repo_url)
    
    try:
        # Setup initial repository state
        github_manager.clear_folder("dags", keep_file_names=[".gitkeep"])
        
        creation_end = time.time()
        print(f"Worker {os.getpid()}: GitHub resource creation took {creation_end - start_time:.2f}s")
        
        # Create resource data
        resource_data = {
            "resource_id": "github_resource",
            "type": "github_resource",
            "test_name": test_name,
            "creation_time": time.time(),
            "worker_pid": os.getpid(),
            "creation_duration": creation_end - start_time,
            "description": f"A GitHub resource for {test_name}",
            "status": "active",
            "github_manager": github_manager,
            "repo_info": github_manager.get_repo_info(),
        }
        
        print(f"Worker {os.getpid()}: Created GitHub resource")
        
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: GitHub fixture setup took {fixture_end_time - start_time:.2f}s total")
        
        yield resource_data
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in GitHub fixture: {e}")
        raise e from e
    finally:
        print(f"Worker {os.getpid()}: Cleaning up GitHub resource")
        cleanup_github_resource(github_manager)


def _check_and_update_gh_secrets(deployment_id: str, deployment_name: str, astro_access_token: str) -> None:
    """
    Checks if the GitHub secrets exists, deletes them if they do, and creates new ones with the given
        deployment ID and name.

    :param str deployment_id: The ID of the deployment.
    :param str deployment_name: The name of the deployment.
    :rtype: None
    """
    # TODO: update this to be able to update different secrets for different tests and use the github manager to do this
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


def cleanup_github_resource(
    github_manager: GitHubManager,
):
    """
    Cleans up a GitHub resource, including the temp directory and the created resources in GitHub.

    :param test_name: The name of the test.
    :param resource_id: The ID of the resource.
    :param created_resources: The list of created resources.
    :param test_dir: The path to the test directory.
    """
    github_manager.reset_repo_state("dags")
    github_manager.cleanup_requirements()
