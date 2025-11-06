"""
GitHub fixture using DEBenchFixture pattern.
Handles GitHub repository operations via GitHubManager.
"""

import os
import time
import uuid
from typing import Dict, Any, Optional, Union
from typing_extensions import TypedDict
from pathlib import Path

from Fixtures.base_fixture import DEBenchFixture
from Fixtures.GitHub.github_manager import GitHubManager


class GitHubResourceConfig(TypedDict):
    resource_id: str
    create_branch: Optional[bool]
    build_info: Optional[Dict[str, str]]
    state_archive_path: Optional[Union[str, Path]] = None


class GitHubResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    github_manager: GitHubManager
    repo_info: Dict[str, Any]
    branch_name: str
    access_token: str
    repo_url: str


class GitHubFixture(
    DEBenchFixture[GitHubResourceConfig, GitHubResourceData, Dict[str, Any]]
):
    """
    GitHub fixture implementation using DEBenchFixture pattern.

    Provides GitHub repository management via GitHubManager.
    Handles branch creation, folder clearing, and cleanup operations.
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """GitHub doesn't require session-level setup"""
        return False

    def session_setup(
        self, session_config: Optional[GitHubResourceConfig] = None
    ) -> Dict[str, Any]:
        """No session setup needed for GitHub"""
        return {}

    def session_teardown(self, session_data: Optional[Dict[str, Any]] = None) -> None:
        """No session teardown needed for GitHub"""
        pass

    def test_setup(
        self, resource_config: Optional[GitHubResourceConfig] = None
    ) -> GitHubResourceData:
        """Set up GitHub manager and initialize repository state"""
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        print(f"🐙 Setting up GitHub resource: {resource_id}")

        # Verify required environment variables
        required_envars = [
            "AIRFLOW_GITHUB_TOKEN",
            "AIRFLOW_REPO",
        ]

        if missing_envars := [
            envar for envar in required_envars if not os.getenv(envar)
        ]:
            raise ValueError(
                f"Missing required environment variables: {missing_envars}"
            )

        creation_start = time.time()

        try:
            # Get GitHub credentials from environment
            access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
            repo_url = os.getenv("AIRFLOW_REPO")

            # Create GitHub manager
            create_branch = config.get("create_branch", True)
            build_info = config.get("build_info")

            state_archive_path = config.get("state_archive_path")

            github_manager = GitHubManager(
                access_token=access_token,
                repo_url=repo_url,
                test_name=resource_id,
                create_branch=create_branch,
                build_info=build_info,
                state_archive_path=state_archive_path,
            )

            # Clear the main dags folder as part of setup
            try:
                github_manager.clear_folder("dags", keep_file_names=[".gitkeep"])
                print(f"✅ Cleared dags folder for {resource_id}")
            except Exception as e:
                print(f"⚠️ Warning: Could not clear main dags folder: {e}")

        except Exception as e:
            print(f"❌ Failed to create GitHub resource {resource_id}: {e}")
            raise

        creation_end = time.time()
        creation_duration = creation_end - creation_start

        # Create resource data
        resource_data = GitHubResourceData(
            resource_id=resource_id,
            type="github_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"GitHub resource for {resource_id}",
            status="active",
            github_manager=github_manager,
            repo_info=github_manager.get_repo_info(),
            branch_name=github_manager.branch_name,
            access_token=access_token,
            repo_url=repo_url,
        )

        # Store for later access during validation
        self._resource_data = resource_data

        print(f"✅ GitHub resource {resource_id} ready! ({creation_duration:.2f}s)")
        return resource_data

    def test_teardown(self, resource_data: GitHubResourceData) -> None:
        """Clean up GitHub resources"""
        resource_id = resource_data["resource_id"]
        github_manager = resource_data["github_manager"]

        print(f"🧹 Cleaning up GitHub resource: {resource_id}")

        try:
            # Perform cleanup operations
            self._cleanup_github_resource(github_manager)

            print(f"✅ GitHub resource {resource_id} cleaned up successfully")

        except Exception as e:
            print(f"❌ Error cleaning up GitHub resource {resource_id}: {e}")

    def _cleanup_github_resource(self, github_manager: GitHubManager) -> None:
        """
        Clean up GitHub resource using the GitHubManager.

        Args:
            github_manager: The GitHubManager instance to clean up
        """
        try:
            # Reset repository state (clear dags folder)
            github_manager.reset_repo_state("dags")
            print(f"✅ Reset repository state for dags folder")
        except Exception as e:
            print(f"⚠️ Error resetting repo state: {e}")

        try:
            # Clean up requirements
            github_manager.cleanup_requirements()
            print(f"✅ Cleaned up requirements")
        except Exception as e:
            print(f"⚠️ Error cleaning up requirements: {e}")

        try:
            # Delete the test branch
            github_manager.delete_branch(github_manager.branch_name)
            print(f"✅ Deleted branch: {github_manager.branch_name}")
        except Exception as e:
            print(f"⚠️ Error deleting branch: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "github_resource"

    @classmethod
    def get_default_config(cls) -> GitHubResourceConfig:
        """Return default configuration for GitHub resources"""
        timestamp = int(time.time())
        test_uuid = uuid.uuid4().hex[:8]

        return GitHubResourceConfig(
            resource_id=f"github_test_{timestamp}_{test_uuid}",
            create_branch=True,
            build_info=None,
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create GitHub config section using the fixture's resource data.

        Returns:
            Dictionary containing the github service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "GitHub resource data not available - ensure test_setup was called"
            )

        # Extract connection details from resource data
        return {
            "github": {
                "token": resource_data.get("github_token", os.getenv("AIRFLOW_GITHUB_TOKEN")),
                "repo": resource_data.get("repo_full_name"),
                "branch": resource_data.get("branch_name"),
                "default_branch": resource_data.get("default_branch", "main"),
            }
        }
