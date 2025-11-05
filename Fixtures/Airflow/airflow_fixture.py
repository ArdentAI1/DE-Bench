"""
AirflowFixture using DEBenchFixture pattern with session-level support.
Handles expensive Airflow deployment operations at the session level.
"""

import os
import time
import tempfile
import uuid
from typing import Dict, Any, Optional, List
from typing_extensions import TypedDict
from pathlib import Path

from Fixtures.base_fixture import DEBenchFixture
from Fixtures.Databricks.cache_manager import CacheManager
from Fixtures.Airflow.Airflow_class import AirflowManager

from braintrust import traced


class AirflowResourceConfig(TypedDict):
    resource_id: str
    deployment_name: Optional[str]
    runtime_version: Optional[str]
    scheduler_size: Optional[str]


class AirflowResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    deployment_id: str
    deployment_name: str
    secret_suffix: str  # Suffix for test-specific GitHub secrets
    base_url: str
    api_url: str
    api_token: str
    api_headers: Dict[str, str]
    username: str
    password: str
    airflow_instance: Any
    test_dir: Path


class AirflowSessionData(TypedDict):
    cache_manager: CacheManager
    astro_logged_in: bool
    available_deployments: List[Dict[str, str]]


class AirflowFixture(
    DEBenchFixture[AirflowResourceConfig, AirflowResourceData, AirflowSessionData]
):
    """
    Airflow fixture implementation with session-level deployment pool management.

    Session-level: Astro login, cache manager, deployment pool
    Per-test: Allocate deployment, configure for specific test, cleanup
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """Airflow requires expensive session-level setup"""
        return True

    def session_setup(
        self, session_config: Optional[AirflowResourceConfig] = None
    ) -> AirflowSessionData:
        """
        Set up shared Airflow session resources:
        - Astro CLI login
        - Cache manager initialization
        - Pre-create hibernated deployment pool
        """
        print("🌐 Setting up Airflow session-level resources...")

        # Verify required environment variables
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
            raise ValueError(
                f"Missing required environment variables: {missing_envars}"
            )

        # switch to the correct workspace
        # self._switch_to_correct_workspace()  # COMMENTED OUT - causing concurrent CLI conflicts

        # make sure ASTRO_API_TOKEN is set
        if not os.getenv("ASTRO_API_TOKEN"):
            raise ValueError("ASTRO_API_TOKEN must be set")

        # 1. Ensure Astro login
        print("🔐 Ensuring Astro CLI login...")
        temp_manager = AirflowManager()
        temp_manager._ensure_astro_login()

        # 2. Initialize cache manager
        print("💾 Initializing deployment cache manager...")
        from Fixtures.Airflow.airflow_resources import _ensure_cache_manager_initialized

        cache_manager = _ensure_cache_manager_initialized()

        # 3. Get available deployments
        available_deployments = cache_manager.get_all_astronomer_deployments()

        print(
            f"✅ Airflow session setup complete! Found {len(available_deployments)} deployments in cache"
        )

        return AirflowSessionData(
            cache_manager=cache_manager,
            astro_logged_in=True,
            available_deployments=available_deployments,
        )

    def session_teardown(
        self, session_data: Optional[AirflowSessionData] = None
    ) -> None:
        """Clean up session-level Airflow resources"""
        if not session_data:
            return

        print("🧹 Cleaning up Airflow session-level resources...")

        # The cache manager and deployments will be cleaned up naturally
        # since they're managed by the Astronomer platform
        print("✅ Airflow session cleanup complete")

    def _test_setup(
        self, resource_config: Optional[AirflowResourceConfig] = None
    ) -> AirflowResourceData:
        """
        Set up the resource and ensure _resource_data is set.
        """
        from braintrust import traced

        @traced(name=f"{self.get_resource_type()}.test_setup")
        def inner_test_setup(
            resource_config: Optional[AirflowResourceConfig] = None,
        ) -> AirflowResourceData:
            return self.test_setup(resource_config)

        resource_data = inner_test_setup(resource_config)
        self._resource_data = resource_data
        return resource_data

    def test_setup(
        self, resource_config: Optional[AirflowResourceConfig] = None
    ) -> AirflowResourceData:
        """
        Set up individual Airflow resource for a test using the unified AirflowManager.
        """
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        print(f"🚀 Setting up Airflow resource: {resource_id}")

        creation_start = time.time()

        # Get session data
        session_data = self.session_data
        if not session_data:
            raise RuntimeError(
                "Session data not available - session setup may have failed"
            )

        cache_manager = session_data["cache_manager"]

        try:
            # Create mock request object for the AirflowManager.create_resource method
            class MockRequest:
                class MockNode:
                    def __init__(self, name):
                        self.name = name

                def __init__(self, name):
                    self.node = self.MockNode(name)

            mock_request = MockRequest(f"test_{resource_id}")

            # Build template for AirflowManager
            build_template = {"resource_id": resource_id}

            # Use the unified AirflowManager to create the resource
            airflow_manager = AirflowManager.create_resource(
                request=mock_request,
                build_template=build_template,
                shared_cache_manager=cache_manager,
            )

            creation_end = time.time()
            print(
                f"✅ Airflow resource creation took {creation_end - creation_start:.2f}s"
            )

            # Create resource data for backward compatibility
            resource_data = AirflowResourceData(
                resource_id=resource_id,
                type="airflow_resource",
                creation_time=creation_start,
                creation_duration=creation_end - creation_start,
                description=f"Airflow resource for {resource_id}",
                status="active",
                deployment_id=airflow_manager.deployment_id,
                deployment_name=airflow_manager.deployment_name,
                secret_suffix=airflow_manager.secret_suffix,  # Include secret suffix for parallel execution
                base_url=airflow_manager.host,
                api_url=airflow_manager.api_url,
                api_token=airflow_manager.api_token,
                api_headers=airflow_manager.api_headers,
                username=os.getenv("AIRFLOW_USERNAME", "airflow"),
                password=os.getenv("AIRFLOW_PASSWORD", "airflow"),
                airflow_instance=airflow_manager,  # Use the manager as the instance
                test_dir=airflow_manager.airflow_dir,
            )

            # Store the manager for later use
            self._airflow_manager = airflow_manager

            print(f"✅ Airflow resource {resource_id} ready!")
            return resource_data

        except Exception as e:
            print(f"❌ Failed to setup Airflow resource {resource_id}: {e}")
            raise

    def _test_teardown(self) -> None:
        """
        Clean up the resource and ensure proper teardown.
        """
        from braintrust import traced

        @traced(name=f"{self.get_resource_type()}.test_teardown")
        def inner_test_teardown(resource_data: AirflowResourceData) -> None:
            return self.test_teardown(resource_data)

        # Check if _resource_data exists before trying to use it
        if hasattr(self, "_resource_data") and self._resource_data:
            inner_test_teardown(self._resource_data)
        else:
            print(
                f"⚠️ No _resource_data found for {self.get_resource_type()}, skipping teardown"
            )

    def test_teardown(self, resource_data: AirflowResourceData) -> None:
        """Clean up individual Airflow resource using unified AirflowManager"""
        resource_id = resource_data["resource_id"]
        test_dir = resource_data["test_dir"]

        print(f"🧹 Cleaning up Airflow resource: {resource_id}")
        # Use the stored AirflowManager instance for cleanup
        if hasattr(self, "_airflow_manager") and self._airflow_manager:
            self._airflow_manager.cleanup_resource(test_dir)
            print(f"✅ Airflow resource {resource_id} cleaned up using AirflowManager")
        else:
            print(f"⚠️ No AirflowManager found for {resource_id}, skipping cleanup")
            raise Exception(f"No AirflowManager found for {resource_id}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "airflow_resource"

    @classmethod
    def get_default_config(cls) -> AirflowResourceConfig:
        """Return default configuration for Airflow resources"""
        timestamp = int(time.time())
        uuid_suffix = uuid.uuid4().hex[:8]

        return AirflowResourceConfig(
            resource_id=f"airflow_test_{timestamp}_{uuid_suffix}",
            deployment_name=None,  # Will be generated from resource_id
            runtime_version=os.getenv("ASTRO_RUNTIME_VERSION", "13.1.0"),
            scheduler_size="small",
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create Airflow config section using the fixture's resource data.

        Returns:
            Dictionary containing the airflow service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "Airflow resource data not available - ensure test_setup was called"
            )

        print(f"resource_data: {resource_data}")

        # Extract connection details from resource data
        github_token = resource_data.get(
            "github_token", os.getenv("AIRFLOW_GITHUB_TOKEN")
        )
        repo = resource_data.get("repo", os.getenv("AIRFLOW_REPO"))
        dag_path = resource_data.get("dag_path", os.getenv("AIRFLOW_DAG_PATH"))
        requirements_path = resource_data.get(
            "requirements_path", os.getenv("AIRFLOW_REQUIREMENTS_PATH")
        )
        api_token = resource_data.get("api_token", os.getenv("AIRFLOW_API_TOKEN"))
        # Use the base URL from resource data if available
        base_url = resource_data.get("base_url", "http://localhost:8080")

        return {
            "airflow": {
                "github_token": github_token,
                "repo": repo,
                "dag_path": dag_path,
                "api_token": api_token,
                "requirements_path": requirements_path,
                "host": base_url,
                "username": "airflow",  # Standard Airflow username
                "password": "airflow",  # Standard Airflow password
            }
        }


# Note: No global instance - each test creates its own AirflowFixture
# with custom resource_id in get_fixtures()
