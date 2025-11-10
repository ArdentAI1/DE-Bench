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
    use_kubernetes: Optional[bool]  # Use Kubernetes instead of Astro
    container_image: Optional[str]  # Container image for Kubernetes deployment
    kubernetes_namespace: Optional[str]  # Kubernetes namespace
    use_ecs: Optional[bool]  # Use ECS instead of Astro
    ecs_namespace: Optional[str]  # ECS namespace
    enable_load_balancer: Optional[bool]  # Enable load balancer for ECS


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
    # Kubernetes-specific fields
    k8s_manager: Optional[Any]  # KubernetesManifestManager instance
    k8s_namespace: Optional[str]  # Kubernetes namespace
    external_ip: Optional[str]  # External IP from LoadBalancer
    # ECS-specific fields
    ecs_manager: Optional[Any]  # ECSManifestManager instance
    ecs_namespace: Optional[str]  # ECS namespace
    ecs_endpoint: Optional[str]  # ECS endpoint (DNS or public IP)


class AirflowSessionData(TypedDict):
    cache_manager: Optional[CacheManager]
    astro_logged_in: Optional[bool] = False
    available_deployments: Optional[List[Dict[str, str]]] = None
    use_kubernetes: Optional[bool] = False
    use_ecs: Optional[bool] = False


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

        # Determine deployment mode
        use_kubernetes = (session_config and session_config.get("use_kubernetes", False)) or os.getenv("USE_KUBERNETES_AIRFLOW", "false").lower() == "true"
        use_ecs = (session_config and session_config.get("use_ecs", False)) or os.getenv("USE_ECS_AIRFLOW", "false").lower() == "true"

        # Validate deployment mode - only one can be active
        if use_kubernetes and use_ecs:
            raise ValueError("Cannot use both Kubernetes and ECS deployment modes simultaneously")

        if use_kubernetes:
            required_envars = [
                "DE_BENCH_AKS_RESOURCE_GROUP",
                "DE_BENCH_AKS_CLUSTER_NAME",
                "DE_BENCH_AKS_IMAGE_NAME",
            ]
        elif use_ecs:
            required_envars = [
                "DE_BENCH_ECS_CLUSTER_NAME",
                "AWS_REGION",
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
            raise ValueError(
                f"Missing required environment variables: {missing_envars}"
            )

        if use_kubernetes:
            # Skip CacheManager initialization for Kubernetes - it's not needed
            # Kubernetes deployments don't use Astronomer cache
            print("✅ Kubernetes mode detected - skipping CacheManager and Astro setup")
            return AirflowSessionData(
                cache_manager=None,
                astro_logged_in=False,
                available_deployments=None,
                use_kubernetes=use_kubernetes,
                use_ecs=False,
            )

        if use_ecs:
            # Skip CacheManager initialization for ECS - it's not needed
            # ECS deployments don't use Astronomer cache
            print("✅ ECS mode detected - skipping CacheManager and Astro setup")
            return AirflowSessionData(
                cache_manager=None,
                astro_logged_in=False,
                available_deployments=None,
                use_kubernetes=False,
                use_ecs=use_ecs,
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
        Set up individual Airflow resource for a test.
        Supports both Astro (via AirflowManager) and Kubernetes (via KubernetesManifestManager).
        """
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        use_kubernetes = config.get("use_kubernetes", False)
        use_ecs = config.get("use_ecs", False)

        # Determine deployment mode
        if use_kubernetes and use_ecs:
            raise ValueError("Cannot use both Kubernetes and ECS deployment modes simultaneously")

        deployment_mode = "Kubernetes" if use_kubernetes else ("ECS" if use_ecs else "Astro")
        print(f"🚀 Setting up Airflow resource: {resource_id}")
        print(f"📦 Deployment mode: {deployment_mode}")

        creation_start = time.time()

        try:
            if use_kubernetes:
                # Kubernetes deployment path
                return self._setup_kubernetes_airflow(config, resource_id, creation_start)
            elif use_ecs:
                # ECS deployment path
                return self._setup_ecs_airflow(config, resource_id, creation_start)
            else:
                # Astro deployment path (existing logic)
                return self._setup_astro_airflow(config, resource_id, creation_start)

        except Exception as e:
            print(f"❌ Failed to setup Airflow resource {resource_id}: {e}")
            raise

    def _setup_astro_airflow(
        self, config: AirflowResourceConfig, resource_id: str, creation_start: float
    ) -> AirflowResourceData:
        """Set up Airflow using Astro (existing logic)"""
        # Get session data
        session_data = self.session_data
        if not session_data:
            raise RuntimeError(
                "Session data not available - session setup may have failed"
            )

        # Ensure we're not in Kubernetes mode
        if session_data.get("use_kubernetes", False):
            raise RuntimeError(
                "Cannot use Astro setup when Kubernetes mode is enabled"
            )

        cache_manager = session_data["cache_manager"]
        if cache_manager is None:
            raise RuntimeError(
                "CacheManager is required for Astro deployments but was not initialized"
            )

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
            secret_suffix=airflow_manager.secret_suffix,
            base_url=airflow_manager.host,
            api_url=airflow_manager.api_url,
            api_token=airflow_manager.api_token,
            api_headers=airflow_manager.api_headers,
            username=os.getenv("AIRFLOW_USERNAME", "airflow"),
            password=os.getenv("AIRFLOW_PASSWORD", "airflow"),
            airflow_instance=airflow_manager,
            test_dir=airflow_manager.airflow_dir,
            k8s_manager=None,
            k8s_namespace=None,
            external_ip=None,
        )

        # Store the manager for later use
        self._airflow_manager = airflow_manager

        print(f"✅ Airflow resource {resource_id} ready!")
        return resource_data

    def _setup_kubernetes_airflow(
        self, config: AirflowResourceConfig, resource_id: str, creation_start: float
    ) -> AirflowResourceData:
        """Set up Airflow using Kubernetes via ManifestManager"""
        from Environment.Kubernetes.ManifestManager import KubernetesManifestManager

        # Get container image from config or environment
        container_image = config.get("container_image") or os.getenv("AIRFLOW_CONTAINER_IMAGE")
        if not container_image:
            raise ValueError("AIRFLOW_CONTAINER_IMAGE environment variable is not set and the container_image is not set in the config.")

        # Get or generate namespace
        namespace = config.get("kubernetes_namespace") or f"airflow-{resource_id}".lower()[:63]

        print(f"🐳 Container image: {container_image}")
        print(f"📦 Kubernetes namespace: {namespace}")

        # Initialize Kubernetes manifest manager
        k8s_manager = KubernetesManifestManager(provider="AZURE")
        # Store k8s_manager and namespace immediately for cleanup in case of failure
        self._k8s_manager = k8s_manager
        self._k8s_namespace = namespace
        
        total_start_time = time.time()
        start_time = time.time()
        # Deploy Airflow to Kubernetes
        print(f"🚀 Deploying Airflow to Kubernetes...")
        try:
            k8s_manager.generate_and_apply_manifest(
                namespace=namespace,
                container=container_image
            )
            print(f"🚀 Kubernetes manifest deployment took {time.time() - start_time:.2f}s")

            # Get the external IP from the LoadBalancer service
            start_time = time.time()
            print(f"🔍 Waiting for external IP from LoadBalancer...")
            external_ip = k8s_manager.get_service_external_ip(namespace=namespace)
            print(f"🚀 External IP retrieval took {time.time() - start_time:.2f}s")
            if not external_ip:
                raise RuntimeError(
                    f"Failed to get external IP for namespace {namespace} after deployment"
                )
        except Exception as e:
            # Clean up namespace on failure
            print(f"❌ Error during Kubernetes setup: {e}")
            print(f"🧹 Cleaning up namespace {namespace} due to setup failure...")
            try:
                if k8s_manager:
                    k8s_manager.delete_namespace(namespace=namespace)
                    print(f"✅ Cleaned up namespace {namespace}")
            except Exception as cleanup_error:
                print(f"⚠️ Error during cleanup: {cleanup_error}")
            raise

        print(f"✅ External IP obtained: {external_ip}")

        # Verify pod health
        start_time = time.time()
        print(f"🏥 Verifying Airflow pod health...")
        if not k8s_manager.verify_pod_health(external_ip=external_ip, port=8080):
            print(f"⚠️ Pod health check failed, but continuing...")
        print(f"🚀 Pod health check took {time.time() - start_time:.2f}s")
        # Construct Airflow URLs using external IP
        base_url = f"http://{external_ip}:8080"
        api_url = f"{base_url}/api/v1"

        # Create a temporary directory for test artifacts
        test_dir = Path(tempfile.mkdtemp(prefix=f"airflow_k8s_{resource_id}_"))

        # Create resource data
        # NOTE: airflow_instance should be an AirflowManager or compatible API client
        # that can interact with the Kubernetes-deployed Airflow instance.
        # We create an AirflowManager instance pointing to the Kubernetes deployment URL.
        start_time = time.time()
        try:
            # Create an AirflowManager instance pointing to the K8s deployment
            # Skip Astro validation since we're using Kubernetes
            airflow_api_client = AirflowManager(
                host=base_url,
                api_url=api_url,
                api_token="",  # Not used for Kubernetes mode
                resource_id=resource_id,
                require_astro=False,  # Skip Astro validation for Kubernetes mode
            )
            print(f"✅ Created AirflowManager instance for Kubernetes deployment")
            
            # Wait for Airflow to be fully ready
            print(f"⏳ Waiting for Airflow to be ready...")
            if not airflow_api_client.wait_for_airflow_to_be_ready():
                print(f"⚠️ Airflow readiness check failed, but continuing...")
            else:
                print(f"✅ Airflow is ready!")
        except Exception as e:
            raise RuntimeError(
                f"Failed to create AirflowManager instance for Kubernetes deployment: {e}"
            ) from e
        print(f"🚀 AirflowManager instance creation took {time.time() - start_time:.2f}s")
        print(f"🚀 Total Kubernetes Airflow deployment took {time.time() - total_start_time:.2f}s")
        creation_end = time.time()
        print(
            f"✅ Kubernetes Airflow deployment took {creation_end - creation_start:.2f}s"
        )

        resource_data = AirflowResourceData(
            resource_id=resource_id,
            type="airflow_resource",
            creation_time=creation_start,
            creation_duration=creation_end - creation_start,
            description=f"Kubernetes Airflow resource for {resource_id}",
            status="active",
            deployment_id=namespace,  # Use namespace as deployment ID
            deployment_name=namespace,
            secret_suffix=resource_id,  # Use resource_id as secret suffix
            base_url=base_url,
            api_url=api_url,
            api_token="",
            api_headers=airflow_api_client.api_headers,
            username=os.getenv("AIRFLOW_USERNAME", "admin"),
            password=os.getenv("AIRFLOW_PASSWORD", "admin"),
            airflow_instance=airflow_api_client,
            test_dir=test_dir,
            k8s_manager=k8s_manager,
            k8s_namespace=namespace,
            external_ip=external_ip,
        )

        # Store the k8s manager for later use
        self._k8s_manager = k8s_manager
        self._k8s_namespace = namespace

        print(f"✅ Kubernetes Airflow resource {resource_id} ready at {base_url}")
        return resource_data

    def _setup_ecs_airflow(
        self, config: AirflowResourceConfig, resource_id: str, creation_start: float
    ) -> AirflowResourceData:
        """Set up Airflow using AWS ECS via ECSManifestManager"""
        from Environment.ECS.ManifestManager import ECSManifestManager

        # Get container image from config or environment
        container_image = config.get("container_image") or os.getenv("AIRFLOW_CONTAINER_IMAGE")
        if not container_image:
            raise ValueError("AIRFLOW_CONTAINER_IMAGE environment variable is not set and the container_image is not set in the config.")

        # Get or generate namespace
        namespace = config.get("ecs_namespace") or f"airflow-{resource_id}".lower()[:63]

        # Get load balancer preference
        enable_load_balancer = config.get("enable_load_balancer", True)

        print(f"🐳 Container image: {container_image}")
        print(f"📦 ECS namespace: {namespace}")
        print(f"⚖️ Load balancer: {'enabled' if enable_load_balancer else 'disabled'}")

        # Initialize ECS manifest manager
        ecs_manager = ECSManifestManager(provider="AWS")
        # Store ecs_manager and namespace immediately for cleanup in case of failure
        self._ecs_manager = ecs_manager
        self._ecs_namespace = namespace

        total_start_time = time.time()
        start_time = time.time()

        # Deploy Airflow to ECS
        print(f"🚀 Deploying Airflow to ECS...")
        try:
            deployment_result = ecs_manager.generate_and_deploy(
                namespace=namespace,
                container_image=container_image,
                use_service=True,  # Always use service for long-running Airflow
                enable_load_balancer=enable_load_balancer
            )
            print(f"🚀 ECS deployment took {time.time() - start_time:.2f}s")

            # Get the endpoint from the deployment result
            start_time = time.time()
            print(f"🔍 Getting ECS endpoint...")

            if enable_load_balancer:
                # Use load balancer DNS
                endpoint = deployment_result.get("dns_name")
                if not endpoint:
                    raise RuntimeError(f"Failed to get load balancer DNS for namespace {namespace}")

                # Get port from deployment result (NLB uses 8080, ALB uses 80)
                port = deployment_result.get("port", 8080)
                base_url = deployment_result.get("base_url", f"http://{endpoint}:{port}")
            else:
                # Use task public IP - need to wait for service to have running tasks
                print(f"⏳ Waiting for ECS service to have running tasks...")
                max_wait = 300  # 5 minutes
                wait_start = time.time()
                endpoint = None

                while time.time() - wait_start < max_wait:
                    endpoint = ecs_manager.get_service_external_ip(namespace=namespace)
                    if endpoint:
                        break
                    print(f"   Still waiting for task IP... ({int(time.time() - wait_start)}s elapsed)")
                    time.sleep(10)

                if not endpoint:
                    raise RuntimeError(f"Failed to get public IP for namespace {namespace} after {max_wait}s")

                base_url = f"http://{endpoint}:8080"
                port = 8080

            print(f"🚀 Endpoint retrieval took {time.time() - start_time:.2f}s")
            print(f"✅ Endpoint obtained: {endpoint}")

        except Exception as e:
            # Clean up ECS resources on failure
            print(f"❌ Error during ECS setup: {e}")
            print(f"🧹 Cleaning up namespace {namespace} due to setup failure...")
            try:
                if ecs_manager:
                    ecs_manager.cleanup_deployment(namespace=namespace, cleanup_infrastructure=False)
                    print(f"✅ Cleaned up namespace {namespace}")
            except Exception as cleanup_error:
                print(f"⚠️ Error during cleanup: {cleanup_error}")
            raise

        # Verify service health
        start_time = time.time()
        print(f"🏥 Verifying Airflow service health...")
        if not ecs_manager.verify_pod_health(endpoint=endpoint, port=port):
            print(f"⚠️ Service health check failed, but continuing...")
        print(f"🚀 Service health check took {time.time() - start_time:.2f}s")

        # Construct Airflow URLs
        api_url = f"{base_url}/api/v1"

        # Create a temporary directory for test artifacts
        test_dir = Path(tempfile.mkdtemp(prefix=f"airflow_ecs_{resource_id}_"))

        # Create resource data
        start_time = time.time()
        try:
            # Create an AirflowManager instance pointing to the ECS deployment
            # Skip Astro validation since we're using ECS
            airflow_api_client = AirflowManager(
                host=base_url,
                api_url=api_url,
                api_token="",  # Not used for ECS mode
                resource_id=resource_id,
                require_astro=False,  # Skip Astro validation for ECS mode
            )
            print(f"✅ Created AirflowManager instance for ECS deployment")

            # Wait for Airflow to be fully ready
            print(f"⏳ Waiting for Airflow to be ready...")
            if not airflow_api_client.wait_for_airflow_to_be_ready():
                print(f"⚠️ Airflow readiness check failed, but continuing...")
            else:
                print(f"✅ Airflow is ready!")
        except Exception as e:
            raise RuntimeError(
                f"Failed to create AirflowManager instance for ECS deployment: {e}"
            ) from e
        print(f"🚀 AirflowManager instance creation took {time.time() - start_time:.2f}s")
        print(f"🚀 Total ECS Airflow deployment took {time.time() - total_start_time:.2f}s")

        creation_end = time.time()
        print(f"✅ ECS Airflow deployment took {creation_end - creation_start:.2f}s")

        resource_data = AirflowResourceData(
            resource_id=resource_id,
            type="airflow_resource",
            creation_time=creation_start,
            creation_duration=creation_end - creation_start,
            description=f"ECS Airflow resource for {resource_id}",
            status="active",
            deployment_id=namespace,  # Use namespace as deployment ID
            deployment_name=namespace,
            secret_suffix=resource_id,  # Use resource_id as secret suffix
            base_url=base_url,
            api_url=api_url,
            api_token="",
            api_headers=airflow_api_client.api_headers,
            username=os.getenv("AIRFLOW_USERNAME", "admin"),
            password=os.getenv("AIRFLOW_PASSWORD", "admin"),
            airflow_instance=airflow_api_client,
            test_dir=test_dir,
            k8s_manager=None,
            k8s_namespace=None,
            external_ip=None,
            ecs_manager=ecs_manager,
            ecs_namespace=namespace,
            ecs_endpoint=endpoint,
        )

        # Store the ecs manager for later use
        self._ecs_manager = ecs_manager
        self._ecs_namespace = namespace

        print(f"✅ ECS Airflow resource {resource_id} ready at {base_url}")
        return resource_data

    def _test_teardown(self) -> None:
        """
        Clean up the resource and ensure proper teardown.
        Handles both successful setup and partial setup failures.
        """
        from braintrust import traced

        @traced(name=f"{self.get_resource_type()}.test_teardown")
        def inner_test_teardown(resource_data: Optional[AirflowResourceData] = None) -> None:
            return self.test_teardown(resource_data)

        # Check if _resource_data exists before trying to use it
        if hasattr(self, "_resource_data") and self._resource_data:
            inner_test_teardown(self._resource_data)
        else:
            # Handle partial setup failure - try to clean up Kubernetes or ECS resources if they exist
            print(
                f"⚠️ No _resource_data found for {self.get_resource_type()}, attempting cleanup of partial setup"
            )
            if hasattr(self, "_k8s_manager") and hasattr(self, "_k8s_namespace") and self._k8s_manager and self._k8s_namespace:
                print("🧹 Cleaning up partially initialized Kubernetes resources...")
                try:
                    self._k8s_manager.delete_namespace(namespace=self._k8s_namespace)
                    print(f"✅ Cleaned up namespace {self._k8s_namespace} from partial setup")
                except Exception as e:
                    print(f"⚠️ Error cleaning up partial setup: {e}")
            elif hasattr(self, "_ecs_manager") and hasattr(self, "_ecs_namespace") and self._ecs_manager and self._ecs_namespace:
                print("🧹 Cleaning up partially initialized ECS resources...")
                try:
                    self._ecs_manager.cleanup_deployment(namespace=self._ecs_namespace, cleanup_infrastructure=False)
                    print(f"✅ Cleaned up namespace {self._ecs_namespace} from partial setup")
                except Exception as e:
                    print(f"⚠️ Error cleaning up partial setup: {e}")
            else:
                print("⚠️ No Kubernetes or ECS resources found to clean up")

    def test_teardown(self, resource_data: Optional[AirflowResourceData] = None) -> None:
        """Clean up individual Airflow resource (Astro, Kubernetes, or ECS)"""
        # Handle case where resource_data might be None (partial setup failure)
        if resource_data is None:
            resource_id = getattr(self, "custom_config", {}).get("resource_id", "unknown")
        else:
            resource_id = resource_data["resource_id"]

        print(f"🧹 Cleaning up Airflow resource: {resource_id}")

        # Check if this is a Kubernetes deployment
        if hasattr(self, "_k8s_manager") and self._k8s_manager:
            # Use resource_data if available, otherwise construct minimal data for cleanup
            if resource_data is None:
                # Create minimal resource_data for cleanup
                if namespace := getattr(self, "_k8s_namespace", None):
                    resource_data = AirflowResourceData(
                        resource_id=resource_id,
                        type="airflow_resource",
                        creation_time=0,
                        creation_duration=0,
                        description="Partial setup cleanup",
                        status="failed",
                        deployment_id=namespace,
                        deployment_name=namespace,
                        secret_suffix=resource_id,
                        base_url="",
                        api_url="",
                        api_token="",
                        api_headers={},
                        username="",
                        password="",
                        airflow_instance=None,
                        test_dir=None,
                        k8s_manager=self._k8s_manager,
                        k8s_namespace=namespace,
                        external_ip=None,
                        ecs_manager=None,
                        ecs_namespace=None,
                        ecs_endpoint=None,
                    )
            if resource_data:
                self._cleanup_kubernetes_airflow(resource_data)
        # Check if this is an ECS deployment
        elif hasattr(self, "_ecs_manager") and self._ecs_manager:
            # Use resource_data if available, otherwise construct minimal data for cleanup
            if resource_data is None:
                # Create minimal resource_data for cleanup
                if namespace := getattr(self, "_ecs_namespace", None):
                    resource_data = AirflowResourceData(
                        resource_id=resource_id,
                        type="airflow_resource",
                        creation_time=0,
                        creation_duration=0,
                        description="Partial setup cleanup",
                        status="failed",
                        deployment_id=namespace,
                        deployment_name=namespace,
                        secret_suffix=resource_id,
                        base_url="",
                        api_url="",
                        api_token="",
                        api_headers={},
                        username="",
                        password="",
                        airflow_instance=None,
                        test_dir=None,
                        k8s_manager=None,
                        k8s_namespace=None,
                        external_ip=None,
                        ecs_manager=self._ecs_manager,
                        ecs_namespace=namespace,
                        ecs_endpoint=None,
                    )
            if resource_data:
                self._cleanup_ecs_airflow(resource_data)
        # Otherwise, use the AirflowManager cleanup
        elif hasattr(self, "_airflow_manager") and self._airflow_manager:
            test_dir = resource_data.get("test_dir") if resource_data else None
            self._airflow_manager.cleanup_resource(test_dir)
            print(f"✅ Airflow resource {resource_id} cleaned up using AirflowManager")
        else:
            print(f"⚠️ No manager found for {resource_id}, skipping cleanup")

    def _cleanup_kubernetes_airflow(self, resource_data: AirflowResourceData) -> None:
        """Clean up Kubernetes Airflow deployment"""
        resource_id = resource_data["resource_id"]
        k8s_namespace = resource_data.get("k8s_namespace")

        if not k8s_namespace:
            print(f"⚠️ No Kubernetes namespace found for {resource_id}, skipping K8s cleanup")
            return

        print(f"🧹 Cleaning up Kubernetes resources in namespace: {k8s_namespace}")

        try:
            # Delete the job
            if hasattr(self, "_k8s_manager") and self._k8s_manager:
                self._k8s_manager.delete_namespace(namespace=k8s_namespace)
                print(f"✅ Deleted Kubernetes namespace: {k8s_namespace}")

            # Note: We don't delete the namespace or service here to allow for debugging
            # They will be cleaned up by Kubernetes garbage collection or manual cleanup
            print(f"✅ Kubernetes resource {resource_id} cleanup complete")

        except Exception as e:
            print(f"⚠️ Error during Kubernetes cleanup: {e}")

        try:
            # Delete the repository from the ACR
            if hasattr(self, "_k8s_manager") and self._k8s_manager:
                repo_name = resource_id.replace("_", "-")[:50]
                self._k8s_manager.delete_repo_from_acr(acr_name=os.getenv("AZURE_ACR_NAME"), repo_name=repo_name)
                print(f"✅ Deleted repository from ACR: {repo_name}")

        except Exception as e:
            print(f"⚠️ Error during ACR cleanup: {e}")

        # Clean up temporary directory
        test_dir = resource_data.get("test_dir")
        if test_dir and test_dir.exists():
            import shutil
            try:
                shutil.rmtree(test_dir)
                print(f"✅ Cleaned up temporary directory: {test_dir}")
            except Exception as e:
                print(f"⚠️ Error cleaning up temporary directory: {e}")

    def _cleanup_ecs_airflow(self, resource_data: AirflowResourceData) -> None:
        """Clean up ECS Airflow deployment"""
        resource_id = resource_data["resource_id"]
        ecs_namespace = resource_data.get("ecs_namespace")

        if not ecs_namespace:
            print(f"⚠️ No ECS namespace found for {resource_id}, skipping ECS cleanup")
            return

        print(f"🧹 Cleaning up ECS resources for namespace: {ecs_namespace}")

        try:
            # Clean up ECS deployment (service, task definitions, load balancer, etc.)
            if hasattr(self, "_ecs_manager") and self._ecs_manager:
                self._ecs_manager.cleanup_deployment(
                    namespace=ecs_namespace,
                    cleanup_infrastructure=False  # Don't cleanup shared infrastructure
                )
                print(f"✅ Deleted ECS deployment: {ecs_namespace}")

            print(f"✅ ECS resource {resource_id} cleanup complete")

        except Exception as e:
            print(f"⚠️ Error during ECS cleanup: {e}")

        try:
            # Delete the repository from ECR
            if hasattr(self, "_ecs_manager") and self._ecs_manager:
                repo_name = resource_id.replace("_", "-")[:50]
                self._ecs_manager.delete_repo_from_ecr(repo_name=repo_name)
                print(f"✅ Deleted repository from ECR: {repo_name}")

        except Exception as e:
            print(f"⚠️ Error during ECR cleanup: {e}")

        # Clean up temporary directory
        test_dir = resource_data.get("test_dir")
        if test_dir and test_dir.exists():
            import shutil
            try:
                shutil.rmtree(test_dir)
                print(f"✅ Cleaned up temporary directory: {test_dir}")
            except Exception as e:
                print(f"⚠️ Error cleaning up temporary directory: {e}")

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
                "username": resource_data.get("username", os.getenv("AIRFLOW_USERNAME", "airflow")),  # Default to standard Airflow username
                "password": resource_data.get("password", os.getenv("AIRFLOW_PASSWORD", "airflow")),  # Default to standard Airflow password
            }
        }


# Note: No global instance - each test creates its own AirflowFixture
# with custom resource_id in get_fixtures()
