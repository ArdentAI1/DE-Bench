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
    airflow_provider: Optional[
        str
    ]  # Deployment provider: "astro", "aks", or "ecs" (default: "astro")
    container_image: Optional[str]  # Container image for AKS/ECS deployment
    kubernetes_namespace: Optional[str]  # Kubernetes namespace (for AKS)
    ecs_namespace: Optional[str]  # ECS namespace (for ECS)
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
    airflow_provider: Optional[str] = (
        "astro"  # Deployment provider: "astro", "aks", or "ecs"
    )
    # Shared NGINX Ingress Controller fields (for AKS)
    shared_ingress_ip: Optional[str]  # External IP of shared LoadBalancer
    shared_ingress_namespace: Optional[str]  # Namespace for NGINX Ingress Controller
    shared_k8s_manager: Optional[Any]  # Shared KubernetesManifestManager instance


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
        self, session_configs: Optional[List[AirflowResourceConfig]] = None
    ) -> AirflowSessionData:
        """
        Set up shared Airflow session resources based on ALL test configurations.

        This receives ALL fixture configs from ALL tests, allowing it to:
        - Detect if any test needs AKS → set up shared NGINX Ingress
        - Detect if any test needs ECS → set up ECS infrastructure
        - Detect if any test needs Astro → set up Astro login + cache

        Args:
            session_configs: List of ALL AirflowResourceConfig from ALL tests in the session
        """
        print("🌐 Setting up Airflow session-level resources...")

        # Default if no configs provided
        if not session_configs:
            session_configs = []

        # Analyze all configs to determine what infrastructure is needed
        providers_needed = set()
        for config in session_configs:
            provider = config.get("airflow_provider", "astro").lower()
            providers_needed.add(provider)

        print(f"📊 Analyzing {len(session_configs)} test configs...")
        print(f"   Providers needed across all tests: {providers_needed}")

        # Validate all providers
        valid_providers = ["astro", "aks", "ecs"]
        for provider in providers_needed:
            if provider not in valid_providers:
                raise ValueError(
                    f"Invalid airflow_provider '{provider}'. Must be one of: {valid_providers}"
                )

        # Initialize session data structure
        session_data = AirflowSessionData(
            cache_manager=None,
            astro_logged_in=False,
            available_deployments=None,
            airflow_provider=None,  # Will be determined per-test
            shared_ingress_ip=None,
            shared_ingress_namespace=None,
            shared_k8s_manager=None,
        )

        # Validate environment variables for EACH provider that's needed
        for provider in providers_needed:
            if provider == "aks":
                required_envars = [
                    "DE_BENCH_AKS_RESOURCE_GROUP",
                    "DE_BENCH_AKS_CLUSTER_NAME",
                    "DE_BENCH_AKS_IMAGE_NAME",
                ]
            elif provider == "ecs":
                required_envars = [
                    "DE_BENCH_ECS_CLUSTER_NAME",
                    "AWS_REGION",
                ]
            else:  # astro
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
                    f"Missing required environment variables for {provider}: {missing_envars}"
                )

        # Set up infrastructure for EACH provider that's needed
        if "aks" in providers_needed:
            # AKS mode: Deploy shared NGINX Ingress Controller (once per session)
            print("✅ AKS mode detected - setting up shared NGINX Ingress Controller")

            from Environment.Kubernetes.ManifestManager import KubernetesManifestManager

            # Create shared KubernetesManifestManager
            k8s_manager = KubernetesManifestManager(provider="AZURE")

            # Deploy shared NGINX Ingress Controller
            ingress_namespace = "ingress-system"
            print(
                f"🚀 Deploying shared NGINX Ingress Controller in namespace: {ingress_namespace}"
            )

            try:
                shared_ingress_ip = k8s_manager.setup_shared_ingress_controller(
                    namespace=ingress_namespace,
                    wait_for_ready=True,
                )

                print(
                    f"✅ Shared NGINX Ingress Controller ready at {shared_ingress_ip}"
                )

                # Store AKS-specific session data
                session_data["shared_ingress_ip"] = shared_ingress_ip
                session_data["shared_ingress_namespace"] = ingress_namespace
                session_data["shared_k8s_manager"] = k8s_manager

            except Exception as e:
                print(f"❌ Failed to setup shared NGINX Ingress Controller: {e}")
                # Attempt cleanup
                try:
                    k8s_manager.cleanup_shared_ingress_controller(
                        namespace=ingress_namespace
                    )
                except Exception as cleanup_error:
                    print(f"⚠️ Error during cleanup: {cleanup_error}")
                raise

        if "ecs" in providers_needed:
            # ECS setup (if needed in the future)
            print("✅ ECS mode detected - ECS session setup placeholder")

        if "astro" in providers_needed:
            # Astro setup (currently commented out due to CLI conflicts)
            print("✅ Astro mode detected - Astro session setup placeholder")
            # # switch to the correct workspace
            # # self._switch_to_correct_workspace()  # COMMENTED OUT - causing concurrent CLI conflicts
            # # make sure ASTRO_API_TOKEN is set
            # if not os.getenv("ASTRO_API_TOKEN"):
            #     raise ValueError("ASTRO_API_TOKEN must be set")
            # # 1. Ensure Astro login
            # print("🔐 Ensuring Astro CLI login...")
            # temp_manager = AirflowManager()
            # temp_manager._ensure_astro_login()
            # # 2. Initialize cache manager
            # print("💾 Initializing deployment cache manager...")
            # from Fixtures.Airflow.airflow_resources import _ensure_cache_manager_initialized
            # cache_manager = _ensure_cache_manager_initialized()
            # # 3. Get available deployments
            # available_deployments = cache_manager.get_all_astronomer_deployments()
            # print(f"✅ Airflow session setup complete! Found {len(available_deployments)} deployments in cache")
            # session_data["cache_manager"] = cache_manager
            # session_data["astro_logged_in"] = True
            # session_data["available_deployments"] = available_deployments

        print("✅ All session-level infrastructure provisioned")
        return session_data

    def session_teardown(
        self, session_data: Optional[AirflowSessionData] = None
    ) -> None:
        """
        Clean up session-level Airflow resources for ALL providers that were set up.

        Note: By default, the shared NGINX Ingress Controller is NOT cleaned up,
        as it's designed to be a persistent piece of infrastructure that can be
        reused across multiple test sessions. Set DE_BENCH_CLEANUP_NGINX=true
        to force cleanup (useful for CI/CD or complete teardown).
        """
        if not session_data:
            return

        print("🧹 Cleaning up Airflow session-level resources...")

        # Check if we need to cleanup shared NGINX Ingress Controller (AKS)
        cleanup_nginx = os.getenv("DE_BENCH_CLEANUP_NGINX", "false").lower() == "true"

        if session_data.get("shared_k8s_manager"):
            k8s_manager = session_data.get("shared_k8s_manager")
            ingress_namespace = session_data.get("shared_ingress_namespace")

            if k8s_manager and ingress_namespace:
                if cleanup_nginx:
                    print(
                        f"🧹 Cleaning up shared NGINX Ingress Controller (AKS) in {ingress_namespace}..."
                    )
                    print("   (DE_BENCH_CLEANUP_NGINX=true)")
                    try:
                        k8s_manager.cleanup_shared_ingress_controller(
                            namespace=ingress_namespace
                        )
                        print("✅ Shared NGINX Ingress Controller cleaned up")
                    except Exception as e:
                        print(
                            f"⚠️ Error cleaning up shared NGINX Ingress Controller: {e}"
                        )
                else:
                    print(
                        f"⏭️  Skipping NGINX cleanup - will be reused in future sessions"
                    )
                    print(
                        f"   NGINX Ingress remains at {session_data.get('shared_ingress_ip')}"
                    )
                    print("   (Set DE_BENCH_CLEANUP_NGINX=true to force cleanup)")

        # ECS cleanup (if needed in the future)
        # if session_data.get("ecs_specific_field"):
        #     print("🧹 Cleaning up ECS resources...")

        # Astro cleanup (if needed in the future)
        # if session_data.get("cache_manager"):
        #     print("🧹 Cleaning up Astro resources...")

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
        Supports Astro, AKS (Kubernetes), and ECS deployment providers.
        """
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]

        # Get provider from config or default to "astro"
        provider = config.get("airflow_provider", "astro").lower()

        # Validate provider value
        valid_providers = ["astro", "aks", "ecs"]
        if provider not in valid_providers:
            raise ValueError(
                f"Invalid airflow_provider '{provider}'. Must be one of: {valid_providers}"
            )

        print(f"🚀 Setting up Airflow resource: {resource_id}")
        print(f"📦 Deployment provider: {provider}")

        creation_start = time.time()

        try:
            if provider == "aks":
                # AKS (Kubernetes) deployment path
                return self._setup_kubernetes_airflow(
                    config, resource_id, creation_start
                )
            elif provider == "ecs":
                # ECS deployment path
                return self._setup_ecs_airflow(config, resource_id, creation_start)
            else:  # astro
                # Astro deployment path
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
                "[Astro setup] Session data not available - session setup may have failed"
            )

        # Ensure we're in Astro mode
        if session_data.get("airflow_provider", "astro") != "astro":
            raise RuntimeError(
                f"Cannot use Astro setup when provider is '{session_data.get('airflow_provider')}'"
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
        print(f"✅ Airflow resource creation took {creation_end - creation_start:.2f}s")

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
        """Set up Airflow using Kubernetes with shared NGINX Ingress Controller"""

        # Get session-level shared resources
        session_data = self.session_data
        if not session_data:
            raise RuntimeError("Session data not available for AKS deployment")

        shared_ingress_ip = session_data.get("shared_ingress_ip")
        shared_k8s_manager = session_data.get("shared_k8s_manager")

        if not shared_ingress_ip or not shared_k8s_manager:
            raise RuntimeError(
                "Shared NGINX Ingress Controller not initialized. "
                "Ensure session_setup() ran successfully."
            )

        print(f"📡 Using shared NGINX Ingress at {shared_ingress_ip}")

        # Get container image from config or environment
        container_image = config.get("container_image") or os.getenv(
            "AIRFLOW_CONTAINER_IMAGE"
        )
        if not container_image:
            raise ValueError(
                "AIRFLOW_CONTAINER_IMAGE environment variable is not set and the container_image is not set in the config."
            )

        # Get or generate namespace
        namespace = (
            config.get("kubernetes_namespace") or f"airflow-{resource_id}".lower()[:63]
        )

        print(f"🐳 Container image: {container_image}")
        print(f"📦 Kubernetes namespace: {namespace}")

        # Use shared K8s manager from session
        k8s_manager = shared_k8s_manager
        # Store k8s_manager and namespace immediately for cleanup in case of failure
        self._k8s_manager = k8s_manager
        self._k8s_namespace = namespace

        total_start_time = time.time()

        try:
            # Step 1: Deploy namespace + Airflow Job (creates ClusterIP service automatically)
            start_time = time.time()
            print("🚀 Deploying Airflow namespace and workload...")
            k8s_manager.generate_and_apply_manifest(
                namespace=namespace, container=container_image
            )
            print(f"   ✓ Deployment took {time.time() - start_time:.2f}s")

            # Step 2: Create Ingress resource for path-based routing
            start_time = time.time()
            print("🔗 Creating Ingress resource for path-based routing...")
            path_prefix = f"/{namespace}"
            ingress_name = f"{namespace}-ingress"
            service_name = f"{namespace}-service"

            k8s_manager.create_ingress_resource(
                namespace=namespace,
                ingress_name=ingress_name,
                service_name=service_name,
                path_prefix=path_prefix,
                service_port=8080,
                ingress_class="nginx",
            )
            print(f"   ✓ Ingress creation took {time.time() - start_time:.2f}s")

            # Step 3: Construct base URL using shared LoadBalancer IP + path prefix
            base_url = f"http://{shared_ingress_ip}{path_prefix}"
            api_url = f"{base_url}/api/v1"

            print(f"📍 Airflow will be accessible at: {base_url}")

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

        # Create a temporary directory for test artifacts
        test_dir = Path(tempfile.mkdtemp(prefix=f"airflow_k8s_{resource_id}_"))

        # Create AirflowManager instance pointing to the AKS deployment
        start_time = time.time()
        try:
            airflow_api_client = AirflowManager(
                host=base_url,
                api_url=api_url,
                api_token="",  # Not used for AKS
                resource_id=resource_id,
                provider="aks",
            )
            print("✅ Created AirflowManager instance for Kubernetes deployment")

            # Wait for Airflow to be fully ready
            print("⏳ Waiting for Airflow to be ready...")
            if not airflow_api_client.wait_for_airflow_to_be_ready():
                print("⚠️ Airflow readiness check failed, but continuing...")
            else:
                print("✅ Airflow is ready!")
        except Exception as e:
            raise RuntimeError(
                f"Failed to create AirflowManager instance for Kubernetes deployment: {e}"
            ) from e

        print(f"🚀 AirflowManager setup took {time.time() - start_time:.2f}s")
        print(f"🚀 Total deployment took {time.time() - total_start_time:.2f}s")

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
            external_ip=shared_ingress_ip,  # The shared NGINX Ingress Controller IP
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
        container_image = config.get("container_image") or os.getenv(
            "AIRFLOW_CONTAINER_IMAGE"
        )
        if not container_image:
            raise ValueError(
                "AIRFLOW_CONTAINER_IMAGE environment variable is not set and the container_image is not set in the config."
            )

        # Get or generate namespace (replace underscores with hyphens for AWS naming constraints)
        namespace = (
            config.get("ecs_namespace")
            or f"airflow-{resource_id}".replace("_", "-").lower()[:63]
        )

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
                enable_load_balancer=enable_load_balancer,
            )
            print(f"🚀 ECS deployment took {time.time() - start_time:.2f}s")

            # Get the endpoint from the deployment result
            start_time = time.time()
            print(f"🔍 Getting ECS endpoint...")

            if enable_load_balancer:
                # Use load balancer DNS
                endpoint = deployment_result.get("dns_name")
                if not endpoint:
                    raise RuntimeError(
                        f"Failed to get load balancer DNS for namespace {namespace}"
                    )

                # Get port from deployment result (NLB uses 8080, ALB uses 80)
                port = deployment_result.get("port", 8080)
                base_url = deployment_result.get(
                    "base_url", f"http://{endpoint}:{port}"
                )
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
                    print(
                        f"   Still waiting for task IP... ({int(time.time() - wait_start)}s elapsed)"
                    )
                    time.sleep(10)

                if not endpoint:
                    raise RuntimeError(
                        f"Failed to get public IP for namespace {namespace} after {max_wait}s"
                    )

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
                    ecs_manager.cleanup_deployment(
                        namespace=namespace, cleanup_infrastructure=False
                    )
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
            airflow_api_client = AirflowManager(
                host=base_url,
                api_url=api_url,
                api_token="",  # Not used for ECS
                resource_id=resource_id,
                provider="ecs",
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
        print(
            f"🚀 AirflowManager instance creation took {time.time() - start_time:.2f}s"
        )
        print(
            f"🚀 Total ECS Airflow deployment took {time.time() - total_start_time:.2f}s"
        )

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
        def inner_test_teardown(
            resource_data: Optional[AirflowResourceData] = None,
        ) -> None:
            return self.test_teardown(resource_data)

        # Check if _resource_data exists before trying to use it
        if hasattr(self, "_resource_data") and self._resource_data:
            inner_test_teardown(self._resource_data)
        else:
            # Handle partial setup failure - try to clean up Kubernetes or ECS resources if they exist
            print(
                f"⚠️ No _resource_data found for {self.get_resource_type()}, attempting cleanup of partial setup"
            )
            if (
                hasattr(self, "_k8s_manager")
                and hasattr(self, "_k8s_namespace")
                and self._k8s_manager
                and self._k8s_namespace
            ):
                print("🧹 Cleaning up partially initialized Kubernetes resources...")
                try:
                    self._k8s_manager.delete_namespace(namespace=self._k8s_namespace)
                    print(
                        f"✅ Cleaned up namespace {self._k8s_namespace} from partial setup"
                    )
                except Exception as e:
                    print(f"⚠️ Error cleaning up partial setup: {e}")
            elif (
                hasattr(self, "_ecs_manager")
                and hasattr(self, "_ecs_namespace")
                and self._ecs_manager
                and self._ecs_namespace
            ):
                print("🧹 Cleaning up partially initialized ECS resources...")
                try:
                    self._ecs_manager.cleanup_deployment(
                        namespace=self._ecs_namespace, cleanup_infrastructure=False
                    )
                    print(
                        f"✅ Cleaned up namespace {self._ecs_namespace} from partial setup"
                    )
                except Exception as e:
                    print(f"⚠️ Error cleaning up partial setup: {e}")
            else:
                print("⚠️ No Kubernetes or ECS resources found to clean up")

    def test_teardown(
        self, resource_data: Optional[AirflowResourceData] = None
    ) -> None:
        """Clean up individual Airflow resource (Astro, Kubernetes, or ECS)"""
        # Handle case where resource_data might be None (partial setup failure)
        if resource_data is None:
            resource_id = getattr(self, "custom_config", {}).get(
                "resource_id", "unknown"
            )
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
            print(
                f"⚠️ No Kubernetes namespace found for {resource_id}, skipping K8s cleanup"
            )
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
                self._k8s_manager.delete_repo_from_acr(
                    acr_name=os.getenv("AZURE_ACR_NAME"), repo_name=repo_name
                )
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
                    cleanup_infrastructure=False,  # Don't cleanup shared infrastructure
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
                "username": resource_data.get(
                    "username", os.getenv("AIRFLOW_USERNAME", "airflow")
                ),  # Default to standard Airflow username
                "password": resource_data.get(
                    "password", os.getenv("AIRFLOW_PASSWORD", "airflow")
                ),  # Default to standard Airflow password
            }
        }


# Note: No global instance - each test creates its own AirflowFixture
# with custom resource_id in get_fixtures()
