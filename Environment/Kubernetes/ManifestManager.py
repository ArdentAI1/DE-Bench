"""
POC to generate and apply a new manifest file for AKS that creates a namespace, a pod, and a LoadBalancer
"""

import os
import time
from typing import Optional

import yaml
from azure.identity import ClientSecretCredential
from azure.mgmt.containerservice import ContainerServiceClient
from azure.containerregistry import ContainerRegistryClient
from dotenv import load_dotenv
from kubernetes import client as k8s_client_sdk
from kubernetes import config as k8s_config

load_dotenv()


class KubernetesManifestManager:
    """Manages Kubernetes manifest generation and application"""

    # Shared NGINX Ingress Controller manifest (deployed once per session)
    NGINX_INGRESS_MANIFEST = """
---
apiVersion: v1
kind: Namespace
metadata:
  name: INGRESS_NAMESPACE

---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nginx-ingress
  namespace: INGRESS_NAMESPACE

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: nginx-ingress-role
rules:
  - apiGroups: [""]
    resources: ["configmaps", "endpoints", "nodes", "pods", "secrets", "namespaces"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingresses/status"]
    verbs: ["update"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingressclasses"]
    verbs: ["get", "list", "watch"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: nginx-ingress-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: nginx-ingress-role
subjects:
  - kind: ServiceAccount
    name: nginx-ingress
    namespace: INGRESS_NAMESPACE

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-configuration
  namespace: INGRESS_NAMESPACE
data:
  proxy-connect-timeout: "3600"
  proxy-send-timeout: "3600"
  proxy-read-timeout: "3600"
  proxy-body-size: "0"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-ingress-controller
  namespace: INGRESS_NAMESPACE
  labels:
    app: nginx-ingress
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nginx-ingress
  template:
    metadata:
      labels:
        app: nginx-ingress
    spec:
      serviceAccountName: nginx-ingress
      containers:
      - name: nginx-ingress-controller
        image: registry.k8s.io/ingress-nginx/controller:v1.9.4
        args:
          - /nginx-ingress-controller
          - --ingress-class=nginx
          - --configmap=$(POD_NAMESPACE)/nginx-configuration
          - --tcp-services-configmap=$(POD_NAMESPACE)/tcp-services
          - --publish-service=$(POD_NAMESPACE)/nginx-ingress
        env:
          - name: POD_NAME
            valueFrom:
              fieldRef:
                fieldPath: metadata.name
          - name: POD_NAMESPACE
            valueFrom:
              fieldRef:
                fieldPath: metadata.namespace
        ports:
          - name: http
            containerPort: 80
          - name: https
            containerPort: 443
        livenessProbe:
          httpGet:
            path: /healthz
            port: 10254
            scheme: HTTP
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 1
        readinessProbe:
          httpGet:
            path: /healthz
            port: 10254
            scheme: HTTP
          periodSeconds: 10
          timeoutSeconds: 1

---
apiVersion: v1
kind: Service
metadata:
  name: nginx-ingress
  namespace: INGRESS_NAMESPACE
  labels:
    app: nginx-ingress
spec:
  type: LoadBalancer
  ports:
  - name: http
    port: 80
    targetPort: 80
    protocol: TCP
  - name: https
    port: 443
    targetPort: 443
    protocol: TCP
  selector:
    app: nginx-ingress

---
apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata:
  name: nginx
  annotations:
    ingressclass.kubernetes.io/is-default-class: "true"
spec:
  controller: k8s.io/ingress-nginx
"""

    # Per-test manifest template (ClusterIP + Ingress, no LoadBalancer)
    MANIFEST_TEMPLATE = """
---
apiVersion: v1
kind: Namespace
metadata:
  name: NAMESPACE_VALUE

---
apiVersion: batch/v1
kind: Job
metadata:
  name: NAMESPACE_VALUE
  namespace: NAMESPACE_VALUE
  labels:
    app: airflow
    test-id: "NAMESPACE_VALUE"
spec:
  backoffLimit: 4
  activeDeadlineSeconds: 3600
  template:
    metadata:
      labels:
        app: airflow
        test-id: "NAMESPACE_VALUE"
    spec:
      restartPolicy: OnFailure
      containers:
      - name: airflow
        image: CONTAINER_NAME
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
          name: airflow-web
        env:
        - name: IS_SANDBOX
          value: "1"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "3Gi"

---
apiVersion: v1
kind: Service
metadata:
  name: NAMESPACE_VALUE-service
  namespace: NAMESPACE_VALUE
  labels:
    app: airflow
    test-id: "NAMESPACE_VALUE"
spec:
  type: ClusterIP
  ports:
  - port: 8080
    targetPort: 8080
    protocol: TCP
    name: airflow-web
  selector:
    app: airflow
"""

    def __init__(self, provider: str = "AZURE"):
        self.provider = provider
        self.cloud_provider_client = self.get_cloud_provider_client()
        self.k8s_core_api = None
        self.k8s_batch_api = None

    def get_cloud_provider_client(self):
        """Get the cloud provider client (Azure)"""
        if self.provider not in ["AZURE"]:
            raise NotImplementedError("Provider not supported")

        azure_credential = ClientSecretCredential(
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
        )

        azure_client = ContainerServiceClient(
            azure_credential,
            os.getenv("AZURE_SUBSCRIPTION_ID"),  # type: ignore
        )

        if not azure_client:
            raise EnvironmentError("Failed to create Azure client")

        return azure_client

    def get_acr_client(self):
        """Get the Azure Container Registry client"""
        azure_credential = ClientSecretCredential(
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
        )

        # Get the ACR endpoint URL (should be like https://yourregistry.azurecr.io)
        acr_name = os.getenv("AZURE_ACR_NAME")
        if not acr_name:
            raise EnvironmentError("AZURE_ACR_NAME environment variable is required")

        # Construct the endpoint URL
        endpoint = f"https://{acr_name}"

        acr_client = ContainerRegistryClient(
            endpoint=endpoint, credential=azure_credential
        )

        if not acr_client:
            raise EnvironmentError("Failed to create Azure Container Registry client")

        return acr_client

    def _ensure_api_attributes_set(self):
        """
        Ensure Kubernetes API clients are initialized
        """
        if not self.k8s_core_api or not self.k8s_batch_api:
            self.get_k8s_client()

    def get_k8s_client(self):
        """Get Kubernetes API clients"""
        try:
            kubeconfig = (
                self.cloud_provider_client.managed_clusters.list_cluster_user_credentials(
                    os.getenv("DE_BENCH_AKS_RESOURCE_GROUP"),
                    os.getenv("DE_BENCH_AKS_CLUSTER_NAME"),
                )
                .kubeconfigs[0]
                .value
            )
        except Exception as e:
            raise Exception(f"Failed to load container credentials: {e}")  # noqa

        # Convert bytearray to string
        kubeconfig_str = kubeconfig.decode("utf-8")  # noqa

        # Write kubeconfig to file
        kubeconfig_path = os.path.expanduser("~/.kube/config")
        os.makedirs(os.path.dirname(kubeconfig_path), exist_ok=True)
        with open(kubeconfig_path, "w") as f:
            f.write(kubeconfig_str)

        # Set the KUBECONFIG environment variable
        os.environ["KUBECONFIG"] = kubeconfig_path

        # Load the kubeconfig
        k8s_config.load_kube_config_from_dict(yaml.safe_load(kubeconfig_str))

        # Create API clients
        k8s_api_client = k8s_client_sdk.ApiClient()
        self.k8s_core_api = k8s_client_sdk.CoreV1Api(k8s_api_client)
        self.k8s_batch_api = k8s_client_sdk.BatchV1Api(k8s_api_client)

    def generate_manifest(self, namespace: str, container: str) -> str:
        """
        Generate manifest from template with given parameters

        :param str namespace: The namespace to create and use for the pod and service
        :param str container: The container image to use for the pod
        :return: Generated manifest as string
        """
        manifest = self.MANIFEST_TEMPLATE.replace("NAMESPACE_VALUE", namespace).replace(
            "CONTAINER_NAME", container
        )
        return manifest

    def generate_and_apply_manifest(self, namespace: str, container: str) -> None:
        """
        Generate and apply the manifest to the Kubernetes cluster

        :param str namespace: The namespace to create and use for the pod and service
        :param str container: The container image to use for the pod
        """
        # Ensure k8s clients are initialized
        self._ensure_api_attributes_set()

        # Generate the manifest
        manifest = self.generate_manifest(namespace, container)
        # Apply the manifest
        self.apply_manifest(manifest)

    def delete_job(self, job_name: str, namespace: str) -> bool:
        """
        Delete a Job from the Kubernetes cluster

        :param str job_name: Name of the Job to delete
        :param str namespace: Namespace where the Job exists
        :return: True if deleted, False if Job didn't exist
        """
        self._ensure_api_attributes_set()

        try:
            self.k8s_batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                propagation_policy="Foreground",  # Delete pods associated with the job
            )
            print(f"Deleted job: {job_name} in namespace: {namespace}")
            return True
        except k8s_client_sdk.ApiException as e:
            if e.status == 404:  # Job doesn't exist
                print(f"Job {job_name} not found in namespace {namespace}")
                return False
            else:
                raise Exception(f"Failed to delete job {job_name}: {e}")  # noqa

    def delete_namespace(self, namespace: str) -> bool:
        """
        Delete a Namespace from the Kubernetes cluster

        :param str namespace: The namespace to delete
        :return: True if deleted, False if Namespace didn't exist
        """
        self._ensure_api_attributes_set()
        try:
            self.k8s_core_api.delete_namespace(name=namespace)
            print(f"Deleted namespace: {namespace}")
            return True
        except k8s_client_sdk.ApiException as e:
            if e.status == 404:  # Namespace doesn't exist
                print(f"Namespace {namespace} not found")
                return False
            else:
                raise Exception(f"Failed to delete namespace {namespace}: {e}")  # noqa

    def reapply_job(self, namespace: str, container: str) -> None:
        """
        Reapply a Job by deleting the existing one and creating a new one with updated container.
        This is necessary because Job specs are immutable.

        :param str namespace: The namespace where the Job exists
        :param str container: The new container image to use
        """
        self._ensure_api_attributes_set()

        job_name = namespace  # Based on your manifest template, job name = namespace

        # Step 1: Delete the existing Job
        print(f"Deleting existing Job: {job_name} in namespace: {namespace}")
        self.delete_job(job_name, namespace)

        # Step 2: Generate new manifest with updated container
        manifest = self.generate_manifest(namespace, container)

        # Step 3: Apply only the Job from the manifest
        manifest_objects = list(yaml.safe_load_all(manifest))

        for obj in manifest_objects:
            if not obj:
                continue

            kind = obj.get("kind")
            if kind == "Job":
                metadata = obj.get("metadata", {})
                obj_name = metadata.get("name")
                obj_namespace = metadata.get("namespace", "default")

                self.k8s_batch_api.create_namespaced_job(
                    namespace=obj_namespace, body=obj
                )
                print(
                    f"Created new job: {obj_name} in namespace: {obj_namespace} with container: {container}"
                )
                break

    def apply_manifest(self, manifest: str) -> None:
        """
        Apply the given manifest to the Kubernetes cluster

        :param str manifest: The Kubernetes manifest to apply
        :rtype: None
        """
        self._ensure_api_attributes_set()
        # Parse the YAML manifest (contains multiple documents)
        manifest_objects = list(yaml.safe_load_all(manifest))

        # Apply each resource
        for obj in manifest_objects:
            if not obj:  # Skip empty documents
                continue

            kind = obj.get("kind")
            metadata = obj.get("metadata", {})
            obj_name = metadata.get("name")
            obj_namespace = metadata.get("namespace", "default")

            try:
                if kind == "Namespace":
                    # Create namespace
                    self.k8s_core_api.create_namespace(body=obj)
                    print(f"Created namespace: {obj_name}")

                elif kind == "Job":
                    # Create job
                    self.k8s_batch_api.create_namespaced_job(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"Created job: {obj_name} in namespace: {obj_namespace}")

                elif kind == "Service":
                    # Create service
                    self.k8s_core_api.create_namespaced_service(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"Created service: {obj_name} in namespace: {obj_namespace}")

                else:
                    print(f"Unknown resource kind: {kind}")

            except k8s_client_sdk.ApiException as e:
                if e.status == 409:  # Resource already exists
                    print(
                        f"{kind} {obj_name} already exists in namespace {obj_namespace}"
                    )
                else:
                    raise Exception(f"Failed to create {kind} {obj_name}: {e}")  # noqa

    def get_service_external_ip(self, namespace: str) -> Optional[str]:
        """
        Get the external IP of the LoadBalancer service in the given namespace
        """
        self._ensure_api_attributes_set()
        max_attempts = 60 * 5  # Wait up to 5 minutes (1 second per attempt)
        for attempt in range(max_attempts):
            try:
                service = self.k8s_core_api.read_namespaced_service(
                    name=f"{namespace}-service", namespace=namespace
                )
                if service.status.load_balancer.ingress:
                    external_ip = service.status.load_balancer.ingress[0].ip
                    if external_ip:
                        print(
                            f"Service {namespace}-service has external IP: {external_ip}"
                        )
                        print(f"Retrieved external IP in {attempt + 1} seconds")
                        return external_ip
                # Print progress every 30 seconds to avoid log spam
                if (attempt + 1) % 30 == 0 or attempt == 0:
                    print(
                        f"Waiting for external IP for service {namespace}-service... (attempt {attempt + 1}/{max_attempts})"
                    )
                time.sleep(1)
            except k8s_client_sdk.ApiException as e:
                if e.status != 404:  # 404 is expected if service doesn't exist yet
                    print(f"Error fetching service {namespace}-service: {e}")
                time.sleep(1)

        print(
            f"Failed to get external IP for service {namespace}-service after {max_attempts} attempts"
        )
        print(f"Waited for a total of {max_attempts} seconds")
        return None

    @staticmethod
    def verify_pod_health(external_ip: str, port: int = 8080) -> bool:
        """
        Verify the health of the pod by sending a request to the external IP and port

        :param str external_ip: The external IP of the service
        :param int port: The port to connect to (default is 8080)
        :return: True if the pod is healthy, False otherwise
        """
        import requests

        url = f"http://{external_ip}:{port}/health"  # noqa
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"Pod is healthy at {url}")
                return True
            else:
                print(
                    f"Pod health check failed with status code {response.status_code} at {url}"
                )
                return False
        except requests.RequestException as e:
            print(f"Error connecting to pod at {url}: {e}")
            return False

    def delete_repo_from_acr(self, acr_name: str, repo_name: str) -> bool:
        """
        Delete a repository from the ACR

        :param str acr_name: The name of the ACR (not used directly, taken from env var)
        :param str repo_name: The name of the repository to delete
        :return: True if deleted, False if repository didn't exist
        :rtype: bool
        """
        try:
            acr_client = self.get_acr_client()
            # Delete the repository using the ContainerRegistryClient
            acr_client.delete_repository(repository=repo_name)
            print(f"✅ Successfully deleted repository {repo_name} from ACR {acr_name}")
        except Exception as e:
            print(f"⚠️ Error deleting repository from ACR: {e}")
            return False
        return True

    # ===== SHARED NGINX INGRESS CONTROLLER METHODS =====

    def _nginx_ingress_exists(self, namespace: str = "ingress-system") -> bool:
        """
        Check if the NGINX Ingress Controller is already deployed.

        :param str namespace: Kubernetes namespace for the ingress controller
        :return: True if NGINX exists and is healthy, False otherwise
        :rtype: bool
        """
        self._ensure_api_attributes_set()

        try:
            # Check if namespace exists
            try:
                self.k8s_core_api.read_namespace(name=namespace)
            except k8s_client_sdk.ApiException as e:
                if e.status == 404:
                    return False
                raise

            # Check if deployment exists and is ready
            apps_api = k8s_client_sdk.AppsV1Api()
            try:
                deployment = apps_api.read_namespaced_deployment(
                    name="nginx-ingress-controller", namespace=namespace
                )
                # Check if deployment has ready replicas
                if (
                    deployment.status.ready_replicas
                    and deployment.status.ready_replicas > 0
                ):
                    return True
            except k8s_client_sdk.ApiException as e:
                if e.status == 404:
                    return False
                raise

            return False
        except Exception as e:
            print(f"⚠️ Error checking NGINX existence: {e}")
            return False

    def setup_shared_ingress_controller(
        self,
        namespace: str = "ingress-system",
        wait_for_ready: bool = True,
        force_recreate: bool = False,
    ) -> Optional[str]:
        """
        Ensure the shared NGINX Ingress Controller exists for path-based routing.

        If it already exists, just return its LoadBalancer IP (fast ~1-2 sec).
        If it doesn't exist, deploy it (slow ~2-3 min, one-time cost).

        This creates (if needed):
        - Ingress namespace
        - NGINX Ingress Controller deployment (2 replicas for HA)
        - LoadBalancer Service (the only LoadBalancer in the cluster)
        - RBAC resources (ServiceAccount, ClusterRole, ClusterRoleBinding)

        :param str namespace: Namespace for the ingress controller (default: ingress-system)
        :param bool wait_for_ready: Wait for LoadBalancer to get external IP
        :param bool force_recreate: If True, delete and recreate even if it exists
        :return: External IP address of the LoadBalancer, or None if not waiting
        :rtype: Optional[str]
        """
        self._ensure_api_attributes_set()

        print(
            f"🔧 Ensuring shared NGINX Ingress Controller exists in namespace: {namespace}"
        )

        # Check if NGINX already exists (unless force_recreate)
        if not force_recreate and self._nginx_ingress_exists(namespace=namespace):
            print("⚡ NGINX already exists - retrieving LoadBalancer IP (fast path)...")

            # Just get the existing LoadBalancer IP
            try:
                service = self.k8s_core_api.read_namespaced_service(
                    name="nginx-ingress", namespace=namespace
                )

                if service.status.load_balancer.ingress:
                    lb_ip = service.status.load_balancer.ingress[0].ip
                    print(f"✅ Using existing NGINX at {lb_ip} (no deployment needed)")
                    return lb_ip
                else:
                    print("⚠️ NGINX exists but LoadBalancer IP not ready, waiting...")
                    return self._wait_for_ingress_controller_ready(namespace)
            except Exception as e:
                print(f"⚠️ Error getting existing LoadBalancer IP: {e}")
                print("   Falling through to full deployment...")

        # NGINX doesn't exist or force_recreate requested - deploy it
        if force_recreate:
            print("🔄 force_recreate=True - recreating NGINX Ingress Controller")
            self.cleanup_shared_ingress_controller(namespace=namespace)

        print("📦 Deploying NGINX Ingress Controller resources (first-time setup)...")

        # Generate manifest with namespace substitution
        nginx_manifest = self.NGINX_INGRESS_MANIFEST.replace(
            "INGRESS_NAMESPACE", namespace
        )

        # Apply the manifest
        self._apply_ingress_manifest(nginx_manifest, namespace)

        if not wait_for_ready:
            print("⏭️  Skipping wait for LoadBalancer IP")
            return None

        # Wait for LoadBalancer to get external IP
        print(
            "⏳ Waiting for Azure LoadBalancer to provision (this may take 5-7 minutes)..."
        )
        external_ip = self._wait_for_ingress_controller_ready(namespace)

        if external_ip:
            print(f"✅ NGINX Ingress Controller ready at {external_ip}")
            return external_ip
        else:
            raise RuntimeError(
                f"Failed to provision LoadBalancer for NGINX Ingress Controller after maximum wait time"
            )

    def _apply_ingress_manifest(self, manifest: str, namespace: str) -> None:
        """
        Apply the NGINX Ingress Controller manifest with specialized handling.

        :param str manifest: The Kubernetes manifest to apply
        :param str namespace: The ingress namespace
        """
        self._ensure_api_attributes_set()

        # Parse the YAML manifest (contains multiple documents)
        manifest_objects = list(yaml.safe_load_all(manifest))

        # Create API clients for different resource types
        rbac_api = k8s_client_sdk.RbacAuthorizationV1Api()
        apps_api = k8s_client_sdk.AppsV1Api()
        networking_api = k8s_client_sdk.NetworkingV1Api()

        # Apply each resource
        for obj in manifest_objects:
            if not obj:  # Skip empty documents
                continue

            kind = obj.get("kind")
            metadata = obj.get("metadata", {})
            obj_name = metadata.get("name")
            obj_namespace = metadata.get("namespace", "default")

            try:
                if kind == "Namespace":
                    self.k8s_core_api.create_namespace(body=obj)
                    print(f"  ✓ Created namespace: {obj_name}")

                elif kind == "ServiceAccount":
                    self.k8s_core_api.create_namespaced_service_account(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"  ✓ Created ServiceAccount: {obj_name}")

                elif kind == "ClusterRole":
                    rbac_api.create_cluster_role(body=obj)
                    print(f"  ✓ Created ClusterRole: {obj_name}")

                elif kind == "ClusterRoleBinding":
                    rbac_api.create_cluster_role_binding(body=obj)
                    print(f"  ✓ Created ClusterRoleBinding: {obj_name}")

                elif kind == "ConfigMap":
                    self.k8s_core_api.create_namespaced_config_map(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"  ✓ Created ConfigMap: {obj_name}")

                elif kind == "Deployment":
                    apps_api.create_namespaced_deployment(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"  ✓ Created Deployment: {obj_name}")

                elif kind == "Service":
                    self.k8s_core_api.create_namespaced_service(
                        namespace=obj_namespace, body=obj
                    )
                    print(f"  ✓ Created Service: {obj_name}")

                elif kind == "IngressClass":
                    networking_api.create_ingress_class(body=obj)
                    print(f"  ✓ Created IngressClass: {obj_name}")

                else:
                    print(f"  ⚠️  Unknown resource kind: {kind}")

            except k8s_client_sdk.ApiException as e:
                if e.status == 409:  # Resource already exists
                    print(f"  ⏭️  {kind} {obj_name} already exists")
                else:
                    raise Exception(f"Failed to create {kind} {obj_name}: {e}")  # noqa

    def _wait_for_ingress_controller_ready(
        self,
        namespace: str,
        max_wait_seconds: int = 420,  # 7 minutes
    ) -> Optional[str]:
        """
        Wait for NGINX Ingress Controller to be ready and get LoadBalancer IP.

        :param str namespace: The ingress namespace
        :param int max_wait_seconds: Maximum time to wait (default: 420 seconds = 7 minutes)
        :return: External IP address or None if timeout
        :rtype: Optional[str]
        """
        self._ensure_api_attributes_set()

        start_time = time.time()
        last_status = ""

        while (time.time() - start_time) < max_wait_seconds:
            elapsed = int(time.time() - start_time)

            try:
                # Check LoadBalancer Service
                service = self.k8s_core_api.read_namespaced_service(
                    name="nginx-ingress", namespace=namespace
                )

                # Check if LoadBalancer has external IP
                if service.status.load_balancer.ingress:
                    external_ip = service.status.load_balancer.ingress[0].ip
                    if external_ip:
                        # Verify NGINX pods are ready
                        print(f"  📡 LoadBalancer IP obtained: {external_ip}")
                        print("  🔍 Verifying NGINX pods are ready...")

                        if self._check_nginx_pods_ready(namespace):
                            elapsed_min = elapsed // 60
                            elapsed_sec = elapsed % 60
                            print(
                                f"  ⏱️  Total setup time: {elapsed_min}m {elapsed_sec}s"
                            )
                            return external_ip
                        else:
                            print("  ⏳ NGINX pods not ready yet, waiting...")

                # Status message (print every 30 seconds to avoid spam)
                if elapsed % 30 == 0:
                    status_msg = f"  ⏳ Waiting for LoadBalancer IP... ({elapsed}s / {max_wait_seconds}s)"
                    if status_msg != last_status:
                        print(status_msg)
                        last_status = status_msg

            except k8s_client_sdk.ApiException as e:
                if e.status != 404:  # 404 is expected if service doesn't exist yet
                    print(f"  ⚠️  Error checking service: {e}")

            time.sleep(5)  # Check every 5 seconds

        print(f"  ❌ Timeout waiting for LoadBalancer after {max_wait_seconds} seconds")
        return None

    def _check_nginx_pods_ready(self, namespace: str) -> bool:
        """
        Check if NGINX Ingress Controller pods are ready.

        :param str namespace: The ingress namespace
        :return: True if at least one pod is ready
        :rtype: bool
        """
        self._ensure_api_attributes_set()

        try:
            pods = self.k8s_core_api.list_namespaced_pod(
                namespace=namespace, label_selector="app=nginx-ingress"
            )

            ready_pods = 0
            for pod in pods.items:
                if pod.status.phase == "Running":
                    # Check if all containers are ready
                    if pod.status.container_statuses:
                        all_ready = all(
                            container.ready
                            for container in pod.status.container_statuses
                        )
                        if all_ready:
                            ready_pods += 1

            print(f"    ✓ {ready_pods}/{len(pods.items)} NGINX pods ready")
            return ready_pods > 0  # At least one pod ready is sufficient

        except k8s_client_sdk.ApiException:
            return False

    def create_clusterip_service(
        self,
        namespace: str,
        service_name: str,
        target_port: int,
        selector: Optional[dict] = None,
    ) -> None:
        """
        Create a ClusterIP service (internal only, no Azure provisioning).

        ClusterIP services are instant and don't require Azure LoadBalancer provisioning.

        :param str namespace: Target namespace
        :param str service_name: Name of the service
        :param int target_port: Target port for the service
        :param dict selector: Pod selector (default: {"app": "airflow"})
        """
        self._ensure_api_attributes_set()

        if selector is None:
            selector = {"app": "airflow"}

        service_manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": service_name,
                "namespace": namespace,
                "labels": {
                    "app": "airflow",
                    "test-id": namespace,
                },
            },
            "spec": {
                "type": "ClusterIP",  # Internal service only
                "ports": [
                    {
                        "port": target_port,
                        "targetPort": target_port,
                        "protocol": "TCP",
                        "name": "http",
                    }
                ],
                "selector": selector,
            },
        }

        try:
            self.k8s_core_api.create_namespaced_service(
                namespace=namespace, body=service_manifest
            )
            print(f"✅ Created ClusterIP service: {service_name} in {namespace}")
        except k8s_client_sdk.ApiException as e:
            if e.status == 409:
                print(
                    f"⏭️  ClusterIP service {service_name} already exists in {namespace}"
                )
            else:
                raise Exception(
                    f"Failed to create ClusterIP service {service_name}: {e}"
                )  # noqa

    def create_ingress_resource(
        self,
        namespace: str,
        ingress_name: str,
        service_name: str,
        path_prefix: str,
        service_port: int = 8080,
        ingress_class: str = "nginx",
    ) -> None:
        """
        Create an Ingress resource for path-based routing to the shared NGINX controller.

        This enables routing like: http://<LB_IP>/airflow-test-123/* -> service:8080/*

        :param str namespace: Target namespace
        :param str ingress_name: Name of the Ingress resource
        :param str service_name: Backend service name
        :param str path_prefix: URL path prefix (e.g., "/airflow-test-123")
        :param int service_port: Backend service port (default: 8080)
        :param str ingress_class: Ingress class (default: "nginx")
        """
        self._ensure_api_attributes_set()

        # Construct regex path for NGINX rewrite
        # Example: /airflow-test-123(/|$)(.*) matches:
        #   - /airflow-test-123/
        #   - /airflow-test-123/api/v1/dags
        # And captures the part after the prefix for rewriting
        regex_path = f"{path_prefix}(/|$)(.*)"

        ingress_manifest = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": ingress_name,
                "namespace": namespace,
                "labels": {
                    "app": "airflow",
                    "test-id": namespace,
                },
                "annotations": {
                    # Rewrite target: strips the path prefix before forwarding
                    "nginx.ingress.kubernetes.io/rewrite-target": "/$2",
                    # Enable regex path matching
                    "nginx.ingress.kubernetes.io/use-regex": "true",
                    # Increase timeouts for long-running operations
                    "nginx.ingress.kubernetes.io/proxy-connect-timeout": "3600",
                    "nginx.ingress.kubernetes.io/proxy-send-timeout": "3600",
                    "nginx.ingress.kubernetes.io/proxy-read-timeout": "3600",
                    # Preserve X-Forwarded headers
                    "nginx.ingress.kubernetes.io/proxy-body-size": "0",
                },
            },
            "spec": {
                "ingressClassName": ingress_class,
                "rules": [
                    {
                        "http": {
                            "paths": [
                                {
                                    "path": regex_path,
                                    "pathType": "Prefix",
                                    "backend": {
                                        "service": {
                                            "name": service_name,
                                            "port": {"number": service_port},
                                        }
                                    },
                                }
                            ]
                        }
                    }
                ],
            },
        }

        networking_api = k8s_client_sdk.NetworkingV1Api()

        try:
            networking_api.create_namespaced_ingress(
                namespace=namespace, body=ingress_manifest
            )
            print(f"✅ Created Ingress: {ingress_name} → {service_name}:{service_port}")
            print(f"   📍 Path routing: {regex_path}")
        except k8s_client_sdk.ApiException as e:
            if e.status == 409:
                print(f"⏭️  Ingress {ingress_name} already exists in {namespace}")
            else:
                raise Exception(f"Failed to create Ingress {ingress_name}: {e}")  # noqa

    def cleanup_shared_ingress_controller(
        self, namespace: str = "ingress-system"
    ) -> None:
        """
        Delete the shared NGINX Ingress Controller (session teardown).

        This will delete:
        - The entire ingress namespace (which cascades to all resources)
        - ClusterRole and ClusterRoleBinding (cluster-scoped resources)

        :param str namespace: The ingress namespace to delete (default: ingress-system)
        """
        self._ensure_api_attributes_set()

        print("🧹 Cleaning up shared NGINX Ingress Controller...")

        # Delete ClusterRole and ClusterRoleBinding (cluster-scoped)
        rbac_api = k8s_client_sdk.RbacAuthorizationV1Api()
        networking_api = k8s_client_sdk.NetworkingV1Api()

        try:
            rbac_api.delete_cluster_role(name="nginx-ingress-role")
            print("  ✓ Deleted ClusterRole: nginx-ingress-role")
        except k8s_client_sdk.ApiException as e:
            if e.status != 404:
                print(f"  ⚠️  Error deleting ClusterRole: {e}")

        try:
            rbac_api.delete_cluster_role_binding(name="nginx-ingress-role-binding")
            print("  ✓ Deleted ClusterRoleBinding: nginx-ingress-role-binding")
        except k8s_client_sdk.ApiException as e:
            if e.status != 404:
                print(f"  ⚠️  Error deleting ClusterRoleBinding: {e}")

        try:
            networking_api.delete_ingress_class(name="nginx")
            print("  ✓ Deleted IngressClass: nginx")
        except k8s_client_sdk.ApiException as e:
            if e.status != 404:
                print(f"  ⚠️  Error deleting IngressClass: {e}")

        # Delete the namespace (cascades to all namespaced resources)
        try:
            self.k8s_core_api.delete_namespace(name=namespace)
            print(f"  ✓ Deleted namespace: {namespace} (LoadBalancer will be released)")
            print("✅ Shared NGINX Ingress Controller cleanup complete")
        except k8s_client_sdk.ApiException as e:
            if e.status == 404:
                print(f"  ⏭️  Namespace {namespace} not found")
            else:
                print(f"  ⚠️  Error deleting namespace {namespace}: {e}")
