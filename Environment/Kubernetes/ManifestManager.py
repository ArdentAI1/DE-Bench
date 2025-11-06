"""
POC to generate and apply a new manifest file for AKS that creates a namespace, a pod, and a LoadBalancer
"""

import argparse
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
spec:
  backoffLimit: 4
  activeDeadlineSeconds: 3600
  template:
    metadata:
      labels:
        app: airflow
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
      restartPolicy: Never

---
apiVersion: v1
kind: Service
metadata:
  name: NAMESPACE_VALUE-service
  namespace: NAMESPACE_VALUE
  labels:
    app: airflow
spec:
  type: LoadBalancer
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
            azure_credential, os.getenv("AZURE_SUBSCRIPTION_ID")  # type: ignore
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
            endpoint=endpoint,
            credential=azure_credential
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
        max_attempts = 60  # Wait up to 60 seconds (1 second per attempt)
        for attempt in range(max_attempts):
            try:
                service = self.k8s_core_api.read_namespaced_service(
                    name=f"{namespace}-service", namespace=namespace
                )
                if service.status.load_balancer.ingress:
                    external_ip = service.status.load_balancer.ingress[0].ip
                    if external_ip:
                        print(f"Service {namespace}-service has external IP: {external_ip}")
                        return external_ip
                print(f"Waiting for external IP for service {namespace}-service... (attempt {attempt + 1}/{max_attempts})")
                time.sleep(1)
            except k8s_client_sdk.ApiException as e:
                if e.status != 404:  # 404 is expected if service doesn't exist yet
                    print(f"Error fetching service {namespace}-service: {e}")
                time.sleep(1)
        
        print(f"Failed to get external IP for service {namespace}-service after {max_attempts} attempts")
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


if __name__ == "__main__":
    k8s_manager = KubernetesManifestManager(provider="AZURE")
    test = k8s_manager.delete_repo_from_acr(acr_name=os.getenv("AZURE_ACR_NAME"), repo_name="hello-universe-pipeline-test-1762410349-c5b15c20")
    if test:
        print(f"✅ Successfully deleted repository hello-universe-pipeline-test-1762410349-c5b15c20 from ACR {os.getenv('AZURE_ACR_NAME')}")
    else:
        print(f"❌ Failed to delete repository hello-universe-pipeline-test-1762410349-c5b15c20 from ACR {os.getenv('AZURE_ACR_NAME')}")