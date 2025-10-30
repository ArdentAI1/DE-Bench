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
    user-id: "NAMESPACE_VALUE"
spec:
  activeDeadlineSeconds: 43200
  template:
    metadata:
      labels:
        user-id: "NAMESPACE_VALUE"
    spec:
      containers:
      - name: custom-container
        image: CONTAINER_NAME
        ports:
        - containerPort: 8080
          name: airflow-web
        env:
        - name: IS_SANDBOX
          value: "1"
      restartPolicy: Never
  backoffLimit: 4

---
apiVersion: v1
kind: Service
metadata:
  name: NAMESPACE_VALUE-service
  namespace: NAMESPACE_VALUE
  labels:
    user-id: "NAMESPACE_VALUE"
spec:
  type: LoadBalancer
  ports:
  - port: 8080
    targetPort: 8080
    protocol: TCP
    name: airflow-web
  selector:
    user-id: "NAMESPACE_VALUE"
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
        for _ in range(10):
            try:
                service = self.k8s_core_api.read_namespaced_service(
                    name=f"{namespace}-service", namespace=namespace
                )
                if service.status.load_balancer.ingress:
                    external_ip = service.status.load_balancer.ingress[0].ip
                    print(f"Service {namespace}-service has external IP: {external_ip}")
                    break
                else:
                    print(f"Waiting for external IP for service {namespace}-service...")
                    time.sleep(1)
            except k8s_client_sdk.ApiException as e:
                print(f"Error fetching service {namespace}-service: {e}")

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


def profile(number_of_instances: int, container: str):
    """
    Profile the manifest generation and application process

    :param int number_of_instances: Number of instances to create for profiling
    :param str container: The container image to use for odd-numbered namespaces
    """
    namespaces = []
    # generate random names
    for i in range(number_of_instances):
        namespace_name = f"profile-namespace-{i + 1}"
        namespaces.append(namespace_name)
    start_time = time.time()
    k8s_manager = KubernetesManifestManager()
    print(
        f"Initialized KubernetesManifestManager in {time.time() - start_time:.2f} seconds"
    )
    for ns in namespaces:
        # see if the namespace ends with an even number
        ns_split = ns.split("-")
        last_number = int(ns_split[-1])
        if last_number % 2 == 0:
            manifest = k8s_manager.generate_manifest(
                namespace=ns,
                container="airflowstates-cudfbgfvekd7f0at.azurecr.io/hello-world-fail:base",
            )
        else:
            manifest = k8s_manager.generate_manifest(namespace=ns, container=container)
        print(f"Generated manifest in {time.time() - start_time:.2f} seconds")
        file_path = f"{ns}-manifest.yml"
        with open(file_path, "w") as f:
            f.write(manifest)
        print(f"Manifest generated and saved to: {file_path}")
        print(f"Saved manifest in {time.time() - start_time:.2f} seconds")
        k8s_manager.apply_manifest(manifest)
        print(
            f"Manifest applied successfully for namespace: {ns} in {time.time() - start_time:.2f} seconds"
        )
        if external_ip := k8s_manager.get_service_external_ip(namespace=ns):
            print(f"External IP for namespace {ns}: {external_ip}")
            print(
                f"Retrieved external IP in {time.time() - start_time:.2f} seconds"
            )
        else:
            print(f"Failed to retrieve external IP for namespace {ns}")
        # use requests to hit the external IP and port 8080 to verify the pod is running
        if external_ip and k8s_manager.verify_pod_health(external_ip):
            print(f"Pod in namespace {ns} is healthy")
            print(f"Verified pod health in {time.time() - start_time:.2f} seconds")
        else:
            print(
                f"Pod in namespace {ns} is not healthy or external IP not available"
            )
    print(
        f"Total time taken: {time.time() - start_time:.2f} seconds for {len(namespaces)} namespace(s)"
    )
    print(
        f"Time per namespace: {(time.time() - start_time) / len(namespaces):.2f} seconds"
    )


def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(
        description="Kubernetes Manifest Manager - Generate and/or apply Kubernetes manifests"
    )

    subparsers = parser.add_subparsers(
        dest="action", help="Action to perform", required=True
    )

    # Generate manifest subcommand
    generate_parser = subparsers.add_parser(
        "generate", help="Generate a manifest and save it to a file"
    )
    generate_parser.add_argument("namespace", type=str, help="The namespace to create")
    generate_parser.add_argument(
        "--container",
        type=str,
        help="The container image to use",
        default="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base",
    )
    generate_parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path for the manifest",
        required=True,
    )

    # Apply manifest subcommand
    apply_parser = subparsers.add_parser(
        "apply", help="Apply an existing manifest file to the cluster"
    )
    apply_parser.add_argument(
        "manifest_file", type=str, help="Path to the manifest file to apply"
    )

    # Generate and apply manifest subcommand
    generate_apply_parser = subparsers.add_parser(
        "generate-and-apply", help="Generate a manifest and apply it to the cluster"
    )
    generate_apply_parser.add_argument(
        "namespace", type=str, help="The namespace to create"
    )
    generate_apply_parser.add_argument(
        "--container",
        type=str,
        help="The container image to use",
        default="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base",
    )
    generate_apply_parser.add_argument(
        "--save",
        "-s",
        type=str,
        help="Optional: save the generated manifest to this file path",
        default=None,
    )

    # Reapply job subcommand
    reapply_parser = subparsers.add_parser(
        "reapply-job",
        help="Reapply a Job by deleting and recreating it with a new container (handles immutable Job specs)",
    )
    reapply_parser.add_argument(
        "namespace", type=str, help="The namespace where the Job exists"
    )
    reapply_parser.add_argument(
        "container", type=str, help="The new container image to use"
    )

    args = parser.parse_args()

    manager = KubernetesManifestManager()

    if args.action == "generate":
        # Generate manifest and save to file
        manifest = manager.generate_manifest(
            namespace=args.namespace, container=args.container
        )
        with open(args.output, "w") as f:
            f.write(manifest)
        print(f"Manifest generated and saved to: {args.output}")

    elif args.action == "apply":
        # Apply existing manifest file
        with open(args.manifest_file, "r") as f:
            manifest = f.read()
        manager.apply_manifest(manifest)
        print(f"\nManifest from {args.manifest_file} applied successfully")

    elif args.action == "generate-and-apply":
        # Generate and apply manifest
        manifest = manager.generate_manifest(
            namespace=args.namespace, container=args.container
        )

        # Optionally save the manifest
        if args.save:
            with open(args.save, "w") as f:
                f.write(manifest)
            print(f"Manifest saved to: {args.save}")

        manager.apply_manifest(manifest)
        print(f"\nManifest applied successfully for namespace: {args.namespace}")

    elif args.action == "reapply-job":
        # Reapply job with new container
        manager.reapply_job(namespace=args.namespace, container=args.container)
        print(f"\nJob reapplied successfully for namespace: {args.namespace}")


if __name__ == "__main__":
    main()
    # profile(number_of_instances=2, container="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base")
    # print("Profile complete")
