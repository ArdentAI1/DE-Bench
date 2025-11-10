import os
from azure.identity import ClientSecretCredential
import time
from kubernetes import client as k8s_client_sdk
from kubernetes import config as k8s_config  # Rename this import
import yaml
from azure.mgmt.containerservice import ContainerServiceClient
import pexpect



class Kubernetes:
    def __init__(self, test_id: str, provider: str = "AZURE"):
        self.provider = provider
        self.cloud_provider_client = self.get_cloud_provider_client()
        self.test_id = test_id

    def get_cloud_provider_client(self):
        if self.provider not in ["AZURE"]:
            raise Exception("Provider not supported")

        azure_client = None

        if self.provider == "AZURE":
            azure_credential = ClientSecretCredential(
                client_id=os.getenv("AZURE_CLIENT_ID"),
                client_secret=os.getenv("AZURE_CLIENT_SECRET"),
                tenant_id=os.getenv("AZURE_TENANT_ID"),
            )
            ## CONSIDER: rename this `azure_container_client`
            azure_client = ContainerServiceClient(
                azure_credential, os.getenv("AZURE_SUBSCRIPTION_ID")
            )
            

        if not azure_client:
            raise Exception("Failed to create Azure client")

        return azure_client

    def get_k8s_client(self, cloud_provider_client):
        try:
            kubeconfig = (
                cloud_provider_client.managed_clusters.list_cluster_user_credentials(
                    os.getenv("AKS_RESOURCE_GROUP"), os.getenv("AKS_CLUSTER_NAME")
                )
                .kubeconfigs[0]
                .value
            )
        except Exception as e:
            raise Exception(f"Failed to load container credentials")

        # Convert bytearray to string
        kubeconfig_str = kubeconfig.decode("utf-8")

        kubeconfig_path = os.path.expanduser("~/.kube/config")
        os.makedirs(os.path.dirname(kubeconfig_path), exist_ok=True)
        with open(kubeconfig_path, "w") as f:
            f.write(kubeconfig_str)

        # Set the KUBECONFIG environment variable
        os.environ["KUBECONFIG"] = kubeconfig_path

        config_test = k8s_config.load_kube_config_from_dict(
            yaml.safe_load(kubeconfig_str)
        )

        k8s_api_client = k8s_client_sdk.ApiClient()

        v1 = k8s_client_sdk.CoreV1Api(k8s_api_client)

        api_instance = k8s_client_sdk.BatchV1Api()

        return api_instance

    def create_job_in_namespace_with_volume_mount(
        self, api_instance, shareName, jobID, mode
    ):
        if mode == "Claude_Code":
            job_manifest = f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: {jobID}
  labels:
    user-id: "{self.test_id}"
spec:
  activeDeadlineSeconds: 43200
  template:
    metadata:
      labels:
        user-id: "{self.test_id}"
    spec:
      containers:
      - name: custom-container
        image: {os.getenv("AKS_IMAGE_NAME")}
        env:
        - name: AWS_ACCESS_KEY_ID
          value: "{os.getenv('AWS_ACCESS_KEY_ID_CLAUDE', '')}"
        - name: AWS_SECRET_ACCESS_KEY
          value: "{os.getenv('AWS_SECRET_ACCESS_KEY_CLAUDE', '')}"
        - name: AWS_REGION
          value: "{os.getenv('AWS_REGION_CLAUDE', 'us-east-1')}"
        - name: CLAUDE_CODE_USE_BEDROCK
          value: "1"
        - name: IS_SANDBOX
          value: "1"
        volumeMounts:
        - name: azure-file-share
          mountPath: /app
      volumes:
      - name: azure-file-share
        azureFile:
          secretName: azure-secret
          shareName: {shareName}
          readOnly: false
      restartPolicy: Never
  backoffLimit: 4
"""
        elif mode == "OpenAI_Codex":
            job_manifest = f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: {jobID}
  labels:
    user-id: "{self.test_id}"
spec:
  activeDeadlineSeconds: 43200
  template:
    metadata:
      labels:
        user-id: "{self.test_id}"
    spec:
      containers:
      - name: custom-container
        image: {os.getenv("AKS_IMAGE_NAME")}
        env:
        - name: OPENAI_API_KEY
          value: "{os.getenv('OPENAI_API_KEY', '')}"
        - name: IS_SANDBOX
          value: "1"
        volumeMounts:
        - name: azure-file-share
          mountPath: /app
      volumes:
      - name: azure-file-share
        azureFile:
          secretName: azure-secret
          shareName: {shareName}
          readOnly: false
      restartPolicy: Never
  backoffLimit: 4
"""
        job_object = yaml.safe_load(job_manifest)

        try:
            api_response = api_instance.create_namespaced_job(
                body=job_object, namespace="default"
            )
        except k8s_client_sdk.ApiException as e:
            print("Exception when creating Job: %s" % e)
            raise Exception(f"Failed to create Kubernetes job '{jobID}': {e}") from e

    def wait_for_pod_to_be_avialable_and_get_name(
        self, api_instance, job_name
    ):
        max_retries = 10
        retry_interval = 2  # seconds
        pod_name = None

        k8s_api_client = k8s_client_sdk.ApiClient()
        core_v1 = k8s_client_sdk.CoreV1Api(k8s_api_client)

        for _ in range(max_retries):
            try:
                job = api_instance.read_namespaced_job_status(
                    name=job_name, namespace="default"
                )

                # Check if the job has any active pods
                if job.status.active is not None and job.status.active > 0:

                    # Get the pod associated with this job
                    pods = core_v1.list_namespaced_pod(
                        namespace="default", label_selector=f"job-name={job_name}"
                    )
                    for pod in pods.items:
                        if pod.status.phase == "Pending":
                            time.sleep(retry_interval)
                            break
                    pod_name = pod.metadata.name
                    break
                else:
                    time.sleep(retry_interval)
            except k8s_client_sdk.ApiException as e:
                time.sleep(retry_interval)
        else:
            error_msg = f"Kubernetes pod failed to start within {max_retries * retry_interval} seconds for job '{job_name}'"
            raise Exception(error_msg)
        
        return pod_name
    
    def run_terminal_command_in_pod(self, pod_name, command):
        child = pexpect.spawn(
            f"kubectl exec -it {pod_name} -- /bin/sh", timeout=None, maxread=20000
        )
        child.expect("#")
        child.sendline(command)
        child.expect("#")
        output = child.before.decode("utf-8")
        child.sendline("exit")
        return output
