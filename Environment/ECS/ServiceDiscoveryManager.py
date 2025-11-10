"""
ECS Service Discovery Manager - Fast deployments with stable DNS endpoints
Solves the AKS LoadBalancer queue bottleneck by using AWS Cloud Map for service discovery
"""

import os
import time
from typing import Optional, Dict, Any

from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Import the base ECS manager
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Environment.ECS.ManifestManager import ECSManifestManager

import boto3
import argparse


class ECSServiceDiscoveryManager(ECSManifestManager):
    """
    Extended ECS Manager with Service Discovery for stable DNS endpoints

    Key features:
    - Stable DNS names that persist across task replacements
    - Fast deployment (~60s, no load balancer queue)
    - Supports 10+ simultaneous deployments
    - Format: <namespace>.de-bench.local:8080
    """

    def __init__(self, provider: str = "AWS"):
        super().__init__(provider)
        self.servicediscovery_client = None
        self.namespace_id = None
        self.namespace_name = "de-bench.local"
        self._setup_service_discovery()

    def _setup_service_discovery(self):
        """Setup AWS Cloud Map for service discovery"""

        # Get session from parent class credentials
        access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("ACCESS_KEY_ID_AWS")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv(
            "SECRET_ACCESS_KEY_AWS"
        )

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=self.region,
        )

        self.servicediscovery_client = session.client("servicediscovery")

        # Create or get private DNS namespace
        self._ensure_namespace_exists()

    def _ensure_namespace_exists(self):
        """Ensure the private DNS namespace exists for service discovery"""
        try:
            # List existing namespaces
            response = self.servicediscovery_client.list_namespaces()

            for ns in response.get("Namespaces", []):
                if ns["Name"] == self.namespace_name and ns["Type"] == "DNS_PRIVATE":
                    self.namespace_id = ns["Id"]
                    print(f"Using existing Cloud Map namespace: {self.namespace_name}")
                    return

            # Create new namespace if doesn't exist
            print(f"Creating Cloud Map namespace: {self.namespace_name}")
            response = self.servicediscovery_client.create_private_dns_namespace(
                Name=self.namespace_name,
                Vpc=self.vpc_id,
                Description="DE-Bench service discovery namespace",
            )

            # Wait for namespace creation
            operation_id = response["OperationId"]
            self._wait_for_operation(operation_id)

            # Get namespace ID
            response = self.servicediscovery_client.list_namespaces()
            for ns in response.get("Namespaces", []):
                if ns["Name"] == self.namespace_name:
                    self.namespace_id = ns["Id"]
                    print(
                        f"Created Cloud Map namespace: {self.namespace_name} (ID: {self.namespace_id})"
                    )
                    return

        except ClientError as e:
            raise Exception(f"Failed to setup service discovery namespace: {e}")

    def _wait_for_operation(self, operation_id: str, timeout: int = 60):
        """Wait for a Cloud Map operation to complete"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.servicediscovery_client.get_operation(
                    OperationId=operation_id
                )

                status = response["Operation"]["Status"]

                if status == "SUCCESS":
                    return
                elif status == "FAIL":
                    raise Exception(
                        f"Operation failed: {response['Operation'].get('ErrorMessage')}"
                    )

                time.sleep(2)
            except ClientError as e:
                raise Exception(f"Error checking operation status: {e}")

        raise Exception(f"Operation timed out after {timeout} seconds")

    def create_service_discovery(self, namespace: str) -> str:
        """
        Create a Cloud Map service for service discovery

        :param str namespace: The service name (will be accessible as <namespace>.de-bench.local)
        :return: Service discovery service ID
        """
        try:
            # Check if service already exists
            try:
                services = self.servicediscovery_client.list_services(
                    Filters=[
                        {
                            "Name": "NAMESPACE_ID",
                            "Values": [self.namespace_id],
                            "Condition": "EQ",
                        }
                    ]
                )

                for svc in services.get("Services", []):
                    if svc["Name"] == namespace:
                        print(
                            f"Using existing service discovery: {namespace}.{self.namespace_name}"
                        )
                        return svc["Id"]
            except Exception:
                pass

            # Create new service
            print(f"Creating service discovery: {namespace}.{self.namespace_name}")
            response = self.servicediscovery_client.create_service(
                Name=namespace,
                NamespaceId=self.namespace_id,
                Description=f"Service discovery for {namespace}",
                DnsConfig={
                    "NamespaceId": self.namespace_id,
                    "DnsRecords": [{"Type": "A", "TTL": 60}],
                    "RoutingPolicy": "MULTIVALUE",
                },
                HealthCheckCustomConfig={"FailureThreshold": 1},
            )

            service_id = response["Service"]["Id"]
            print(f"Created service discovery service ID: {service_id}")
            return service_id

        except ClientError as e:
            raise Exception(f"Failed to create service discovery: {e}")

    def deploy_with_service_discovery(
        self, namespace: str, container_image: str, desired_count: int = 1
    ) -> Dict[str, Any]:
        """
        Deploy ECS service with service discovery (stable DNS, no load balancer)

        :param str namespace: Unique identifier for this deployment
        :param str container_image: The container image to use
        :param int desired_count: Number of tasks to run
        :return: Deployment details including stable DNS name
        """
        # Register task definition
        task_def_arn = self.register_task_definition(namespace, container_image)

        # Create service discovery
        service_discovery_id = self.create_service_discovery(namespace)

        # Create or update ECS service with service discovery
        service_name = f"{namespace}-service"

        # Check if service exists
        try:
            existing_services = self.ecs_client.describe_services(
                cluster=self.cluster_name, services=[service_name]
            )

            service_exists = (
                existing_services["services"]
                and existing_services["services"][0]["status"] != "INACTIVE"
            )
        except ClientError:
            service_exists = False

        service_config = {
            "cluster": self.cluster_name,
            "serviceName": service_name,
            "taskDefinition": task_def_arn,
            "desiredCount": desired_count,
            "launchType": "FARGATE",
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": self.subnet_ids,
                    "securityGroups": self.security_group_ids,
                    "assignPublicIp": "ENABLED",
                }
            },
            "serviceRegistries": [
                {
                    "registryArn": f"arn:aws:servicediscovery:{self.region}:{self._get_account_id()}:service/{service_discovery_id}"
                }
            ],
        }

        try:
            if service_exists:
                # Update existing service
                print(f"Updating existing service: {service_name}")
                response = self.ecs_client.update_service(
                    cluster=self.cluster_name,
                    service=service_name,
                    taskDefinition=task_def_arn,
                    desiredCount=desired_count,
                    networkConfiguration=service_config["networkConfiguration"],
                )
            else:
                # Create new service
                print(f"Creating new service: {service_name}")
                response = self.ecs_client.create_service(**service_config)

            service = response["service"]

            # Wait for service to stabilize
            print("Waiting for service to become stable...")
            time.sleep(5)  # Give it a moment to register

            # Get the stable DNS name
            dns_name = f"{namespace}.{self.namespace_name}"

            result = {
                "namespace": namespace,
                "task_definition_arn": task_def_arn,
                "service": service,
                "service_discovery_id": service_discovery_id,
                "dns_name": dns_name,
                "endpoint": f"http://{dns_name}:8080",
                "note": "DNS resolution works from within the VPC. Use public IP for external access.",
            }

            # Also get public IP for external access
            public_ip = self._get_service_public_ip(service_name)
            if public_ip:
                result["public_ip"] = public_ip
                result["public_endpoint"] = f"http://{public_ip}:8080"

            return result

        except ClientError as e:
            raise Exception(f"Failed to create/update service: {e}")

    def _get_account_id(self) -> str:
        """Get AWS account ID"""

        sts = boto3.client("sts")
        return sts.get_caller_identity()["Account"]

    def _get_service_public_ip(
        self, service_name: str, max_attempts: int = 30
    ) -> Optional[str]:
        """Get public IP of the first running task in the service"""
        for attempt in range(max_attempts):
            try:
                # List tasks in service
                tasks_response = self.ecs_client.list_tasks(
                    cluster=self.cluster_name,
                    serviceName=service_name,
                    desiredStatus="RUNNING",
                )

                if not tasks_response.get("taskArns"):
                    if attempt % 5 == 0:
                        print(
                            f"Waiting for tasks to start... ({attempt}/{max_attempts})"
                        )
                    time.sleep(2)
                    continue

                # Get task details
                task_arn = tasks_response["taskArns"][0]
                return self.get_task_public_ip(task_arn, max_attempts=10)

            except ClientError as e:
                print(f"Error getting public IP: {e}")
                time.sleep(2)

        print("Warning: Could not get public IP")
        return None

    def delete_service_discovery(self, namespace: str) -> bool:
        """Delete the service discovery service"""
        try:
            # Get service ID
            services = self.servicediscovery_client.list_services(
                Filters=[
                    {
                        "Name": "NAMESPACE_ID",
                        "Values": [self.namespace_id],
                        "Condition": "EQ",
                    }
                ]
            )

            service_id = None
            for svc in services.get("Services", []):
                if svc["Name"] == namespace:
                    service_id = svc["Id"]
                    break

            if not service_id:
                print(f"Service discovery {namespace} not found")
                return False

            # Delete service
            self.servicediscovery_client.delete_service(Id=service_id)
            print(f"Deleted service discovery: {namespace}.{self.namespace_name}")
            return True

        except ClientError as e:
            print(f"Error deleting service discovery: {e}")
            return False

    def cleanup_deployment(
        self, namespace: str, cleanup_infrastructure: bool = False
    ) -> bool:
        """
        Complete cleanup including service discovery

        :param str namespace: Unique identifier for the deployment
        :param bool cleanup_infrastructure: If True, also cleanup created infrastructure
        :return: True if cleanup successful
        """
        print(f"Cleaning up deployment: {namespace}")

        # Delete ECS service
        self.delete_service(namespace)

        # Delete service discovery
        self.delete_service_discovery(namespace)

        # Stop any running tasks
        self.stop_tasks(namespace)

        # Deregister task definitions
        self.delete_task_definition(namespace)

        # Delete CloudWatch log group
        self.delete_log_group(namespace)

        # Optionally cleanup infrastructure
        if cleanup_infrastructure:
            self._cleanup_infrastructure()

        print(f"Cleanup completed for: {namespace}")
        return True


def main():
    """CLI entry point"""

    parser = argparse.ArgumentParser(
        description="ECS Service Discovery Manager - Fast deployments with stable DNS"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Deploy command
    deploy_parser = subparsers.add_parser(
        "deploy", help="Deploy with service discovery"
    )
    deploy_parser.add_argument("namespace", help="Unique namespace for deployment")
    deploy_parser.add_argument("--container", required=True, help="Container image")
    deploy_parser.add_argument("--count", type=int, default=1, help="Number of tasks")

    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up deployment")
    cleanup_parser.add_argument("namespace", help="Namespace to clean up")
    cleanup_parser.add_argument(
        "--infrastructure", action="store_true", help="Also cleanup infrastructure"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = ECSServiceDiscoveryManager()

    if args.command == "deploy":
        result = manager.deploy_with_service_discovery(
            namespace=args.namespace,
            container_image=args.container,
            desired_count=args.count,
        )
        print("\n" + "=" * 60)
        print("DEPLOYMENT COMPLETE!")
        print("=" * 60)
        print(f"Namespace: {result['namespace']}")
        print(f"Internal DNS: {result['dns_name']}")
        print(f"Internal Endpoint: {result['endpoint']}")
        if "public_ip" in result:
            print(f"Public IP: {result['public_ip']}")
            print(f"Public Endpoint: {result['public_endpoint']}")
        print("=" * 60)

    elif args.command == "cleanup":
        manager.cleanup_deployment(
            args.namespace, cleanup_infrastructure=args.infrastructure
        )


if __name__ == "__main__":
    main()
