"""
AWS ECS Manifest Manager for deploying Airflow containers to ECS Fargate
Automatically creates all required infrastructure (VPC, subnets, security groups, IAM roles, ECS cluster)
"""

import argparse
import json
import os
import time
import boto3
from typing import Optional, Dict, Any
import requests

from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


class ECSManifestManager:
    """Manages ECS task definitions, services, and deployments with automatic infrastructure creation"""

    def __init__(self, provider: str = "AWS"):
        self.provider = provider
        if self.provider != "AWS":
            raise NotImplementedError("Only AWS provider is supported")

        self.ecs_client = None
        self.ec2_client = None
        self.elbv2_client = None
        self.ecr_client = None
        self.logs_client = None
        self.iam_client = None

        # ECS Configuration from environment (simplified - only essentials required)
        self.cluster_name = os.getenv("DE_BENCH_ECS_CLUSTER_NAME")
        self.region = os.getenv("AWS_REGION", "us-east-1")

        # Optional: Use existing resources if provided
        self.existing_subnet_ids = (
            os.getenv("DE_BENCH_ECS_SUBNET_IDS", "").split(",")
            if os.getenv("DE_BENCH_ECS_SUBNET_IDS")
            else None
        )
        self.existing_security_group_ids = (
            os.getenv("DE_BENCH_ECS_SECURITY_GROUP_IDS", "").split(",")
            if os.getenv("DE_BENCH_ECS_SECURITY_GROUP_IDS")
            else None
        )
        self.existing_task_execution_role_arn = os.getenv(
            "DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN"
        )
        self.existing_task_role_arn = os.getenv("DE_BENCH_ECS_TASK_ROLE_ARN")

        # Resources that will be created/used
        self.vpc_id = None
        self.subnet_ids = []
        self.security_group_ids = []
        self.task_execution_role_arn = None
        self.task_role_arn = None
        self.internet_gateway_id = None
        self.route_table_id = None

        # Track what we created vs what was provided
        self.created_vpc = False
        self.created_subnets = []
        self.created_security_groups = []
        self.created_roles = []
        self.created_cluster = False
        self.created_internet_gateway = False
        self.created_route_table = False

        # Validate minimal configuration
        self._validate_configuration()

        # Initialize AWS clients
        self._initialize_clients()

        # Setup infrastructure
        self._setup_infrastructure()

    def _validate_configuration(self):
        """Validate required environment variables (only minimal requirements)"""
        required_vars = [
            "DE_BENCH_ECS_CLUSTER_NAME",
        ]

        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Required: cluster name, AWS credentials (ACCESS_KEY_ID_AWS, SECRET_ACCESS_KEY_AWS), and region"
            )

    def _initialize_clients(self):
        """Initialize AWS service clients"""
        # Use AWS credentials from environment or IAM role
        # Support both AWS_* and ACCESS_KEY_ID_AWS formats
        access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("ACCESS_KEY_ID_AWS")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv(
            "SECRET_ACCESS_KEY_AWS"
        )

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            region_name=self.region,
        )

        self.ecs_client = session.client("ecs")
        self.ec2_client = session.client("ec2")
        self.elbv2_client = session.client("elbv2")
        self.ecr_client = session.client("ecr")
        self.logs_client = session.client("logs")
        self.iam_client = session.client("iam")

    def _setup_infrastructure(self):
        """Setup all required infrastructure (VPC, subnets, security groups, IAM roles, ECS cluster)"""
        print("Setting up ECS infrastructure...")

        # 1. Setup VPC and networking
        if self.existing_subnet_ids and self.existing_subnet_ids[0]:
            print(f"Using existing subnets: {self.existing_subnet_ids}")
            self.subnet_ids = self.existing_subnet_ids
            self.vpc_id = self._get_vpc_id_from_subnet()
        else:
            print("Creating VPC and subnets...")
            self._create_vpc_and_subnets()

        # 2. Setup security groups
        if self.existing_security_group_ids and self.existing_security_group_ids[0]:
            print(f"Using existing security groups: {self.existing_security_group_ids}")
            self.security_group_ids = self.existing_security_group_ids
        else:
            print("Creating security group...")
            self._create_security_group()

        # 3. Setup IAM roles
        if self.existing_task_execution_role_arn:
            print(
                f"Using existing task execution role: {self.existing_task_execution_role_arn}"
            )
            self.task_execution_role_arn = self.existing_task_execution_role_arn
        else:
            print("Creating IAM task execution role...")
            self._create_task_execution_role()

        if self.existing_task_role_arn:
            self.task_role_arn = self.existing_task_role_arn
        # Task role is optional, don't create if not provided

        # 4. Setup ECS cluster
        self._ensure_cluster_exists()

        print("Infrastructure setup complete!")

    def _create_vpc_and_subnets(self):
        """Create VPC with public subnets in multiple AZs"""
        try:
            # Create VPC
            vpc_response = self.ec2_client.create_vpc(
                CidrBlock="10.0.0.0/16",
                TagSpecifications=[
                    {
                        "ResourceType": "vpc",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"de-bench-ecs-vpc-{self.cluster_name}",
                            },
                            {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                        ],
                    }
                ],
            )
            self.vpc_id = vpc_response["Vpc"]["VpcId"]
            self.created_vpc = True
            print(f"Created VPC: {self.vpc_id}")

            # Wait for VPC to be available
            self.ec2_client.get_waiter("vpc_available").wait(VpcIds=[self.vpc_id])

            # Enable DNS hostname support
            self.ec2_client.modify_vpc_attribute(
                VpcId=self.vpc_id, EnableDnsHostnames={"Value": True}
            )
            self.ec2_client.modify_vpc_attribute(
                VpcId=self.vpc_id, EnableDnsSupport={"Value": True}
            )

            # Create Internet Gateway
            igw_response = self.ec2_client.create_internet_gateway(
                TagSpecifications=[
                    {
                        "ResourceType": "internet-gateway",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"de-bench-ecs-igw-{self.cluster_name}",
                            },
                            {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                        ],
                    }
                ]
            )
            self.internet_gateway_id = igw_response["InternetGateway"][
                "InternetGatewayId"
            ]
            self.created_internet_gateway = True
            print(f"Created Internet Gateway: {self.internet_gateway_id}")

            # Attach Internet Gateway to VPC
            self.ec2_client.attach_internet_gateway(
                InternetGatewayId=self.internet_gateway_id, VpcId=self.vpc_id
            )

            # Get available AZs
            azs_response = self.ec2_client.describe_availability_zones(
                Filters=[{"Name": "state", "Values": ["available"]}]
            )
            azs = [az["ZoneName"] for az in azs_response["AvailabilityZones"]][
                :2
            ]  # Use first 2 AZs

            # Create public subnets in different AZs
            for i, az in enumerate(azs):
                subnet_response = self.ec2_client.create_subnet(
                    VpcId=self.vpc_id,
                    CidrBlock=f"10.0.{i}.0/24",
                    AvailabilityZone=az,
                    TagSpecifications=[
                        {
                            "ResourceType": "subnet",
                            "Tags": [
                                {
                                    "Key": "Name",
                                    "Value": f"de-bench-ecs-subnet-{i + 1}-{self.cluster_name}",
                                },
                                {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                            ],
                        }
                    ],
                )
                subnet_id = subnet_response["Subnet"]["SubnetId"]
                self.subnet_ids.append(subnet_id)
                self.created_subnets.append(subnet_id)
                print(f"Created subnet: {subnet_id} in {az}")

                # Enable auto-assign public IP
                self.ec2_client.modify_subnet_attribute(
                    SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True}
                )

            # Create route table
            rt_response = self.ec2_client.create_route_table(
                VpcId=self.vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "route-table",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"de-bench-ecs-rt-{self.cluster_name}",
                            },
                            {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                        ],
                    }
                ],
            )
            self.route_table_id = rt_response["RouteTable"]["RouteTableId"]
            self.created_route_table = True
            print(f"Created route table: {self.route_table_id}")

            # Create route to Internet Gateway
            self.ec2_client.create_route(
                RouteTableId=self.route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=self.internet_gateway_id,
            )

            # Associate route table with subnets
            for subnet_id in self.subnet_ids:
                self.ec2_client.associate_route_table(
                    RouteTableId=self.route_table_id, SubnetId=subnet_id
                )

        except ClientError as e:
            raise Exception(f"Failed to create VPC and subnets: {e}")

    def _create_security_group(self):
        """Create security group allowing traffic on port 8080 and HTTP (80)"""
        try:
            sg_response = self.ec2_client.create_security_group(
                GroupName=f"de-bench-ecs-sg-{self.cluster_name}",
                Description="Security group for DE-Bench ECS tasks",
                VpcId=self.vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "security-group",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"de-bench-ecs-sg-{self.cluster_name}",
                            },
                            {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                        ],
                    }
                ],
            )
            sg_id = sg_response["GroupId"]
            self.security_group_ids = [sg_id]
            self.created_security_groups.append(sg_id)
            print(f"Created security group: {sg_id}")

            # Allow inbound traffic on port 8080 (Airflow)
            self.ec2_client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpRanges": [
                            {
                                "CidrIp": "0.0.0.0/0",
                                "Description": "Airflow web interface",
                            }
                        ],
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 80,
                        "ToPort": 80,
                        "IpRanges": [
                            {"CidrIp": "0.0.0.0/0", "Description": "HTTP for ALB"}
                        ],
                    },
                ],
            )

            # Note: AWS automatically creates a default egress rule allowing all outbound traffic
            # No need to explicitly add it

        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidGroup.Duplicate":
                # Security group exists, try to find it
                sg_response = self.ec2_client.describe_security_groups(
                    Filters=[
                        {
                            "Name": "group-name",
                            "Values": [f"de-bench-ecs-sg-{self.cluster_name}"],
                        },
                        {"Name": "vpc-id", "Values": [self.vpc_id]},
                    ]
                )
                if sg_response["SecurityGroups"]:
                    sg_id = sg_response["SecurityGroups"][0]["GroupId"]
                    self.security_group_ids = [sg_id]
                    print(f"Using existing security group: {sg_id}")
                else:
                    raise
            else:
                raise Exception(f"Failed to create security group: {e}")

    def _create_task_execution_role(self):
        """Create IAM role for ECS task execution"""
        role_name = f"de-bench-ecs-execution-role-{self.cluster_name}"

        # Trust policy for ECS tasks
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        try:
            # Create role
            role_response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Execution role for DE-Bench ECS tasks",
                Tags=[
                    {"Key": "ManagedBy", "Value": "DE-Bench-ECS"},
                ],
            )
            self.task_execution_role_arn = role_response["Role"]["Arn"]
            self.created_roles.append(role_name)
            print(f"Created IAM role: {self.task_execution_role_arn}")

            # Attach AWS managed policy for ECS task execution
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
            )

            # Attach policy for ECR access
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
            )

            # Add inline policy for CloudWatch Logs (includes CreateLogGroup)
            logs_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        "Resource": "arn:aws:logs:*:*:*",
                    }
                ],
            }

            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName="CloudWatchLogsFullAccess",
                PolicyDocument=json.dumps(logs_policy),
            )
            print(f"Added CloudWatch Logs permissions to role")

            # Wait a moment for IAM to propagate
            time.sleep(10)

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                # Role exists, get its ARN
                role_response = self.iam_client.get_role(RoleName=role_name)
                self.task_execution_role_arn = role_response["Role"]["Arn"]
                print(f"Using existing IAM role: {self.task_execution_role_arn}")

                # Ensure the role has CloudWatch Logs permissions
                try:
                    logs_policy = {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "logs:CreateLogGroup",
                                    "logs:CreateLogStream",
                                    "logs:PutLogEvents",
                                ],
                                "Resource": "arn:aws:logs:*:*:*",
                            }
                        ],
                    }

                    self.iam_client.put_role_policy(
                        RoleName=role_name,
                        PolicyName="CloudWatchLogsFullAccess",
                        PolicyDocument=json.dumps(logs_policy),
                    )
                    print(f"Added/updated CloudWatch Logs permissions to existing role")
                except ClientError as policy_error:
                    print(
                        f"Warning: Could not add CloudWatch Logs policy to existing role: {policy_error}"
                    )
            else:
                raise Exception(f"Failed to create IAM role: {e}")

    def _ensure_cluster_exists(self):
        """Ensure ECS cluster exists, create if it doesn't"""
        try:
            # Check if cluster exists
            response = self.ecs_client.describe_clusters(clusters=[self.cluster_name])

            if response["clusters"] and response["clusters"][0]["status"] == "ACTIVE":
                print(f"Using existing ECS cluster: {self.cluster_name}")
            else:
                # Create cluster
                self.ecs_client.create_cluster(
                    clusterName=self.cluster_name,
                    tags=[
                        {"key": "ManagedBy", "value": "DE-Bench-ECS"},
                    ],
                )
                self.created_cluster = True
                print(f"Created ECS cluster: {self.cluster_name}")

        except ClientError as e:
            raise Exception(f"Failed to ensure cluster exists: {e}")

    def generate_task_definition(
        self,
        namespace: str,
        container_image: str,
        cpu: str = "1024",
        memory: str = "2048",
    ) -> Dict[str, Any]:
        """
        Generate ECS task definition

        :param str namespace: Unique identifier for this deployment (used as family name)
        :param str container_image: The container image to use
        :param str cpu: CPU units (256, 512, 1024, 2048, 4096)
        :param str memory: Memory in MB (512, 1024, 2048, 4096, 8192, etc.)
        :return: Task definition dictionary
        """
        task_definition = {
            "family": namespace,
            "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "cpu": cpu,
            "memory": memory,
            "executionRoleArn": self.task_execution_role_arn,
            "containerDefinitions": [
                {
                    "name": "airflow",
                    "image": container_image,
                    "essential": True,
                    "portMappings": [
                        {
                            "containerPort": 8080,
                            "protocol": "tcp",
                            "name": "airflow-web",
                        }
                    ],
                    "environment": [{"name": "IS_SANDBOX", "value": "1"}],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": f"/ecs/{namespace}",
                            "awslogs-region": self.region,
                            "awslogs-stream-prefix": "airflow",
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            "tags": [
                {"key": "Environment", "value": "DE-Bench"},
                {"key": "Namespace", "value": namespace},
            ],
        }

        # Add task role if specified
        if self.task_role_arn:
            task_definition["taskRoleArn"] = self.task_role_arn

        return task_definition

    def register_task_definition(
        self,
        namespace: str,
        container_image: str,
        cpu: str = "1024",
        memory: str = "2048",
    ) -> str:
        """
        Register a new task definition with ECS

        :param str namespace: Unique identifier for this deployment
        :param str container_image: The container image to use
        :param str cpu: CPU units
        :param str memory: Memory in MB
        :return: Task definition ARN
        """
        task_def = self.generate_task_definition(
            namespace, container_image, cpu, memory
        )

        try:
            response = self.ecs_client.register_task_definition(**task_def)
            task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
            print(f"Registered task definition: {task_def_arn}")
            return task_def_arn
        except ClientError as e:
            raise Exception(f"Failed to register task definition: {e}")

    def create_or_update_service(
        self,
        namespace: str,
        task_definition_arn: str,
        desired_count: int = 1,
        enable_load_balancer: bool = True,
        target_group_arn: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create or update an ECS service

        :param str namespace: Unique identifier for this deployment
        :param str task_definition_arn: ARN of the task definition
        :param int desired_count: Number of tasks to run
        :param bool enable_load_balancer: Whether to attach a load balancer
        :param str target_group_arn: ARN of the target group (if using load balancer)
        :return: Service details
        """
        service_name = f"{namespace}-service"

        # Check if service already exists
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

        # Prepare service configuration
        service_config = {
            "cluster": self.cluster_name,
            "serviceName": service_name,
            "taskDefinition": task_definition_arn,
            "desiredCount": desired_count,
            "launchType": "FARGATE",
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": self.subnet_ids,
                    "securityGroups": self.security_group_ids,
                    "assignPublicIp": "ENABLED",
                }
            },
            "tags": [
                {"key": "Environment", "value": "DE-Bench"},
                {"key": "Namespace", "value": namespace},
            ],
        }

        # Add load balancer configuration if specified
        if enable_load_balancer and target_group_arn:
            service_config["loadBalancers"] = [
                {
                    "targetGroupArn": target_group_arn,
                    "containerName": "airflow",
                    "containerPort": 8080,
                }
            ]
            service_config["healthCheckGracePeriodSeconds"] = 300

        try:
            if service_exists:
                # Update existing service
                print(f"Updating existing service: {service_name}")
                response = self.ecs_client.update_service(
                    cluster=self.cluster_name,
                    service=service_name,
                    taskDefinition=task_definition_arn,
                    desiredCount=desired_count,
                    networkConfiguration=service_config["networkConfiguration"],
                )
            else:
                # Create new service
                print(f"Creating new service: {service_name}")
                response = self.ecs_client.create_service(**service_config)

            return response["service"]
        except ClientError as e:
            raise Exception(f"Failed to create/update service: {e}")

    def run_task(
        self,
        namespace: str,
        task_definition_arn: str,
        wait_for_completion: bool = False,
    ) -> Dict[str, Any]:
        """
        Run a standalone ECS task (equivalent to Kubernetes Job)

        :param str namespace: Unique identifier for this deployment
        :param str task_definition_arn: ARN of the task definition
        :param bool wait_for_completion: Whether to wait for task to complete
        :return: Task details
        """
        try:
            response = self.ecs_client.run_task(
                cluster=self.cluster_name,
                taskDefinition=task_definition_arn,
                launchType="FARGATE",
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": self.subnet_ids,
                        "securityGroups": self.security_group_ids,
                        "assignPublicIp": "ENABLED",
                    }
                },
                tags=[
                    {"key": "Environment", "value": "DE-Bench"},
                    {"key": "Namespace", "value": namespace},
                ],
            )

            if not response["tasks"]:
                raise Exception("Failed to start task")

            task = response["tasks"][0]
            task_arn = task["taskArn"]
            print(f"Started task: {task_arn}")

            if wait_for_completion:
                self._wait_for_task_completion(task_arn)

            return task
        except ClientError as e:
            raise Exception(f"Failed to run task: {e}")

    def _wait_for_task_completion(self, task_arn: str, timeout: int = 3600):
        """Wait for a task to complete"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.ecs_client.describe_tasks(
                    cluster=self.cluster_name, tasks=[task_arn]
                )

                if not response["tasks"]:
                    print(f"Task {task_arn} not found")
                    return

                task = response["tasks"][0]
                last_status = task["lastStatus"]

                print(f"Task status: {last_status}")

                if last_status in ["STOPPED", "DEPROVISIONING"]:
                    stop_code = task.get("stopCode", "Unknown")
                    print(f"Task stopped with code: {stop_code}")
                    return

                time.sleep(10)
            except ClientError as e:
                print(f"Error checking task status: {e}")
                time.sleep(10)

        print(f"Task did not complete within {timeout} seconds")

    def create_load_balancer_and_target_group(
        self, namespace: str, vpc_id: Optional[str] = None, use_nlb: bool = True
    ) -> tuple[str, str]:
        """
        Create a Network Load Balancer (NLB) or Application Load Balancer (ALB) and Target Group

        :param str namespace: Unique identifier for this deployment
        :param str vpc_id: VPC ID (will be auto-detected if not provided)
        :param bool use_nlb: Use NLB instead of ALB (faster, simpler, TCP-based)
        :return: Tuple of (load_balancer_arn, target_group_arn)
        """
        lb_name = f"{namespace}-lb"[:32]  # LB names have 32 char limit
        tg_name = f"{namespace}-tg"[:32]

        # Get VPC ID if not provided
        if not vpc_id:
            vpc_id = self._get_vpc_id_from_subnet()

        # Create target group
        try:
            if use_nlb:
                # NLB uses TCP health checks - simpler and faster
                tg_response = self.elbv2_client.create_target_group(
                    Name=tg_name,
                    Protocol="TCP",
                    Port=8080,
                    VpcId=vpc_id,
                    TargetType="ip",
                    HealthCheckEnabled=True,
                    HealthCheckProtocol="TCP",  # TCP health check - just checks if port is open
                    HealthCheckIntervalSeconds=30,
                    HealthyThresholdCount=2,
                    UnhealthyThresholdCount=2,
                    Tags=[
                        {"Key": "Environment", "Value": "DE-Bench"},
                        {"Key": "Namespace", "Value": namespace},
                    ],
                )
            else:
                # ALB uses HTTP health checks with path
                tg_response = self.elbv2_client.create_target_group(
                    Name=tg_name,
                    Protocol="HTTP",
                    Port=8080,
                    VpcId=vpc_id,
                    TargetType="ip",
                    HealthCheckEnabled=True,
                    HealthCheckPath="/login",  # Airflow login page exists and doesn't require auth
                    HealthCheckIntervalSeconds=30,
                    HealthCheckTimeoutSeconds=10,  # Increased from 5s - Airflow can be slow
                    HealthyThresholdCount=2,
                    UnhealthyThresholdCount=3,
                    Matcher={"HttpCode": "200,302"},  # Allow redirects
                    Tags=[
                        {"Key": "Environment", "Value": "DE-Bench"},
                        {"Key": "Namespace", "Value": namespace},
                    ],
                )
            target_group_arn = tg_response["TargetGroups"][0]["TargetGroupArn"]
            print(
                f"Created {'NLB' if use_nlb else 'ALB'} target group: {target_group_arn}"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateTargetGroupName":
                # Target group already exists, get its ARN
                tg_response = self.elbv2_client.describe_target_groups(Names=[tg_name])
                target_group_arn = tg_response["TargetGroups"][0]["TargetGroupArn"]
                print(f"Using existing target group: {target_group_arn}")
            else:
                raise Exception(f"Failed to create target group: {e}")

        # Create load balancer
        try:
            lb_config = {
                "Name": lb_name,
                "Subnets": self.subnet_ids,
                "Scheme": "internet-facing",
                "Type": "network" if use_nlb else "application",
                "IpAddressType": "ipv4",
                "Tags": [
                    {"Key": "Environment", "Value": "DE-Bench"},
                    {"Key": "Namespace", "Value": namespace},
                ],
            }

            # NLB doesn't use security groups (uses target security groups instead)
            if not use_nlb:
                lb_config["SecurityGroups"] = self.security_group_ids

            lb_response = self.elbv2_client.create_load_balancer(**lb_config)
            load_balancer_arn = lb_response["LoadBalancers"][0]["LoadBalancerArn"]
            print(f"Created {'NLB' if use_nlb else 'ALB'}: {load_balancer_arn}")

            # Wait for load balancer to be active
            self._wait_for_load_balancer_active(load_balancer_arn)

        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateLoadBalancer":
                # Load balancer already exists, get its ARN
                lb_response = self.elbv2_client.describe_load_balancers(Names=[lb_name])
                load_balancer_arn = lb_response["LoadBalancers"][0]["LoadBalancerArn"]
                print(
                    f"Using existing {'NLB' if use_nlb else 'ALB'}: {load_balancer_arn}"
                )
            else:
                raise Exception(f"Failed to create load balancer: {e}")

        # Create listener
        try:
            listener_config = {
                "LoadBalancerArn": load_balancer_arn,
                "Protocol": "TCP" if use_nlb else "HTTP",
                "Port": 8080,  # Forward port 8080 to port 8080
                "DefaultActions": [
                    {"Type": "forward", "TargetGroupArn": target_group_arn}
                ],
            }

            self.elbv2_client.create_listener(**listener_config)
            print(f"Created {'TCP' if use_nlb else 'HTTP'} listener on port 8080")
        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateListener":
                print("Listener already exists")
            else:
                raise Exception(f"Failed to create listener: {e}")

        return load_balancer_arn, target_group_arn

    def _get_vpc_id_from_subnet(self) -> str:
        """Get VPC ID from the first subnet"""
        try:
            response = self.ec2_client.describe_subnets(SubnetIds=[self.subnet_ids[0]])
            return response["Subnets"][0]["VpcId"]
        except ClientError as e:
            raise Exception(f"Failed to get VPC ID: {e}")

    def _wait_for_load_balancer_active(
        self, load_balancer_arn: str, timeout: int = 300
    ):
        """Wait for load balancer to become active"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.elbv2_client.describe_load_balancers(
                    LoadBalancerArns=[load_balancer_arn]
                )

                state = response["LoadBalancers"][0]["State"]["Code"]
                print(f"Load balancer state: {state}")

                if state == "active":
                    print(
                        f"Load balancer is active after {int(time.time() - start_time)} seconds"
                    )
                    return

                time.sleep(10)
            except ClientError as e:
                print(f"Error checking load balancer status: {e}")
                time.sleep(10)

        raise Exception(f"Load balancer did not become active within {timeout} seconds")

    def generate_and_deploy(
        self,
        namespace: str,
        container_image: str,
        use_service: bool = True,
        enable_load_balancer: bool = True,
        use_nlb: bool = True,
    ) -> Dict[str, Any]:
        """
        Complete deployment workflow: register task, create/update service or run task

        :param str namespace: Unique identifier for this deployment
        :param str container_image: The container image to use
        :param bool use_service: If True, create a service; if False, run a standalone task
        :param bool enable_load_balancer: Whether to create a load balancer (only with service)
        :param bool use_nlb: Use NLB instead of ALB (faster, simpler, TCP-based)
        :return: Deployment details
        """
        # Register task definition
        task_def_arn = self.register_task_definition(namespace, container_image)

        result = {
            "namespace": namespace,
            "task_definition_arn": task_def_arn,
            "use_nlb": use_nlb,
        }

        if use_service:
            # Create load balancer if requested
            if enable_load_balancer:
                lb_arn, tg_arn = self.create_load_balancer_and_target_group(
                    namespace, use_nlb=use_nlb
                )
                result["load_balancer_arn"] = lb_arn
                result["target_group_arn"] = tg_arn
            else:
                tg_arn = None

            # Create or update service
            service = self.create_or_update_service(
                namespace,
                task_def_arn,
                enable_load_balancer=enable_load_balancer,
                target_group_arn=tg_arn,
            )
            result["service"] = service

            if enable_load_balancer:
                # Get load balancer DNS
                dns_name = self.get_load_balancer_dns(namespace)
                result["dns_name"] = dns_name
                # NLB uses port 8080, ALB uses port 80
                port = 8080 if use_nlb else 80
                result["base_url"] = (
                    f"http://{dns_name}:{port}" if use_nlb else f"http://{dns_name}"
                )
                result["port"] = port
        else:
            # Run standalone task
            task = self.run_task(namespace, task_def_arn)
            result["task"] = task

            # Get public IP of the task
            public_ip = self.get_task_public_ip(task["taskArn"])
            result["public_ip"] = public_ip
            if public_ip:
                result["base_url"] = f"http://{public_ip}:8080"

        return result

    def get_load_balancer_dns(self, namespace: str) -> Optional[str]:
        """
        Get the DNS name of the load balancer

        :param str namespace: Unique identifier for the deployment
        :return: DNS name or None
        """
        lb_name = f"{namespace}-lb"[:32]

        try:
            response = self.elbv2_client.describe_load_balancers(Names=[lb_name])
            if response["LoadBalancers"]:
                dns_name = response["LoadBalancers"][0]["DNSName"]
                print(f"Load balancer DNS: {dns_name}")
                return dns_name
        except ClientError as e:
            print(f"Error getting load balancer DNS: {e}")

        return None

    def get_task_public_ip(
        self, task_arn: str, max_attempts: int = 60
    ) -> Optional[str]:
        """
        Get the public IP of a running task

        :param str task_arn: ARN of the task
        :param int max_attempts: Maximum number of attempts to wait for IP
        :return: Public IP or None
        """
        for attempt in range(max_attempts):
            try:
                response = self.ecs_client.describe_tasks(
                    cluster=self.cluster_name, tasks=[task_arn]
                )

                if not response["tasks"]:
                    print(f"Task {task_arn} not found")
                    return None

                task = response["tasks"][0]

                # Check if task has network interface
                for attachment in task.get("attachments", []):
                    if attachment["type"] == "ElasticNetworkInterface":
                        for detail in attachment.get("details", []):
                            if detail["name"] == "networkInterfaceId":
                                eni_id = detail["value"]

                                # Get public IP from ENI
                                eni_response = (
                                    self.ec2_client.describe_network_interfaces(
                                        NetworkInterfaceIds=[eni_id]
                                    )
                                )

                                if eni_response["NetworkInterfaces"]:
                                    association = eni_response["NetworkInterfaces"][
                                        0
                                    ].get("Association", {})
                                    public_ip = association.get("PublicIp")

                                    if public_ip:
                                        print(f"Task public IP: {public_ip}")
                                        return public_ip

                # Print progress every 10 attempts
                if (attempt + 1) % 10 == 0:
                    print(
                        f"Waiting for task public IP... (attempt {attempt + 1}/{max_attempts})"
                    )

                time.sleep(5)
            except ClientError as e:
                print(f"Error getting task public IP: {e}")
                time.sleep(5)

        print(f"Failed to get task public IP after {max_attempts} attempts")
        return None

    def get_service_external_ip(self, namespace: str) -> Optional[str]:
        """
        Get the external endpoint for the service (load balancer DNS)

        :param str namespace: Unique identifier for the deployment
        :return: DNS name or public IP
        """
        # First try to get load balancer DNS
        dns_name = self.get_load_balancer_dns(namespace)
        if dns_name:
            return dns_name

        # If no load balancer, try to get task public IP
        service_name = f"{namespace}-service"
        try:
            response = self.ecs_client.describe_services(
                cluster=self.cluster_name, services=[service_name]
            )

            if response["services"] and response["services"][0]["status"] == "ACTIVE":
                # Get tasks for this service
                tasks_response = self.ecs_client.list_tasks(
                    cluster=self.cluster_name,
                    serviceName=service_name,
                    desiredStatus="RUNNING",
                )

                if tasks_response["taskArns"]:
                    task_arn = tasks_response["taskArns"][0]
                    return self.get_task_public_ip(task_arn)
        except ClientError as e:
            print(f"Error getting service endpoint: {e}")

        return None

    @staticmethod
    def verify_pod_health(endpoint: str, port: int = 80) -> bool:
        """
        Verify the health of the deployment by sending a request

        :param str endpoint: The endpoint (DNS or IP) to check
        :param int port: The port to connect to (default is 80 for ALB, 8080 for direct)
        :return: True if healthy, False otherwise
        """

        # Use /login for Airflow health check (exists and doesn't require auth)
        url = (
            f"http://{endpoint}:{port}/login"
            if port != 80
            else f"http://{endpoint}/login"
        )

        try:
            response = requests.get(url, timeout=10, allow_redirects=True)
            # Accept 200 (OK) or 302 (redirect) as healthy
            if response.status_code in [200, 302]:
                print(f"Service is healthy at {url} (status: {response.status_code})")
                return True
            else:
                print(
                    f"Health check failed with status code {response.status_code} at {url}"
                )
                return False
        except requests.RequestException as e:
            print(f"Error connecting to service at {url}: {e}")
            return False

    def delete_service(self, namespace: str) -> bool:
        """
        Delete an ECS service

        :param str namespace: Unique identifier for the deployment
        :return: True if deleted, False if service didn't exist
        """
        service_name = f"{namespace}-service"

        try:
            # First, scale service to 0
            self.ecs_client.update_service(
                cluster=self.cluster_name, service=service_name, desiredCount=0
            )
            print(f"Scaled service {service_name} to 0 tasks")

            # Then delete the service
            self.ecs_client.delete_service(
                cluster=self.cluster_name, service=service_name, force=True
            )
            print(f"Deleted service: {service_name}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ServiceNotFoundException":
                print(f"Service {service_name} not found")
                return False
            else:
                raise Exception(f"Failed to delete service {service_name}: {e}")

    def delete_task_definition(self, namespace: str) -> bool:
        """
        Deregister all task definitions for a family

        :param str namespace: Task definition family name
        :return: True if deleted, False if not found
        """
        try:
            # List all task definitions in the family
            response = self.ecs_client.list_task_definitions(
                familyPrefix=namespace, status="ACTIVE"
            )

            task_def_arns = response.get("taskDefinitionArns", [])

            if not task_def_arns:
                print(f"No active task definitions found for family: {namespace}")
                return False

            # Deregister each task definition
            for task_def_arn in task_def_arns:
                self.ecs_client.deregister_task_definition(taskDefinition=task_def_arn)
                print(f"Deregistered task definition: {task_def_arn}")

            return True
        except ClientError as e:
            print(f"Error deregistering task definitions: {e}")
            return False

    def delete_load_balancer(self, namespace: str) -> bool:
        """
        Delete load balancer and target group

        :param str namespace: Unique identifier for the deployment
        :return: True if deleted, False if not found
        """
        lb_name = f"{namespace}-lb"[:32]
        tg_name = f"{namespace}-tg"[:32]

        # Delete load balancer
        try:
            response = self.elbv2_client.describe_load_balancers(Names=[lb_name])
            if response["LoadBalancers"]:
                lb_arn = response["LoadBalancers"][0]["LoadBalancerArn"]
                self.elbv2_client.delete_load_balancer(LoadBalancerArn=lb_arn)
                print(f"Deleted load balancer: {lb_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "LoadBalancerNotFound":
                print(f"Error deleting load balancer: {e}")

        # Delete target group (must wait for LB to be deleted first)
        time.sleep(10)
        try:
            response = self.elbv2_client.describe_target_groups(Names=[tg_name])
            if response["TargetGroups"]:
                tg_arn = response["TargetGroups"][0]["TargetGroupArn"]
                self.elbv2_client.delete_target_group(TargetGroupArn=tg_arn)
                print(f"Deleted target group: {tg_name}")
                return True
        except ClientError as e:
            if e.response["Error"]["Code"] != "TargetGroupNotFound":
                print(f"Error deleting target group: {e}")

        return False

    def cleanup_deployment(
        self, namespace: str, cleanup_infrastructure: bool = False
    ) -> bool:
        """
        Complete cleanup of a deployment

        :param str namespace: Unique identifier for the deployment
        :param bool cleanup_infrastructure: If True, also cleanup created infrastructure (VPC, subnets, etc.)
        :return: True if cleanup successful
        """
        print(f"Cleaning up deployment: {namespace}")

        # Delete service
        self.delete_service(namespace)

        # Delete load balancer and target group
        self.delete_load_balancer(namespace)

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

    def _cleanup_infrastructure(self):
        """Cleanup infrastructure that was automatically created"""
        print("Cleaning up infrastructure...")

        # Delete security groups
        for sg_id in self.created_security_groups:
            try:
                self.ec2_client.delete_security_group(GroupId=sg_id)
                print(f"Deleted security group: {sg_id}")
            except ClientError as e:
                print(f"Error deleting security group {sg_id}: {e}")

        # Delete subnets
        for subnet_id in self.created_subnets:
            try:
                self.ec2_client.delete_subnet(SubnetId=subnet_id)
                print(f"Deleted subnet: {subnet_id}")
            except ClientError as e:
                print(f"Error deleting subnet {subnet_id}: {e}")

        # Delete route table
        if self.created_route_table and self.route_table_id:
            try:
                self.ec2_client.delete_route_table(RouteTableId=self.route_table_id)
                print(f"Deleted route table: {self.route_table_id}")
            except ClientError as e:
                print(f"Error deleting route table: {e}")

        # Detach and delete internet gateway
        if self.created_internet_gateway and self.internet_gateway_id:
            try:
                if self.vpc_id:
                    self.ec2_client.detach_internet_gateway(
                        InternetGatewayId=self.internet_gateway_id, VpcId=self.vpc_id
                    )
                self.ec2_client.delete_internet_gateway(
                    InternetGatewayId=self.internet_gateway_id
                )
                print(f"Deleted internet gateway: {self.internet_gateway_id}")
            except ClientError as e:
                print(f"Error deleting internet gateway: {e}")

        # Delete VPC
        if self.created_vpc and self.vpc_id:
            try:
                self.ec2_client.delete_vpc(VpcId=self.vpc_id)
                print(f"Deleted VPC: {self.vpc_id}")
            except ClientError as e:
                print(f"Error deleting VPC: {e}")

        # Delete IAM roles
        for role_name in self.created_roles:
            try:
                # Delete inline policies first
                inline_policies = self.iam_client.list_role_policies(RoleName=role_name)
                for policy_name in inline_policies.get("PolicyNames", []):
                    self.iam_client.delete_role_policy(
                        RoleName=role_name, PolicyName=policy_name
                    )
                    print(f"Deleted inline policy {policy_name} from role {role_name}")

                # Detach managed policies
                attached_policies = self.iam_client.list_attached_role_policies(
                    RoleName=role_name
                )
                for policy in attached_policies.get("AttachedPolicies", []):
                    self.iam_client.detach_role_policy(
                        RoleName=role_name, PolicyArn=policy["PolicyArn"]
                    )

                # Delete role
                self.iam_client.delete_role(RoleName=role_name)
                print(f"Deleted IAM role: {role_name}")
            except ClientError as e:
                print(f"Error deleting IAM role {role_name}: {e}")

        # Delete ECS cluster if we created it
        if self.created_cluster:
            try:
                self.ecs_client.delete_cluster(cluster=self.cluster_name)
                print(f"Deleted ECS cluster: {self.cluster_name}")
            except ClientError as e:
                print(f"Error deleting ECS cluster: {e}")

        print("Infrastructure cleanup complete!")

    def stop_tasks(self, namespace: str) -> bool:
        """
        Stop all running tasks for a namespace

        :param str namespace: Unique identifier for the deployment
        :return: True if tasks stopped
        """
        try:
            # List tasks with the namespace tag
            response = self.ecs_client.list_tasks(
                cluster=self.cluster_name, desiredStatus="RUNNING"
            )

            task_arns = response.get("taskArns", [])

            if not task_arns:
                print("No running tasks found")
                return True

            # Describe tasks to find ones matching our namespace
            tasks_response = self.ecs_client.describe_tasks(
                cluster=self.cluster_name, tasks=task_arns
            )

            for task in tasks_response.get("tasks", []):
                # Check if task belongs to our namespace
                for tag in task.get("tags", []):
                    if tag["key"] == "Namespace" and tag["value"] == namespace:
                        task_arn = task["taskArn"]
                        self.ecs_client.stop_task(
                            cluster=self.cluster_name, task=task_arn, reason="Cleanup"
                        )
                        print(f"Stopped task: {task_arn}")

            return True
        except ClientError as e:
            print(f"Error stopping tasks: {e}")
            return False

    def delete_log_group(self, namespace: str) -> bool:
        """
        Delete CloudWatch log group

        :param str namespace: Unique identifier for the deployment
        :return: True if deleted
        """
        log_group_name = f"/ecs/{namespace}"

        try:
            self.logs_client.delete_log_group(logGroupName=log_group_name)
            print(f"Deleted log group: {log_group_name}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Log group {log_group_name} not found")
                return False
            else:
                print(f"Error deleting log group: {e}")
                return False

    def delete_repo_from_ecr(self, repo_name: str) -> bool:
        """
        Delete a repository from ECR

        :param str repo_name: The name of the repository to delete
        :return: True if deleted, False if repository didn't exist
        """
        try:
            self.ecr_client.delete_repository(
                repositoryName=repo_name,
                force=True,  # Delete even if contains images
            )
            print(f"✅ Successfully deleted repository {repo_name} from ECR")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                print(f"Repository {repo_name} not found in ECR")
                return False
            else:
                print(f"⚠️ Error deleting repository from ECR: {e}")
                return False


def profile(number_of_instances: int = 10, container: str = None):
    """
    Profile the ECS deployment by creating multiple instances

    :param int number_of_instances: Number of instances to create
    :param str container: Container image to use
    """
    if not container:
        container = os.getenv("DE_BENCH_ECS_IMAGE_NAME")
        if not container:
            raise ValueError(
                "Container image must be provided or set in DE_BENCH_ECS_IMAGE_NAME"
            )

    manager = ECSManifestManager(provider="AWS")

    total_start = time.time()
    times = []

    for i in range(1, number_of_instances + 1):
        namespace = f"profile-namespace-{i}"
        instance_start = time.time()

        print(f"\n{'=' * 60}")
        print(f"Profile Instance {i}/{number_of_instances}: {namespace}")
        print(f"{'=' * 60}")

        try:
            result = manager.generate_and_deploy(
                namespace=namespace,
                container_image=container,
                use_service=False,  # Use standalone tasks for profiling
                enable_load_balancer=False,
            )

            instance_time = time.time() - instance_start
            times.append(instance_time)

            print(f"\nInstance {i} completed in {instance_time:.2f}s")
            print(f"Task ARN: {result.get('task', {}).get('taskArn', 'N/A')}")
            print(f"Public IP: {result.get('public_ip', 'N/A')}")

        except Exception as e:
            print(f"❌ Error in instance {i}: {e}")
            instance_time = time.time() - instance_start
            times.append(instance_time)

    total_time = time.time() - total_start
    avg_time = sum(times) / len(times) if times else 0

    print(f"\n{'=' * 60}")
    print("PROFILE SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total instances: {number_of_instances}")
    print(f"Successful deployments: {len(times)}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Average time per instance: {avg_time:.2f}s")
    print(f"{'=' * 60}\n")


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="ECS Manifest Manager for Airflow deployments"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy Airflow to ECS")
    deploy_parser.add_argument("namespace", help="Unique namespace for deployment")
    deploy_parser.add_argument(
        "--container", required=True, help="Container image to deploy"
    )
    deploy_parser.add_argument(
        "--service", action="store_true", help="Create ECS service (vs standalone task)"
    )
    deploy_parser.add_argument(
        "--load-balancer", action="store_true", help="Enable load balancer"
    )

    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up a deployment")
    cleanup_parser.add_argument("namespace", help="Namespace to clean up")
    cleanup_parser.add_argument(
        "--infrastructure",
        action="store_true",
        help="Also cleanup created infrastructure (VPC, subnets, roles, etc.)",
    )

    # Profile command
    profile_parser = subparsers.add_parser(
        "profile", help="Profile deployment performance"
    )
    profile_parser.add_argument(
        "--instances", type=int, default=10, help="Number of instances to deploy"
    )
    profile_parser.add_argument("--container", help="Container image to deploy")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = ECSManifestManager(provider="AWS")

    if args.command == "deploy":
        result = manager.generate_and_deploy(
            namespace=args.namespace,
            container_image=args.container,
            use_service=args.service,
            enable_load_balancer=args.load_balancer,
        )
        print("\nDeployment completed!")
        print(f"Namespace: {result['namespace']}")
        print(f"Task Definition: {result['task_definition_arn']}")
        if "base_url" in result:
            print(f"Base URL: {result['base_url']}")

    elif args.command == "cleanup":
        manager.cleanup_deployment(
            args.namespace, cleanup_infrastructure=args.infrastructure
        )

    elif args.command == "profile":
        profile(number_of_instances=args.instances, container=args.container)


if __name__ == "__main__":
    main()
