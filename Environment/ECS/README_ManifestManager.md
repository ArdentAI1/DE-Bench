# ECS Manifest Manager

A Python-based tool for automating the deployment of containerized Airflow applications to AWS ECS (Elastic Container Service). This tool streamlines the process of creating isolated, sandboxed Airflow environments with standardized ECS resources for DE-Bench evaluations.

## Key Benefits

### 1. **Cost Efficiency**
- **No Astronomer Fees**: Eliminate expensive Astro deployment costs by running Airflow directly on AWS ECS Fargate
- **Pay-per-Use**: Only pay for the CPU and memory your containers actually use
- **Automatic Cleanup**: Built-in resource cleanup prevents runaway costs from forgotten deployments
- **Fargate Pricing**: No need to manage EC2 instances - pay only for container runtime

### 2. **Faster Test Execution**
- **Rapid Deployment**: Deploy Airflow instances in under 2 minutes
- **Parallel Testing**: Run multiple isolated Airflow instances simultaneously
- **No Rate Limits**: Direct AWS API access without intermediary service restrictions
- **Quick Teardown**: Fast cleanup with parallel resource deletion

### 3. **Full Control & Flexibility**
- **Custom Container Images**: Use any Airflow container image from ECR or public registries
- **Environment Variables**: Full control over Airflow configuration
- **Direct API Access**: Direct access to ECS, ALB, and CloudWatch APIs
- **Choice of Deployment**: Use ECS Services (long-running) or Tasks (batch jobs)

### 4. **Evaluation Framework Integration**
- **Fixture Pattern**: Seamlessly integrates with DEBenchFixture for test setup/teardown
- **Load Balancer Support**: Optional Application Load Balancer with DNS for AI agent access
- **Public IP Access**: Direct task access via public IPs for standalone deployments
- **Health Monitoring**: Built-in health checks ensure Airflow is ready before tests run

## Quick Start

Get started with ECS deployments in 5 minutes:

### 1. Configure Environment Variables

Create or update your `.env` file with AWS credentials and ECS configuration:

```bash
# Copy example and edit
cp Environment/ECS/.env.example .env.ecs
# Edit with your values
```

### 2. Push Your Container to ECR

```bash
# Authenticate to ECR
AWS_REGION="us-east-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws ecr get-login-password --region $AWS_REGION | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Tag and push your image
docker tag airflow:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base
```

### 3. Deploy Airflow to ECS

```bash
# Deploy as standalone task (fastest)
uv run python scripts/ecs_airflow_manager.py deploy test-airflow \
  --container ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base

# Or deploy as service with load balancer (production-like)
uv run python scripts/ecs_airflow_manager.py deploy prod-airflow \
  --container ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base \
  --service \
  --load-balancer
```

### 4. Access Your Airflow Instance

```bash
# Get the endpoint
uv run python scripts/ecs_airflow_manager.py get-endpoint test-airflow

# Check health
uv run python scripts/ecs_airflow_manager.py health-check <endpoint> --port 8080
```

### 5. Cleanup When Done

```bash
uv run python scripts/ecs_airflow_manager.py cleanup test-airflow
```

## Architecture Overview

### ECS vs Kubernetes Mapping

| Kubernetes Concept | ECS Equivalent | Notes |
|-------------------|----------------|-------|
| Namespace | Tags/Naming Convention | ECS uses tags for logical isolation |
| Job | ECS Task (Fargate) | Standalone task that runs to completion |
| Deployment/Service | ECS Service | Long-running service with desired count |
| LoadBalancer Service | Application Load Balancer + Target Group | External HTTP/HTTPS access |
| Pod | ECS Task | Unit of deployment running containers |

### Resource Architecture

Each deployment creates:

1. **ECS Task Definition**: Container specification with CPU, memory, ports, and environment
2. **ECS Service** (optional): Manages desired count of running tasks
3. **Application Load Balancer** (optional): Provides stable DNS endpoint
4. **Target Group** (optional): Routes traffic to ECS tasks
5. **CloudWatch Log Group**: Centralized logging for containers

## Configuration

Ensure your `.env` file is in the project root with your AWS credentials:

```bash
# AWS Authentication
AWS_ACCESS_KEY_ID="your-access-key-id"
AWS_SECRET_ACCESS_KEY="your-secret-access-key"
AWS_REGION="us-east-2"  # Or your preferred region (us-east-1, us-west-2, etc.)

# AWS ECR Configuration
AWS_ECR_BASE="123456789012.dkr.ecr.us-east-2.amazonaws.com"

# ECS Cluster Configuration
DE_BENCH_ECS_CLUSTER_NAME="your-ecs-cluster-name"
DE_BENCH_ECS_IMAGE_NAME="123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base"

# Optional: Additional container images for specific tests
ECS_FAILURE_HELLO_WORLD_AIRFLOW_IMAGE="123456789012.dkr.ecr.us-east-2.amazonaws.com/failed-hello-world:base"

# Network Configuration
DE_BENCH_ECS_SUBNET_IDS="subnet-xxxxx,subnet-yyyyy"  # Comma-separated, use 2+ for ALB
DE_BENCH_ECS_SECURITY_GROUP_IDS="sg-xxxxx"  # Must allow inbound 8080 and outbound internet

# IAM Roles
DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN="arn:aws:iam::123456789012:role/ecsTaskExecutionRole"
DE_BENCH_ECS_TASK_ROLE_ARN="arn:aws:iam::123456789012:role/ecsTaskRole"  # Optional
```

**Security Note**: Never commit the `.env` file to version control. It contains sensitive credentials.

### Example Configuration

Here's a real-world example configuration (with anonymized credentials):

```bash
# AWS Authentication
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-very-secret-access-key
AWS_REGION=us-east-2

# AWS ECR
AWS_ECR_BASE=123456789.dkr.ecr.us-east-2.amazonaws.com

# ECS Configuration
DE_BENCH_ECS_CLUSTER_NAME=de-bench-cluster
DE_BENCH_ECS_IMAGE_NAME="123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-image:latest"
ECS_FAILURE_HELLO_WORLD_AIRFLOW_IMAGE="123456789012.dkr.ecr.us-east-2.amazonaws.com/failed-airflow-state:latest"

# Network (must be configured in AWS first)
DE_BENCH_ECS_SUBNET_IDS="subnet-123456789012,subnet-123456789013"
DE_BENCH_ECS_SECURITY_GROUP_IDS="sg-123456789012-asiouhd"

# IAM Roles (must be created in AWS first)
DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN="arn:aws:iam::123456789012:role/ecsTaskExecutionRole"
DE_BENCH_ECS_TASK_ROLE_ARN="arn:aws:iam::123456789012:role/ecsTaskRole"
```

### Required AWS Permissions

Your AWS credentials need the following permissions:

#### ECS Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:RegisterTaskDefinition",
        "ecs:DeregisterTaskDefinition",
        "ecs:CreateService",
        "ecs:UpdateService",
        "ecs:DeleteService",
        "ecs:DescribeServices",
        "ecs:RunTask",
        "ecs:StopTask",
        "ecs:DescribeTasks",
        "ecs:ListTasks",
        "ecs:ListTaskDefinitions",
        "ecs:TagResource"
      ],
      "Resource": "*"
    }
  ]
}
```

#### EC2/VPC Permissions (for networking)
```json
{
  "Effect": "Allow",
  "Action": [
    "ec2:DescribeSubnets",
    "ec2:DescribeNetworkInterfaces",
    "ec2:DescribeSecurityGroups"
  ],
  "Resource": "*"
}
```

#### ELB Permissions (for load balancers)
```json
{
  "Effect": "Allow",
  "Action": [
    "elasticloadbalancing:CreateLoadBalancer",
    "elasticloadbalancing:DeleteLoadBalancer",
    "elasticloadbalancing:DescribeLoadBalancers",
    "elasticloadbalancing:CreateTargetGroup",
    "elasticloadbalancing:DeleteTargetGroup",
    "elasticloadbalancing:DescribeTargetGroups",
    "elasticloadbalancing:CreateListener",
    "elasticloadbalancing:DescribeListeners",
    "elasticloadbalancing:AddTags"
  ],
  "Resource": "*"
}
```

#### CloudWatch Logs
```json
{
  "Effect": "Allow",
  "Action": [
    "logs:CreateLogGroup",
    "logs:DeleteLogGroup",
    "logs:PutRetentionPolicy"
  ],
  "Resource": "*"
}
```

#### ECR Permissions (optional, for private registries)
```json
{
  "Effect": "Allow",
  "Action": [
    "ecr:DeleteRepository",
    "ecr:BatchGetImage",
    "ecr:GetDownloadUrlForLayer"
  ],
  "Resource": "*"
}
```

### Network Requirements

1. **Subnets**: Must be in the same VPC
   - For ALB: Use at least 2 subnets in different Availability Zones
   - For standalone tasks: 1 subnet is sufficient

2. **Security Groups**: Must allow:
   - Inbound: Port 8080 (for Airflow web UI)
   - Outbound: All traffic (for container registry access and internet)

3. **Internet Access**: Tasks need internet access to:
   - Pull container images from ECR or public registries
   - Install dependencies
   - Access external services

## Pushing Container Images to ECR

Before deploying, you need to push your container images to ECR. Here's a quick guide:

### 1. Authenticate Docker to ECR

```bash
# Set variables
AWS_REGION="us-east-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
```

### 2. Create ECR Repository (if needed)

```bash
# Create repository
aws ecr create-repository \
    --repository-name airflow2-session-auth \
    --region $AWS_REGION \
    --image-scanning-configuration scanOnPush=true
```

### 3. Tag and Push Your Image

```bash
# Tag your local image
docker tag airflow:latest \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base

# Push to ECR
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow2-session-auth:base
```

### 4. Verify Image in ECR

```bash
# List images in repository
aws ecr describe-images \
    --repository-name airflow2-session-auth \
    --region $AWS_REGION
```

### Complete Push Script

Save this as `scripts/push_to_ecr.sh`:

```bash
#!/bin/bash
set -e

# Configuration
REPO_NAME="airflow2-session-auth"
IMAGE_TAG="base"
AWS_REGION="us-east-2"
LOCAL_IMAGE="airflow:latest"

# Get account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}"

echo "AWS Account: $AWS_ACCOUNT_ID"
echo "ECR URI: $ECR_URI"

# Create repository if it doesn't exist
if ! aws ecr describe-repositories --repository-names $REPO_NAME --region $AWS_REGION 2>/dev/null; then
    echo "Creating repository..."
    aws ecr create-repository \
        --repository-name $REPO_NAME \
        --region $AWS_REGION \
        --image-scanning-configuration scanOnPush=true
fi

# Authenticate
echo "Authenticating..."
aws ecr get-login-password --region $AWS_REGION | \
    docker login --username AWS --password-stdin $ECR_URI

# Tag and push
echo "Tagging image..."
docker tag $LOCAL_IMAGE ${ECR_URI}:${IMAGE_TAG}

echo "Pushing to ECR..."
docker push ${ECR_URI}:${IMAGE_TAG}

echo "✅ Successfully pushed ${ECR_URI}:${IMAGE_TAG}"
```

Make it executable and run:
```bash
chmod +x scripts/push_to_ecr.sh
./scripts/push_to_ecr.sh
```

## Usage

### Programmatic Usage (Recommended for Fixtures)

The primary use case is programmatic integration with DEBench fixtures:

```python
from Environment.ECS.ManifestManager import ECSManifestManager

# Initialize the manager
ecs_manager = ECSManifestManager(provider="AWS")

# Deploy Airflow to ECS
namespace = "my-airflow-test"
container = "123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest"

result = ecs_manager.generate_and_deploy(
    namespace=namespace,
    container_image=container,
    use_service=True,  # Use ECS Service for long-running deployment
    enable_load_balancer=True  # Create ALB for external access
)

# Access deployment details
print(f"Airflow available at: {result['base_url']}")
print(f"Task Definition: {result['task_definition_arn']}")
print(f"Load Balancer DNS: {result['dns_name']}")

# Verify health (ALB uses port 80)
if ecs_manager.verify_pod_health(endpoint=result['dns_name'], port=80):
    print("Airflow is healthy and ready!")

# Cleanup when done
ecs_manager.cleanup_deployment(namespace)
```

### Standalone Task Deployment (Batch Jobs)

For short-lived batch jobs, use standalone tasks:

```python
from Environment.ECS.ManifestManager import ECSManifestManager

ecs_manager = ECSManifestManager(provider="AWS")

result = ecs_manager.generate_and_deploy(
    namespace="batch-job-123",
    container_image="123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest",
    use_service=False,  # Run as standalone task
    enable_load_balancer=False
)

# Access via public IP (port 8080)
print(f"Task running at: http://{result['public_ip']}:8080")

# Cleanup
ecs_manager.cleanup_deployment("batch-job-123")
```

### Integration with AirflowFixture

The ECS ManifestManager integrates with the AirflowFixture for ECS-based deployments:

```python
from Fixtures.Airflow.airflow_fixture import AirflowFixture

# Configure Airflow to use ECS
custom_airflow_config = {
    "resource_id": "my_test_12345",
    "use_ecs": True,  # Enable ECS deployment
    "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest",
    "use_load_balancer": True,  # Optional: enable ALB
}

# Create fixture - ECS ManifestManager is used internally
airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)

# After setup, access the ECS deployment details
resource_data = airflow_fixture._resource_data
print(f"Airflow URL: {resource_data['base_url']}")
print(f"DNS Name: {resource_data['dns_name']}")
print(f"Task Definition: {resource_data['task_definition_arn']}")
```

### CLI Usage

The ECS ManifestManager provides two command-line interfaces:

#### Using the Dedicated Script (Recommended)

The `scripts/ecs_airflow_manager.py` provides a user-friendly CLI with additional commands:

**Deploy with Service and Load Balancer:**
```bash
uv run python scripts/ecs_airflow_manager.py deploy my-airflow \
  --container 123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base \
  --service \
  --load-balancer
```

**Deploy as Standalone Task (faster for testing):**
```bash
uv run python scripts/ecs_airflow_manager.py deploy my-airflow-task \
  --container 123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base
```

**Get Deployment Endpoint:**
```bash
uv run python scripts/ecs_airflow_manager.py get-endpoint my-airflow
```

**Check Health:**
```bash
# For ALB (port 80)
uv run python scripts/ecs_airflow_manager.py health-check my-alb-dns.elb.amazonaws.com

# For direct task access (port 8080)
uv run python scripts/ecs_airflow_manager.py health-check 54.123.45.67 --port 8080
```

**Cleanup Deployment:**
```bash
uv run python scripts/ecs_airflow_manager.py cleanup my-airflow
```

**Profile Performance:**
```bash
uv run python scripts/ecs_airflow_manager.py profile 10 \
  123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base
```

**Additional Commands:**
```bash
# Stop all tasks for a namespace
uv run python scripts/ecs_airflow_manager.py stop-tasks my-airflow

# Delete just the service
uv run python scripts/ecs_airflow_manager.py delete-service my-airflow

# Delete just the load balancer
uv run python scripts/ecs_airflow_manager.py delete-load-balancer my-airflow

# Get public IP of a specific task
uv run python scripts/ecs_airflow_manager.py get-task-ip arn:aws:ecs:us-east-2:123456789012:task/cluster/abc123
```

#### Using the ManifestManager Directly

You can also use the ManifestManager module directly:

```bash
# Deploy
python Environment/ECS/ManifestManager.py deploy my-airflow \
  --container 123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base \
  --service \
  --load-balancer

# Cleanup
python Environment/ECS/ManifestManager.py cleanup my-airflow

# Profile
python Environment/ECS/ManifestManager.py profile \
  --instances 10 \
  --container 123456789012.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base
```

## Key Methods

### Deployment Methods

#### `register_task_definition(namespace, container_image, cpu="1024", memory="2048")`
Registers an ECS task definition with specified resources.

**Parameters:**
- `namespace`: Unique identifier (used as task family name)
- `container_image`: Full container image URI
- `cpu`: CPU units (256, 512, 1024, 2048, 4096)
- `memory`: Memory in MB (512, 1024, 2048, 4096, 8192, 16384)

**Returns:** Task definition ARN

#### `create_or_update_service(namespace, task_definition_arn, desired_count=1)`
Creates or updates an ECS service for long-running deployments.

**Parameters:**
- `namespace`: Unique identifier
- `task_definition_arn`: ARN of registered task definition
- `desired_count`: Number of tasks to maintain
- `enable_load_balancer`: Whether to attach ALB
- `target_group_arn`: ARN of target group (if using ALB)

**Returns:** Service details dictionary

#### `run_task(namespace, task_definition_arn, wait_for_completion=False)`
Runs a standalone ECS task (batch job).

**Parameters:**
- `namespace`: Unique identifier
- `task_definition_arn`: ARN of registered task definition
- `wait_for_completion`: Block until task completes

**Returns:** Task details dictionary

#### `generate_and_deploy(namespace, container_image, use_service=True, enable_load_balancer=True)`
Complete deployment workflow in one call.

**Parameters:**
- `namespace`: Unique identifier
- `container_image`: Full container image URI
- `use_service`: True for ECS Service, False for standalone task
- `enable_load_balancer`: Create ALB (only with service)

**Returns:** Complete deployment details dictionary

### Load Balancer Methods

#### `create_load_balancer_and_target_group(namespace, vpc_id=None)`
Creates Application Load Balancer and target group.

**Parameters:**
- `namespace`: Unique identifier
- `vpc_id`: VPC ID (auto-detected from subnets if not provided)

**Returns:** Tuple of (load_balancer_arn, target_group_arn)

#### `get_load_balancer_dns(namespace)`
Retrieves the DNS name of the load balancer.

**Returns:** DNS name string or None

### Task Management Methods

#### `get_task_public_ip(task_arn, max_attempts=60)`
Retrieves the public IP address of a running task.

**Parameters:**
- `task_arn`: ARN of the task
- `max_attempts`: Number of polling attempts (5s each)

**Returns:** Public IP string or None

#### `stop_tasks(namespace)`
Stops all running tasks for a namespace.

### Cleanup Methods

#### `cleanup_deployment(namespace)`
Complete cleanup of all resources for a deployment.

**Deletes:**
- ECS Service
- Running tasks
- Application Load Balancer
- Target Group
- Task definitions
- CloudWatch log groups

#### `delete_service(namespace)`
Deletes an ECS service (scales to 0 first).

#### `delete_load_balancer(namespace)`
Deletes load balancer and associated target group.

#### `delete_task_definition(namespace)`
Deregisters all task definitions in a family.

#### `delete_log_group(namespace)`
Deletes CloudWatch log group.

#### `delete_repo_from_ecr(repo_name)`
Deletes an ECR repository (with force flag).

### Health Check Methods

#### `verify_pod_health(endpoint, port=80)`
Verifies service health via HTTP GET to `/health`.

**Parameters:**
- `endpoint`: DNS name or IP address
- `port`: Port number (80 for ALB, 8080 for direct access)

**Returns:** True if healthy (200 status), False otherwise

## Task Definition Structure

The generated ECS task definition includes:

```json
{
  "family": "namespace",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::account:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::account:role/ecsTaskRole",
  "containerDefinitions": [
    {
      "name": "airflow",
      "image": "container-image-uri",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp",
          "name": "airflow-web"
        }
      ],
      "environment": [
        {
          "name": "IS_SANDBOX",
          "value": "1"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/namespace",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "airflow",
          "awslogs-create-group": "true"
        }
      }
    }
  ],
  "tags": [
    {
      "key": "Environment",
      "value": "DE-Bench"
    },
    {
      "key": "Namespace",
      "value": "namespace"
    }
  ]
}
```

## Profiling and Batch Deployment

The `profile()` function provides performance testing by deploying multiple instances:

```python
from Environment.ECS.ManifestManager import profile

# Deploy 10 Airflow instances and measure performance
profile(
    number_of_instances=10,
    container="123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest"
)
```

### Profile Behavior

- **Namespace Creation**: Generates namespaces named `profile-namespace-{1..N}`
- **Standalone Tasks**: Uses standalone tasks (not services) for faster deployment
- **No Load Balancers**: Direct task access via public IPs
- **Performance Metrics**: Tracks timing for each operation
- **Parallel Deployment**: Deploys tasks concurrently

### Example Output

```
============================================================
Profile Instance 1/10: profile-namespace-1
============================================================
Registered task definition: arn:aws:ecs:us-east-1:123456789012:task-definition/profile-namespace-1:1
Started task: arn:aws:ecs:us-east-1:123456789012:task/cluster-name/abc123
Task public IP: 54.123.45.67

Instance 1 completed in 78.34s

============================================================
PROFILE SUMMARY
============================================================
Total instances: 10
Successful deployments: 10
Total time: 783.40s
Average time per instance: 78.34s
============================================================
```

## Project Structure

```
DE-Bench/
├── Environment/
│   ├── ECS/
│   │   ├── ManifestManager.py          # ECS deployment manager
│   │   ├── __init__.py                 # Module initialization
│   │   ├── .env.example                # Environment variable template
│   │   └── README_ManifestManager.md   # This documentation
│   └── Kubernetes/
│       ├── ManifestManager.py          # AKS/K8s version
│       └── README_ManifestManager.md
├── scripts/
│   ├── ecs_airflow_manager.py          # CLI script for ECS
│   ├── aks_airflow_manager.py          # CLI script for AKS
│   └── push_to_ecr.sh                  # Helper script to push to ECR
├── Fixtures/
│   └── Airflow/
│       └── airflow_fixture.py          # AirflowFixture with ECS/AKS support
├── Tests/
│   └── Airflow_Agent_Hello_World_Failure/
│       └── test_airflow_agent_hello_universe_pipeline.py
└── .env                                # Environment variables (not in repo)
```

## Infrastructure as Code: Terraform Integration (Optional)

While the Python SDK is recommended for dynamic test deployments, you can use Terraform to manage the **base infrastructure** that DE-Bench depends on.

### Recommended Hybrid Approach

**Use Terraform for persistent infrastructure:**
- ✅ VPC, Subnets, Security Groups
- ✅ ECS Cluster
- ✅ IAM Roles (Task Execution Role, Task Role)
- ✅ ECR Repositories
- ✅ CloudWatch Log Groups

**Use Python SDK (this tool) for ephemeral test resources:**
- ✅ Task Definitions (per-test, temporary)
- ✅ ECS Tasks/Services (per-test, temporary)
- ✅ Load Balancers (per-test, temporary)
- ✅ Target Groups (per-test, temporary)

### Why This Hybrid Approach?

**Benefits:**
- Infrastructure as Code for base resources
- Dynamic, programmatic control for test resources
- Easy pytest integration
- Fast test iteration
- Clean separation of concerns
- No need to manage Terraform state for ephemeral resources

**Example Terraform Setup:**

See the main README for a complete Terraform configuration example that creates:
- VPC with public subnets across 2 AZs
- Internet Gateway and route tables
- Security group allowing port 8080
- ECS Cluster with Container Insights
- IAM roles with proper permissions
- ECR repository for container images

After deploying with Terraform, use its outputs to populate your `.env` file:

```bash
cd terraform
terraform output -raw cluster_name        # DE_BENCH_ECS_CLUSTER_NAME
terraform output -raw subnet_ids          # DE_BENCH_ECS_SUBNET_IDS
terraform output -raw security_group_id   # DE_BENCH_ECS_SECURITY_GROUP_IDS
terraform output -raw task_execution_role_arn  # DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN
```

### Why Not Pure Terraform?

Using Terraform for everything would require:
1. Generating `.tf` files dynamically from Python
2. Calling `terraform apply` via subprocess
3. Managing state files per test
4. Slower iteration cycles

For a testing framework that needs to spin up/tear down many isolated instances dynamically, the Python SDK approach is more practical.

## Comparison: ECS vs Kubernetes (AKS)

| Feature | ECS (This Implementation) | Kubernetes (AKS) |
|---------|---------------------------|------------------|
| **Deployment Time** | ~60-90s (task), ~120s (service+ALB) | ~180-300s (with LoadBalancer) |
| **Cost** | $0.04/vCPU/hour + $0.004/GB/hour | AKS management free + node costs |
| **Management** | Fully managed (Fargate) | Node management required |
| **Networking** | ALB or direct IP | K8s LoadBalancer Service |
| **Isolation** | Tags + naming | Namespaces |
| **Learning Curve** | Moderate (AWS-specific) | Steeper (K8s concepts) |
| **Portability** | AWS-only | Multi-cloud (with configs) |
| **Batch Jobs** | Native (ECS Tasks) | Jobs resource |
| **Service Discovery** | ALB + Route 53 | K8s Services + Ingress |
| **Logging** | CloudWatch Logs | Container Insights / custom |

## Error Handling

The tool includes robust error handling:

- **ClientError Handling**: All boto3 calls wrapped in try/except
- **Resource Conflicts**: Handles existing resources gracefully
- **404 Not Found**: Handles missing resources during cleanup
- **Timeouts**: Configurable timeouts for async operations
- **Validation**: Environment variable validation on initialization

## Dependencies

```txt
boto3>=1.34.0              # AWS SDK for Python
botocore>=1.34.0           # Low-level AWS SDK
python-dotenv==1.0.1       # Environment variable management
requests>=2.31.0           # HTTP requests for health checks
```

Install dependencies:
```bash
pip install boto3 botocore python-dotenv requests
```

## Security Considerations

### 1. Credential Management
- Never commit `.env` file to version control
- Use AWS Secrets Manager or Systems Manager Parameter Store in production
- Rotate access keys regularly
- Consider using IAM roles for EC2/ECS instead of access keys

### 2. IAM Roles
- **Task Execution Role**: Minimal permissions for ECS to pull images and write logs
- **Task Role**: Application-specific permissions (e.g., S3 access)
- Use least-privilege principle

### 3. Network Security
- Security groups should restrict inbound access
- Consider using private subnets with NAT Gateway
- Use VPC endpoints for AWS services to avoid internet traffic
- Enable VPC Flow Logs for network monitoring

### 4. Container Images
- Use ECR with image scanning enabled
- Use specific image tags, never use `:latest` in production
- Implement image signing and verification
- Scan images for vulnerabilities before deployment

### 5. Secrets Management
- Use ECS secrets integration with Secrets Manager/Parameter Store
- Never embed secrets in container images
- Use environment variable injection for sensitive data

### 6. Monitoring & Auditing
- Enable CloudTrail for API auditing
- Use CloudWatch alarms for unusual activity
- Monitor task and service metrics
- Set up log retention policies

## Troubleshooting

### Authentication Issues
- Verify AWS credentials in `.env` or IAM role
- Check IAM permissions match requirements above
- Ensure region is correctly set
- Test credentials: `aws sts get-caller-identity`

### Task Launch Failures

**Issue**: Task fails to start
- Check CloudWatch logs: `/ecs/namespace`
- Verify container image is accessible from ECS
- Ensure task execution role can pull from ECR
- Check resource limits (CPU/memory) aren't exceeded

**Issue**: Task starts but immediately stops
- Review CloudWatch logs for application errors
- Verify environment variables are correct
- Check security group allows outbound internet access
- Ensure sufficient task resources (CPU/memory)

### Network Issues

**Issue**: Cannot reach task via public IP
- Verify security group allows inbound port 8080
- Ensure `assignPublicIp: ENABLED` is set
- Check subnets have internet gateway attached
- Verify task is in RUNNING state

**Issue**: Load balancer health checks failing
- Check security group allows ALB → task traffic
- Verify `/health` endpoint exists in application
- Review health check configuration (path, interval)
- Check CloudWatch logs for application errors

### Load Balancer Issues

**Issue**: ALB creation fails
- Ensure at least 2 subnets in different AZs
- Verify subnets are in the same VPC
- Check AWS service quotas for ALBs
- Review IAM permissions for ELB actions

**Issue**: Cannot access via ALB DNS
- Wait for ALB to become active (1-2 minutes)
- Verify listener is configured (port 80)
- Check target group health status
- Ensure tasks are registered with target group

### Resource Cleanup Issues

**Issue**: Cannot delete target group
- Must delete ALB first (TG is dependent resource)
- Wait 5-10 seconds between ALB and TG deletion
- Check if TG is attached to other resources

**Issue**: Service deletion hangs
- Scale service to 0 tasks first
- Use `force=True` flag in delete call
- Check for dependent resources (ALB, TG)

## Advanced Usage

### Custom Task Definition

Override default task definition settings:

```python
manager = ECSManifestManager()

# Register custom task definition
task_def = manager.generate_task_definition(
    namespace="custom-airflow",
    container_image="my-image:latest",
    cpu="2048",  # 2 vCPU
    memory="4096"  # 4 GB
)

# Modify task definition as needed
task_def["containerDefinitions"][0]["environment"].append({
    "name": "CUSTOM_VAR",
    "value": "custom_value"
})

# Register modified definition
response = manager.ecs_client.register_task_definition(**task_def)
task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
```

### Multiple Containers

Deploy with sidecar containers:

```python
task_def = manager.generate_task_definition(
    namespace="multi-container",
    container_image="airflow:latest"
)

# Add sidecar container
task_def["containerDefinitions"].append({
    "name": "log-router",
    "image": "fluent/fluentd:latest",
    "essential": False,
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
            "awslogs-group": f"/ecs/multi-container",
            "awslogs-region": manager.region,
            "awslogs-stream-prefix": "fluentd"
        }
    }
})

response = manager.ecs_client.register_task_definition(**task_def)
```

### Using Secrets

Inject secrets from AWS Secrets Manager:

```python
task_def = manager.generate_task_definition(
    namespace="secure-airflow",
    container_image="airflow:latest"
)

# Add secrets
task_def["containerDefinitions"][0]["secrets"] = [
    {
        "name": "DB_PASSWORD",
        "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-password-abc123"
    }
]

response = manager.ecs_client.register_task_definition(**task_def)
```

## Cost Estimation

### Fargate Pricing (us-east-1)
- **vCPU**: $0.04048 per vCPU per hour
- **Memory**: $0.004445 per GB per hour

### Example: 1024 CPU (1 vCPU), 2048 MB (2 GB)
- vCPU cost: $0.04048/hour
- Memory cost: $0.00889/hour (2 GB)
- **Total**: $0.04937/hour or **~$35.50/month** (continuous)

### Batch Job Example (2 hours runtime)
- **Per run**: $0.04937 × 2 = $0.09874
- **100 runs/month**: $9.87

### Additional Costs
- **ALB**: $0.0225/hour + $0.008/LCU-hour (~$16.50/month base)
- **CloudWatch Logs**: $0.50/GB ingested, $0.03/GB stored
- **Data Transfer**: $0.09/GB out (first 10 TB)

### Cost Optimization Tips
1. Use standalone tasks for batch jobs (avoid ALB)
2. Delete resources immediately after use
3. Use appropriate CPU/memory sizes
4. Monitor CloudWatch costs (set retention policies)
5. Use spot capacity for fault-tolerant workloads

## Monitoring and Observability

### CloudWatch Logs

View logs for a deployment:
```bash
aws logs tail /ecs/my-airflow --follow
```

### CloudWatch Metrics

Monitor ECS metrics:
- CPUUtilization
- MemoryUtilization
- TaskCount
- TargetResponseTime (with ALB)

### CloudWatch Alarms

Set up alarms for failures:
```python
import boto3

cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_alarm(
    AlarmName='ecs-task-failure',
    MetricName='TaskCount',
    Namespace='AWS/ECS',
    Statistic='Average',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='LessThanThreshold',
    Dimensions=[
        {'Name': 'ClusterName', 'Value': 'your-cluster'},
        {'Name': 'ServiceName', 'Value': 'your-service'}
    ]
)
```

## Conclusion

The ECS ManifestManager provides a robust, cost-effective alternative to Kubernetes for deploying Airflow in DE-Bench evaluations. With support for both long-running services and batch tasks, integrated load balancing, and comprehensive cleanup capabilities, it offers flexibility for various testing scenarios.

### Summary of Key Features

✅ **Two Deployment Modes**: ECS Services (long-running) or Tasks (batch jobs)
✅ **Load Balancer Support**: Optional ALB with stable DNS endpoints
✅ **Direct IP Access**: Public IPs for standalone tasks
✅ **Comprehensive CLI**: User-friendly `ecs_airflow_manager.py` script
✅ **Python SDK Integration**: Seamless integration with pytest fixtures
✅ **ECR Integration**: Built-in support for private container registries
✅ **Auto-Cleanup**: Complete resource cleanup with single command
✅ **CloudWatch Logging**: Centralized logging with automatic log group creation
✅ **Health Checks**: Built-in HTTP health verification
✅ **Cost-Effective**: Pay only for actual container runtime (Fargate)
✅ **Terraform Ready**: Optional base infrastructure management with IaC

### Quick Links

- **Getting Started**: See [Quick Start](#quick-start) section
- **CLI Reference**: See [CLI Usage](#cli-usage) section
- **Pushing to ECR**: See [Pushing Container Images to ECR](#pushing-container-images-to-ecr)
- **API Reference**: See [Key Methods](#key-methods) section
- **Troubleshooting**: See troubleshooting sections throughout
- **Cost Calculator**: See [Cost Estimation](#cost-estimation) section

### Next Steps

1. **Set up base infrastructure** (VPC, ECS Cluster, IAM roles)
2. **Push your Airflow container** to ECR
3. **Configure `.env`** with AWS credentials and resource IDs
4. **Deploy your first test** with `scripts/ecs_airflow_manager.py`
5. **Integrate with tests** using programmatic API

### Support

For questions or issues:
- AWS ECS Documentation: https://docs.aws.amazon.com/ecs/
- AWS Fargate Pricing: https://aws.amazon.com/fargate/pricing/
- DE-Bench Repository: Open an issue in the DE-Bench repository

---

**Version**: 1.0.0
**Last Updated**: 2025-01
**Region Example**: us-east-2
**Account Example**: 867765745132
