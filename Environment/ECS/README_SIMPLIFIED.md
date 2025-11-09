# ECS ManifestManager - Simplified Configuration

## Overview

The ECS ManifestManager has been updated to **automatically create all required AWS infrastructure**, making it as simple to use as the Kubernetes ManifestManager. Users now only need to provide minimal configuration:

1. **ECS Cluster Name** - A name for your cluster
2. **AWS Credentials** - Access key and secret key
3. **AWS Region** - The region to deploy to
4. **Container Image** - ECR image path

Everything else (VPC, subnets, security groups, IAM roles, ECS cluster) is created automatically!

## Required Environment Variables

Create a `.env` file with only these essential variables:

```env
# Required: ECS Cluster Name
DE_BENCH_ECS_CLUSTER_NAME=de-bench-cluster

# Required: AWS Credentials
ACCESS_KEY_ID_AWS=your_access_key_id
SECRET_ACCESS_KEY_AWS=your_secret_access_key

# Required: AWS Region
AWS_REGION=us-east-2
```

That's it! No need to manually create VPCs, subnets, security groups, or IAM roles.

## What Gets Created Automatically

When you initialize `ECSManifestManager()`, it automatically creates:

### 1. VPC and Networking
- **VPC** (10.0.0.0/16) with DNS support enabled
- **2 Public Subnets** in different Availability Zones (10.0.0.0/24, 10.0.1.0/24)
- **Internet Gateway** for public internet access
- **Route Table** configured with route to Internet Gateway
- Auto-assign public IP enabled on subnets

### 2. Security Group
- Allows inbound traffic on port **8080** (Airflow)
- Allows inbound traffic on port **80** (HTTP/ALB)
- Allows all outbound traffic

### 3. IAM Roles
- **Task Execution Role** with policies:
  - `AmazonECSTaskExecutionRolePolicy` (required for ECS)
  - `AmazonEC2ContainerRegistryReadOnly` (for pulling from ECR)

### 4. ECS Cluster
- Creates the ECS cluster if it doesn't exist
- All resources are tagged with `ManagedBy: DE-Bench-ECS` for easy identification

## Usage Examples

### Basic Deployment

```python
from Environment.ECS.ManifestManager import ECSManifestManager

# Initialize (creates all infrastructure automatically)
manager = ECSManifestManager()

# Deploy a container
result = manager.generate_and_deploy(
    namespace="my-airflow-test",
    container_image=f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/airflow:latest",
    use_service=True,
    enable_load_balancer=True
)

print(f"Deployment URL: {result['base_url']}")
```

### Using the CLI

```bash
# Deploy with service and load balancer
uv run python Environment/ECS/ManifestManager.py deploy my-namespace \
  --container ${AWS_ACCOUNT_ID}$.dkr.ecr.${AWS_REGION}$.amazonaws.com/airflow:latest \
  --service \
  --load-balancer

# Cleanup deployment only (keeps infrastructure)
uv run python Environment/ECS/ManifestManager.py cleanup my-namespace

# Cleanup deployment AND infrastructure
uv run python Environment/ECS/ManifestManager.py cleanup my-namespace --infrastructure
```

## Optional: Use Existing Resources

If you already have VPC, subnets, security groups, or IAM roles, you can still use them by providing these optional environment variables:

```env
# Optional: Use existing subnets
DE_BENCH_ECS_SUBNET_IDS=subnet-abc123,subnet-def456

# Optional: Use existing security groups
DE_BENCH_ECS_SECURITY_GROUP_IDS=sg-abc123

# Optional: Use existing IAM role
DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN=arn:aws:iam::123456789:role/my-role
```

If any of these are provided, the manager will use them instead of creating new ones.

## Infrastructure Cleanup

The manager tracks which resources it created vs which were provided. When cleaning up:

```python
# Cleanup deployment only (default)
manager.cleanup_deployment("my-namespace")

# Cleanup deployment AND all created infrastructure
manager.cleanup_deployment("my-namespace", cleanup_infrastructure=True)
```

**CLI:**
```bash
# Cleanup deployment only
uv run python Environment/ECS/ManifestManager.py cleanup my-namespace

# Cleanup deployment AND infrastructure
uv run python Environment/ECS/ManifestManager.py cleanup my-namespace --infrastructure
```

The `--infrastructure` flag will delete:
- Security groups (created by manager)
- Subnets (created by manager)
- Route tables (created by manager)
- Internet Gateway (created by manager)
- VPC (created by manager)
- IAM roles (created by manager)
- ECS cluster (if created by manager)

Resources provided via environment variables are **never deleted** during cleanup.

## Comparison with Kubernetes ManifestManager

Both managers now have similar simplicity:

### Kubernetes (EKS)
```python
from Environment.Kubernetes.EKSManifestManager import EKSManifestManager

manager = EKSManifestManager()
manager.generate_and_apply_manifest(
    namespace="my-test",
    container="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}$.amazonaws.com/airflow:latest"
)
```

### ECS
```python
from Environment.ECS.ManifestManager import ECSManifestManager

manager = ECSManifestManager()
manager.generate_and_deploy(
    namespace="my-test",
    container_image="${AWS_ACCOUNT_ID}$.dkr.ecr.${AWS_REGION}$.amazonaws.com/airflow:latest",
    use_service=True
)
```

## Resource Tagging

All created resources are tagged with:
- `ManagedBy: DE-Bench-ECS`
- `Name: de-bench-ecs-<resource-type>-<cluster-name>`

This makes it easy to identify and manage resources in the AWS Console.

## Naming Convention

All created resources follow a consistent naming pattern:
- VPC: `de-bench-ecs-vpc-{cluster_name}`
- Subnets: `de-bench-ecs-subnet-1-{cluster_name}`, `de-bench-ecs-subnet-2-{cluster_name}`
- Security Group: `de-bench-ecs-sg-{cluster_name}`
- IAM Role: `de-bench-ecs-execution-role-{cluster_name}`
- Route Table: `de-bench-ecs-rt-{cluster_name}`
- Internet Gateway: `de-bench-ecs-igw-{cluster_name}`

## Cost Considerations

Creating infrastructure has cost implications:

### Free Tier Eligible:
- VPC
- Subnets
- Internet Gateway (when attached)
- Route Tables
- Security Groups
- IAM Roles

### Costs Incurred:
- **ECS Tasks/Services**: Pay per vCPU and memory per second
- **Load Balancers**: ~$0.0225/hour (~$16/month) + per GB processed
- **Data Transfer**: Outbound data transfer charges
- **NAT Gateway**: If using private subnets (not created by default)

**Tip:** Use the `cleanup_deployment()` method with `cleanup_infrastructure=True` to delete everything when done testing.

## Required IAM Permissions

Your AWS credentials need these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateVpc",
        "ec2:CreateSubnet",
        "ec2:CreateSecurityGroup",
        "ec2:CreateInternetGateway",
        "ec2:CreateRouteTable",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:ModifyVpcAttribute",
        "ec2:ModifySubnetAttribute",
        "ec2:AttachInternetGateway",
        "ec2:CreateRoute",
        "ec2:AssociateRouteTable",
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeAvailabilityZones",
        "ec2:DescribeNetworkInterfaces",
        "ec2:CreateTags",
        "ec2:Delete*",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "iam:GetRole",
        "iam:PassRole",
        "iam:ListAttachedRolePolicies",
        "iam:DetachRolePolicy",
        "iam:DeleteRole",
        "ecs:CreateCluster",
        "ecs:CreateService",
        "ecs:RegisterTaskDefinition",
        "ecs:RunTask",
        "ecs:DescribeClusters",
        "ecs:DescribeServices",
        "ecs:DescribeTasks",
        "ecs:ListTasks",
        "ecs:UpdateService",
        "ecs:DeleteService",
        "ecs:DeregisterTaskDefinition",
        "ecs:DeleteCluster",
        "ecs:StopTask",
        "elasticloadbalancing:CreateLoadBalancer",
        "elasticloadbalancing:CreateTargetGroup",
        "elasticloadbalancing:CreateListener",
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTargetGroups",
        "elasticloadbalancing:DeleteLoadBalancer",
        "elasticloadbalancing:DeleteTargetGroup",
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:DescribeRepositories",
        "ecr:DeleteRepository",
        "logs:CreateLogGroup",
        "logs:DeleteLogGroup"
      ],
      "Resource": "*"
    }
  ]
}
```

## Troubleshooting

### Error: "Missing required environment variables"
**Solution:** Ensure `DE_BENCH_ECS_CLUSTER_NAME` is set in your `.env` file.

### Error: "VPC limit exceeded"
**Solution:** You may have reached the AWS VPC limit (5 per region by default). Either:
- Delete unused VPCs
- Request a limit increase
- Use existing VPC by providing `DE_BENCH_ECS_SUBNET_IDS`

### Error: "IAM role already exists"
**Solution:** The manager will automatically use the existing role. This is not an error.

### Deployment slow or timing out
**Solution:**
- Check security group rules allow traffic
- Verify subnets have internet connectivity
- Check ECS task logs in CloudWatch

### Infrastructure not being deleted
**Solution:** Resources may have dependencies. The cleanup order is:
1. Security groups
2. Subnets
3. Route tables
4. Internet gateway
5. VPC
6. IAM roles
7. ECS cluster

If cleanup fails, you may need to manually delete resources in the AWS Console.

## Migration Guide

If you were using the old ECS ManifestManager with manual infrastructure:

### Old Way (Manual):
1. Create VPC manually
2. Create subnets manually
3. Create security groups manually
4. Create IAM roles manually
5. Create ECS cluster manually
6. Add all IDs to `.env` file
7. Run deployment

### New Way (Automatic):
1. Set cluster name in `.env`
2. Run deployment
3. Done!

### Backward Compatibility

The new version is **backward compatible**. If you provide the old environment variables (`DE_BENCH_ECS_SUBNET_IDS`, etc.), the manager will use them instead of creating new resources.

## Summary

The updated ECS ManifestManager provides:

✅ **Automatic infrastructure creation**
✅ **Minimal configuration required**
✅ **Backward compatible with existing setups**
✅ **Safe cleanup with infrastructure tracking**
✅ **Consistent with Kubernetes ManifestManager UX**
✅ **Resource tagging for easy management**
✅ **Clear error messages**

Now deploying to ECS is as simple as deploying to Kubernetes!
