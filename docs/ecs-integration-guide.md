# ECS Integration Guide

This document describes the integration of AWS ECS deployment support into the DE-Bench evaluation framework, following the same pattern as the existing AKS (Kubernetes) integration.

## Overview

The ECS integration allows Airflow tests to deploy to AWS ECS Fargate instead of Astronomer or Azure Kubernetes Service. This provides a third deployment option with the following benefits:

- **Fast Deployment**: ~60 seconds without load balancer, ~2-5 minutes with load balancer
- **Stable Endpoints**: Service endpoints remain stable across container updates
- **Parallel Execution**: Supports 10+ simultaneous deployments without queue bottlenecks
- **Automatic Infrastructure**: VPC, subnets, security groups, and IAM roles are created automatically

## Architecture

### Files Modified

1. **`Fixtures/Airflow/airflow_fixture.py`**
   - Added ECS-specific configuration fields to `AirflowResourceConfig`
   - Added ECS-specific data fields to `AirflowResourceData`
   - Updated `session_setup()` to support ECS mode
   - Created `_setup_ecs_airflow()` method (mirrors `_setup_kubernetes_airflow()`)
   - Created `_cleanup_ecs_airflow()` method (mirrors `_cleanup_kubernetes_airflow()`)
   - Updated `test_setup()` to route ECS deployments
   - Updated `test_teardown()` to handle ECS cleanup

2. **`Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py`**
   - Updated `get_fixtures()` to support three deployment modes (Astro, Kubernetes, ECS)
   - Updated `validate_test()` to construct ECS-specific build_info for GitHub Actions

### New Configuration Fields

**AirflowResourceConfig (TypedDict)**:
- `use_ecs: Optional[bool]` - Enable ECS deployment mode
- `ecs_namespace: Optional[str]` - ECS namespace (like Kubernetes namespace)
- `enable_load_balancer: Optional[bool]` - Enable ALB for ECS (default: True)

**AirflowResourceData (TypedDict)**:
- `ecs_manager: Optional[Any]` - ECSManifestManager instance
- `ecs_namespace: Optional[str]` - ECS namespace used
- `ecs_endpoint: Optional[str]` - ECS endpoint (DNS or public IP)

## Usage

### Environment Variables Required

For ECS deployment, set these environment variables:

```bash
# Deployment mode
export USE_ECS_AIRFLOW=true

# AWS Configuration
export DE_BENCH_ECS_CLUSTER_NAME=my-ecs-cluster
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...

# Container Image
export AIRFLOW_CONTAINER_IMAGE=123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest

# Optional: Use existing infrastructure
export DE_BENCH_ECS_SUBNET_IDS=subnet-xxx,subnet-yyy
export DE_BENCH_ECS_SECURITY_GROUP_IDS=sg-xxx
export DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN=arn:aws:iam::...

# Optional: Disable load balancer for faster deployment
export ECS_ENABLE_LOAD_BALANCER=false

# GitHub Integration (for build_info)
export AWS_ACCOUNT_ID=123456789012
```

### Running Tests with ECS

```bash
# Set deployment mode to ECS
export USE_ECS_AIRFLOW=true

# Run the test
python -m pytest Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py
```

### Deployment Modes Comparison

The test now supports three mutually exclusive deployment modes:

| Mode | Environment Variable | Infrastructure |
|------|---------------------|----------------|
| Astro | Default (or `USE_ASTRO_AIRFLOW=true`) | Astronomer Cloud |
| Kubernetes | `USE_KUBERNETES_AIRFLOW=true` | Azure AKS |
| ECS | `USE_ECS_AIRFLOW=true` | AWS ECS Fargate |

## Implementation Details

### ECS Deployment Flow

1. **Session Setup** (`session_setup`)
   - Detects `USE_ECS_AIRFLOW=true` environment variable
   - Validates required environment variables (cluster name, region)
   - Skips Astronomer cache manager initialization (not needed for ECS)
   - Returns session data with `use_ecs=True`

2. **Test Setup** (`_setup_ecs_airflow`)
   - Initializes `ECSManifestManager` from `Environment/ECS/ManifestManager.py`
   - Deploys Airflow container to ECS using `generate_and_deploy()`
   - Retrieves endpoint (load balancer DNS or task public IP)
   - Verifies service health
   - Creates `AirflowManager` instance pointing to ECS deployment
   - Waits for Airflow to be ready
   - Returns `AirflowResourceData` with ECS-specific fields

3. **Test Teardown** (`_cleanup_ecs_airflow`)
   - Cleans up ECS deployment (service, task definitions, load balancer)
   - Deletes ECR repository if created
   - Cleans up temporary directories
   - Does NOT cleanup shared infrastructure (VPC, subnets) by default

### GitHub Actions Integration

The test provides ECS-specific build information to GitHub Actions workflows:

```python
build_info = {
    "ecrRegistry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
    "ecrRepository": "airflow-namespace",
    "ecsCluster": "my-ecs-cluster",
    "ecsNamespace": "airflow-namespace",
    "ecsServiceName": "airflow-namespace-service",
}
```

This allows GitHub Actions to:
1. Build and push container images to ECR
2. Update ECS task definitions
3. Force new deployment to ECS service

### Load Balancer vs Direct Access

**With Load Balancer** (`enable_load_balancer: true`, DEFAULT):
- **Deployment time**: ~2-5 minutes (ALB provisioning)
- **Endpoint**: `http://<alb-dns-name>` (port 80)
- **Stability**: DNS name never changes, even across redeployments
- **Benefits**:
  - Production-ready with health checks
  - Standard port 80 (no :8080 suffix)
  - Can scale to multiple tasks
  - Handles SSL termination (if configured)
- **Cost**: ~$16-20/month for ALB + task costs
- **Use case**: Production deployments, stable URLs needed across CI/CD runs

**Without Load Balancer** (`enable_load_balancer: false`):
- **Deployment time**: ~60-90 seconds (task startup + IP assignment)
- **Endpoint**: `http://<task-public-ip>:8080`
- **Stability**: IP changes on every redeployment
- **Benefits**:
  - Faster for one-off testing
  - Lower cost (task costs only)
  - Simpler infrastructure
- **Limitations**:
  - **IP changes**: Each redeployment gets new IP (breaks bookmarks/links)
  - **Timing**: Need to wait for task to start and get IP (60-90s)
  - **Port 8080**: Non-standard port
  - **Single task**: Can't horizontally scale
- **Use case**: Quick local testing, temporary environments

**Recommendation**: Keep load balancer enabled (default) for evaluation tests because:
1. The test framework expects stable endpoints across GitHub Actions runs
2. The extra 2-3 minutes is worth the reliability
3. Your workflow does 10+ parallel deployments - load balancer provides better isolation

## Comparison with Kubernetes Integration

The ECS integration follows the exact same pattern as Kubernetes:

| Aspect | Kubernetes | ECS |
|--------|-----------|-----|
| Manager Class | `KubernetesManifestManager` | `ECSManifestManager` |
| Setup Method | `_setup_kubernetes_airflow()` | `_setup_ecs_airflow()` |
| Cleanup Method | `_cleanup_kubernetes_airflow()` | `_cleanup_ecs_airflow()` |
| Namespace Field | `k8s_namespace` | `ecs_namespace` |
| Manager Field | `k8s_manager` | `ecs_manager` |
| Endpoint Field | `external_ip` | `ecs_endpoint` |
| Infrastructure | Azure (VNet, AKS, ACR) | AWS (VPC, ECS, ECR) |

## Error Handling

The integration includes comprehensive error handling:

1. **Deployment Failures**: Automatic cleanup of ECS resources on setup failure
2. **Partial Setup**: Cleanup of partially initialized resources in `_test_teardown`
3. **Validation**: Prevents using both Kubernetes and ECS modes simultaneously
4. **Missing Configuration**: Clear error messages for missing environment variables

## Testing the Integration

### Minimal Test

```python
from Fixtures.Airflow.airflow_fixture import AirflowFixture

# Create ECS-based Airflow fixture
config = {
    "resource_id": "test_ecs_deployment",
    "use_ecs": True,
    "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest",
    "ecs_namespace": "test-deployment",
    "enable_load_balancer": False,  # Faster for testing
}

fixture = AirflowFixture(custom_config=config)

# Setup will create ECS deployment
resource_data = fixture._test_setup()

print(f"Airflow URL: {resource_data['base_url']}")
print(f"ECS Namespace: {resource_data['ecs_namespace']}")

# Cleanup
fixture._test_teardown()
```

### Full Test with GitHub Integration

Run the complete test:

```bash
export USE_ECS_AIRFLOW=true
export DE_BENCH_ECS_CLUSTER_NAME=test-cluster
export AIRFLOW_CONTAINER_IMAGE=123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow:latest

python -m pytest Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py -v
```

## Troubleshooting

### Common Issues

1. **"Failed to get public IP" when `enable_load_balancer=false`**
   - **Cause**: Task hasn't started or IP not assigned yet
   - **Fix**: The code now waits up to 5 minutes for task IP
   - **Workaround**: Enable load balancer (default) for reliable endpoints
   - **Debug**: Check CloudWatch logs for task startup issues

2. **Missing AWS Credentials**
   - Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set
   - Or use IAM role credentials

3. **VPC Creation Failures**
   - Check IAM permissions (see `docs/required-iam-policy.json`)
   - Or provide existing subnets via `DE_BENCH_ECS_SUBNET_IDS`

4. **Service Health Check Failures**
   - Verify security groups allow ingress on port 8080 (or 80 for ALB)
   - Check CloudWatch logs for container errors
   - Wait for Airflow to fully initialize (~60-120s after task starts)

5. **Slow Deployments**
   - If using load balancer, expect 2-5 minutes for ALB provisioning
   - This is normal AWS behavior - ALB provisioning is slow
   - Without load balancer: expect 60-90 seconds for task IP

### Debug Mode

Enable verbose logging:

```python
# In your test
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Future Enhancements

1. **Service Discovery Integration**: Use the existing `ServiceDiscoveryManager` for even faster deployments
2. **Multi-Region Support**: Deploy to different AWS regions in parallel
3. **Cost Optimization**: Automatically clean up old deployments
4. **Health Checks**: More sophisticated health checks beyond basic HTTP requests

## Related Documentation

- [ECS ManifestManager Implementation](../Environment/ECS/ManifestManager.py)
- [ECS Cleanup Guide](./cleanup-guide.md)
- [Fast Deployment Strategies](./fast-deployment-strategies.md)
- [Required IAM Policy](./required-iam-policy.json)
