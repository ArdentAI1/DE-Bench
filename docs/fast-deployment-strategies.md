# Fast Deployment Strategies for AWS

## The Problem
Load Balancer provisioning takes 2-5 minutes on **all** cloud platforms (AWS, Azure, GCP). This is a fundamental limitation of cloud load balancers.

## Solutions

### 1. Pre-create and Reuse Load Balancer (Fastest - ~30 seconds)

Create ONE load balancer and target group upfront, then reuse it for all deployments.

**Pros:**
- Deployment: ~30 seconds (no LB creation)
- Works with both ECS and EKS
- Cost-effective (one LB for multiple services)

**Cons:**
- All services share the same LB endpoint
- Need to use different ports or path-based routing

**Implementation for ECS:**

```python
# One-time setup: Create shared load balancer
from Environment.ECS.ManifestManager import ECSManifestManager

manager = ECSManifestManager()
lb_arn, tg_arn = manager.create_load_balancer_and_target_group("shared")

# Save these to .env
print(f"DE_BENCH_ECS_SHARED_LB_ARN={lb_arn}")
print(f"DE_BENCH_ECS_SHARED_TG_ARN={tg_arn}")

# Deploy without creating new LB (fast!)
result = manager.generate_and_deploy(
    namespace="my-test",
    container_image="...",
    use_service=True,
    enable_load_balancer=False  # Don't create new LB
)
```

### 2. Use ECS with Public IP (No Load Balancer - ~60 seconds)

Skip the load balancer entirely and use the task's public IP directly.

**Pros:**
- Deployment: ~60 seconds
- No load balancer cost (~$16/month savings)
- Simple architecture

**Cons:**
- IP changes if task restarts
- No automatic failover
- Not suitable for production
- Good for testing/development

**Implementation:**

```bash
# Deploy without load balancer
uv run python Environment/ECS/ManifestManager.py deploy my-test \
  --container "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow:base" \
  --service  # No --load-balancer flag

# Or use standalone task (even faster - no service)
uv run python Environment/ECS/ManifestManager.py deploy my-test \
  --container "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow:base"
  # No --service flag
```

### 3. AWS App Runner (Serverless - ~90 seconds)

AWS App Runner is a fully managed service that deploys containers without managing infrastructure.

**Pros:**
- Deployment: ~90 seconds
- Auto-scaling
- No infrastructure management
- Built-in load balancing
- Pay-per-use

**Cons:**
- Less control than ECS
- Different API
- Not compatible with existing code

**Not yet implemented in DE-Bench**

### 4. EKS with Ingress Controller (One-time LB - ~45 seconds after setup)

Use AWS Load Balancer Controller with Ingress instead of Service LoadBalancer.

**Pros:**
- Deployment: ~45 seconds (after initial setup)
- One ALB for many services
- Path-based routing
- More K8s-native

**Cons:**
- Initial setup complexity
- Requires Ingress controller installation

**Implementation:**

```bash
# One-time: Install AWS Load Balancer Controller
helm repo add eks https://aws.github.io/eks-charts
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --set clusterName=your-cluster-name

# Then use Ingress instead of LoadBalancer service
# Deployment is fast because LB already exists
```

### 5. Lambda + API Gateway (Serverless - ~10 seconds)

For true serverless with fastest cold starts.

**Pros:**
- Deployment: ~10 seconds
- No infrastructure
- Pay-per-request
- Auto-scaling

**Cons:**
- 15-minute execution limit
- Different architecture (not container-based)
- Not suitable for long-running tasks like Airflow

## Comparison Table

| Solution | Deployment Time | Cost | Complexity | Production Ready | Best For |
|----------|----------------|------|------------|------------------|----------|
| Pre-created LB | ~30s | $ | Low | ✅ Yes | Multiple services |
| ECS Public IP | ~60s | $ | Very Low | ❌ Dev only | Testing/Dev |
| ECS + New LB | ~180s | $$ | Low | ✅ Yes | Single service |
| EKS + New LB | ~180s | $$$ | Medium | ✅ Yes | K8s workloads |
| EKS + Ingress | ~45s (after setup) | $$ | High | ✅ Yes | Many K8s services |
| App Runner | ~90s | $$ | Very Low | ✅ Yes | Simple containers |
| Lambda | ~10s | $ | Medium | ✅ Yes | Short tasks |

## Recommended Approach for DE-Bench

### For Testing/Development (Fastest)
```bash
# No load balancer - use public IP
uv run python Environment/ECS/ManifestManager.py deploy test-1 \
  --container "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/airflow:base"
```

**Deployment time: ~60 seconds**

### For Production (Best Balance)
```bash
# 1. One-time: Create shared load balancer
uv run python -c "
from Environment.ECS.ManifestManager import ECSManifestManager
m = ECSManifestManager()
lb, tg = m.create_load_balancer_and_target_group('shared')
print(f'LB: {lb}')
print(f'TG: {tg}')
"

# 2. Add to .env:
# DE_BENCH_ECS_SHARED_TG_ARN=arn:aws:elasticloadbalancing:...

# 3. Modify code to use shared target group
# Then deployments take ~30 seconds
```

## Why is Load Balancer Creation Slow?

Load balancers are slow to create because they involve:
1. **DNS propagation** - Creating DNS records
2. **Network provisioning** - Attaching to subnets in multiple AZs
3. **Health checks** - Setting up health monitoring
4. **SSL/TLS** - Certificate provisioning (if using HTTPS)
5. **AWS internal validation** - Security and compliance checks

This is the same on **all cloud providers**:
- AWS ALB/NLB: 2-5 minutes
- Azure Load Balancer: 2-4 minutes
- GCP Load Balancer: 2-6 minutes

## Bottom Line

**For fastest deployments in DE-Bench:**

1. **Development/Testing**: Skip load balancer entirely
   ```bash
   # ~60 seconds
   python Environment/ECS/ManifestManager.py deploy test --container <image>
   ```

2. **Production**: Pre-create one load balancer, reuse for all services
   ```bash
   # First time: 3 minutes
   # Subsequent: 30 seconds
   ```

3. **If you need Kubernetes**: Use EKS with Ingress Controller
   ```bash
   # After setup: ~45 seconds per deployment
   ```

The ECS solution you're using is actually **the fastest** for container deployments in AWS when you skip the load balancer or reuse an existing one!
