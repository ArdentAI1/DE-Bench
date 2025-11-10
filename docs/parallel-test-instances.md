# Parallel Test Instances

## Overview

The `--num-instances` (or `-i`) flag allows you to create multiple parallel instances of the same test, each with unique identifiers and isolated resources. This is useful for:

- **Load testing**: Run 10+ instances of the same test simultaneously to test ECS/Kubernetes infrastructure
- **Reliability testing**: Verify that multiple deployments don't interfere with each other
- **CI/CD stress testing**: Simulate parallel PR merges and deployments

## How It Works

### Without `--num-instances` (default)

```bash
python run_braintrust_eval.py --filter "Hello.*" Ardent
```

Creates **1 instance** of each matching test:
- `hello_universe_pipeline_test_1234567890_abc12345`

### With `--num-instances 10`

```bash
python run_braintrust_eval.py --num-instances 10 --filter "Hello.*" Ardent
```

Creates **10 instances** of each matching test:
- `hello_universe_pipeline_test_1234567890_abc12345_inst_1`
- `hello_universe_pipeline_test_1234567890_abc12345_inst_2`
- `hello_universe_pipeline_test_1234567890_abc12345_inst_3`
- ... (10 instances total)

Each instance gets:
- ✅ **Unique resource_id** - ECS namespace, Kubernetes namespace, etc.
- ✅ **Isolated resources** - Separate ECS services, Kubernetes pods, load balancers
- ✅ **Independent execution** - Failures in one instance don't affect others
- ✅ **Parallel deployment** - All instances deploy simultaneously

## Use Cases

### Use Case 1: Test Parallel ECS Deployments

Verify that 10 simultaneous ECS deployments work correctly:

```bash
python run_braintrust_eval.py \
  --num-instances 10 \
  --filter "Airflow_Agent_Hello_Universe_Pipeline" \
  Ardent
```

**What happens:**
- Creates 10 ECS services: `hello-universe-...-inst-1` through `hello-universe-...-inst-10`
- Each gets its own load balancer (or direct IP if LB disabled)
- Each has isolated namespace and resources
- All deploy in parallel (respects max_concurrency)

**Expected behavior:**
- ✅ All 10 instances deploy successfully
- ✅ No resource conflicts (VPC, subnet, security group sharing works)
- ✅ Each instance accessible at its own endpoint
- ✅ Cleanup removes all 10 instances

### Use Case 2: Kubernetes Load Testing

Test that AKS can handle multiple simultaneous namespace creations:

```bash
export USE_KUBERNETES_AIRFLOW=true

python run_braintrust_eval.py \
  --num-instances 15 \
  --filter "Airflow.*" \
  Claude_Code
```

**What happens:**
- Creates 15 Kubernetes namespaces
- Each namespace gets its own deployment, service, and load balancer
- Tests run in parallel
- Cleans up all 15 namespaces after completion

### Use Case 3: Multiple Tests with Multiple Instances

Run 2 different tests, each with 5 instances:

```bash
python run_braintrust_eval.py \
  --num-instances 5 \
  --filter "MongoDB.*" "MySQL.*" \
  Ardent
```

**What happens:**
- MongoDB test: 5 instances
- MySQL test: 5 instances
- Total: 10 test instances running in parallel

## Difference from `--num-trials`

### `--num-trials` (Braintrust Trials)

```bash
python run_braintrust_eval.py --num-trials 3 --filter "Hello.*" Ardent
```

- Runs the **same test 3 times sequentially** (or with Braintrust's built-in parallelism)
- **Reuses the same resources** for all 3 trials
- Used for statistical validation (does the test pass 3 times in a row?)
- Tracked as separate trials in Braintrust UI

### `--num-instances` (Parallel Instances)

```bash
python run_braintrust_eval.py --num-instances 3 --filter "Hello.*" Ardent
```

- Creates **3 separate test instances with unique resources**
- Each instance has its own ECS service, namespace, load balancer
- All run **in parallel**
- Used for load testing and parallel execution validation

### Combining Both

```bash
python run_braintrust_eval.py \
  --num-trials 2 \
  --num-instances 5 \
  --filter "Hello.*" \
  Ardent
```

- Creates **5 instances** of the test
- Each instance runs **2 trials**
- Total: 10 executions (5 instances × 2 trials)

## Resource Identifier Scheme

### Original Resource ID

From the test configuration:
```python
resource_id = f"hello_universe_pipeline_test_{test_timestamp}_{test_uuid}"
# Example: hello_universe_pipeline_test_1762672629_2ecd4cad
```

### With `--num-instances 10`

Instance 1:
```python
resource_id = "hello_universe_pipeline_test_1762672629_2ecd4cad_inst_1"
```

Instance 10:
```python
resource_id = "hello_universe_pipeline_test_1762672629_2ecd4cad_inst_10"
```

### ECS Deployment Example

**Instance 1:**
```properties
ecsNamespace=hello-universe-pipeline-test-1762672629-2ecd4cad-inst-1
ecsServiceName=hello-universe-pipeline-test-1762672629-2ecd4cad-inst-1-service
ecrRepository=hello-universe-pipeline-test-17-inst-1  # Truncated to 32 chars
```

**Instance 2:**
```properties
ecsNamespace=hello-universe-pipeline-test-1762672629-2ecd4cad-inst-2
ecsServiceName=hello-universe-pipeline-test-1762672629-2ecd4cad-inst-2-service
ecrRepository=hello-universe-pipeline-test-17-inst-2
```

All have **unique identifiers** = no resource conflicts!

## Concurrency Control

By default, max_concurrency is 20. Use `--full-concurrency` to run all instances truly in parallel:

```bash
# Default: max 20 concurrent tasks
python run_braintrust_eval.py --num-instances 50 --filter "Hello.*" Ardent

# Full concurrency: all 50 run in parallel
python run_braintrust_eval.py \
  --num-instances 50 \
  --full-concurrency \
  --filter "Hello.*" \
  Ardent
```

**Note:** AWS has limits on parallel ECS service creations (~10-20 per region). If you exceed this, some instances will wait in the queue.

## Monitoring Multiple Instances

### Braintrust UI

Each instance appears as a separate task in Braintrust:
- `hello_universe_pipeline_test_..._inst_1`
- `hello_universe_pipeline_test_..._inst_2`
- etc.

You can filter, compare, and analyze each instance independently.

### AWS Console

**ECS Services:**
```
my-cluster:
  - hello-universe-...-inst-1-service (ACTIVE)
  - hello-universe-...-inst-2-service (ACTIVE)
  - hello-universe-...-inst-3-service (ACTIVE)
  ...
```

**Load Balancers:**
```
- hello-universe-...-inst-1-lb
- hello-universe-...-inst-2-lb
- hello-universe-...-inst-3-lb
```

Each is completely isolated!

## Cleanup

All instances are cleaned up automatically when the test completes:

```
🧹 Tearing down hello_universe_pipeline_test_..._inst_1
  ✅ Deleted ECS service: hello-universe-...-inst-1-service
  ✅ Deleted load balancer: hello-universe-...-inst-1-lb
  ✅ Deleted target group: hello-universe-...-inst-1-tg

🧹 Tearing down hello_universe_pipeline_test_..._inst_2
  ✅ Deleted ECS service: hello-universe-...-inst-2-service
  ...
```

Cleanup happens in parallel (fastest cleanup possible).

## Common Patterns

### Pattern 1: Quick Parallel Test

Test that 5 instances work:
```bash
python run_braintrust_eval.py -i 5 --filter "Hello.*" Ardent
```

### Pattern 2: Load Test with Metrics

Run 20 instances and measure deployment times:
```bash
python run_braintrust_eval.py \
  -i 20 \
  --full-concurrency \
  --filter "Airflow.*" \
  Ardent
```

Check Braintrust for deployment time distribution across all 20 instances.

### Pattern 3: Multiple Tests, Multiple Instances

```bash
python run_braintrust_eval.py \
  -i 10 \
  --filter "MongoDB.*" "Airflow.*" \
  Ardent
```

Creates:
- 10 instances of MongoDB test
- 10 instances of Airflow test
- Total: 20 instances

### Pattern 4: Stress Test

```bash
python run_braintrust_eval.py \
  -i 50 \
  --full-concurrency \
  --filter ".*" \
  Ardent
```

Creates 50 instances of **every** test (use with caution - may hit AWS limits!).

## Limitations

1. **AWS Service Limits**:
   - ECS service creations: ~20 parallel per region
   - Load balancers: ~20 parallel per region
   - Exceeding limits causes queuing (not failures)

2. **Namespace Uniqueness**:
   - Each instance must have unique resource_id
   - Ensured by appending `_inst_N`

3. **Memory/CPU**:
   - Each instance uses resources
   - 50 instances = 50 ECS tasks running simultaneously
   - Monitor your account limits

## Cost Estimation

**Example: 10 instances of Hello Universe test**

Per instance (1 hour):
- ECS task (1 vCPU, 2GB): $0.04
- NLB: $0.0225
- Total per instance: ~$0.06/hour

10 instances:
- $0.60/hour
- For 10-minute test: ~$0.10 total

**Conclusion:** Very affordable for testing!

## Troubleshooting

### Error: "Resource already exists"

**Problem:** Two instances tried to use the same resource_id

**Solution:** The code should prevent this. If it happens, file a bug report with the full error trace.

### Error: "Rate limit exceeded"

**Problem:** Too many parallel AWS API calls

**Solution:**
- Reduce `--num-instances` (try 10-20)
- Don't use `--full-concurrency` with high instance counts

### Some instances fail, others succeed

**Expected behavior!** This is useful for finding flaky infrastructure issues:
- If 1/10 fails → investigate why
- If 5/10 fail → possible AWS service issue or quota limit
- If 10/10 fail → bug in test or deployment code

## Summary

`--num-instances` is perfect for:
- ✅ Load testing your deployment infrastructure
- ✅ Verifying parallel deployments work
- ✅ Finding race conditions or resource conflicts
- ✅ Stress testing CI/CD pipelines

Use it when you want **multiple isolated copies** of the same test running simultaneously.
