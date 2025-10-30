# Kubernetes Manifest Manager

A Python-based tool for automating the generation and deployment of Kubernetes manifests to Azure Kubernetes Service (AKS). This tool streamlines the process of creating isolated, sandboxed Airflow environments with standardized Kubernetes resources for DE-Bench evaluations.

## Key Benefits

### 1. **Cost Efficiency**
- **No Astronomer Fees**: Eliminate expensive Astro deployment costs by running Airflow directly on your own AKS cluster
- **Resource Control**: Pay only for the Kubernetes resources you use, with granular control over pod sizing
- **Automatic Cleanup**: Built-in resource cleanup prevents runaway costs from forgotten deployments

### 2. **Faster Test Execution**
- **Rapid Deployment**: Deploy Airflow instances in seconds vs. minutes with Astro
- **Parallel Testing**: Run multiple isolated Airflow instances simultaneously for parallel test execution
- **No Rate Limits**: Avoid Astro API rate limits and deployment quotas

### 3. **Full Control & Flexibility**
- **Custom Container Images**: Use any Airflow container image without registry restrictions
- **Environment Variables**: Full control over Airflow configuration and environment
- **Direct API Access**: Direct access to Kubernetes and Airflow APIs without intermediary services

### 4. **Evaluation Framework Integration**
- **Fixture Pattern**: Seamlessly integrates with DEBenchFixture for test setup/teardown
- **External IP Exposure**: Automatic LoadBalancer provisioning with external IP for AI agent access
- **Health Monitoring**: Built-in health checks ensure Airflow is ready before tests run

## Features

- **Automated Manifest Generation**: Create Kubernetes manifests from templates with configurable namespaces and container images
- **Azure AKS Integration**: Seamless authentication and deployment to Azure Kubernetes Service using service principals
- **Batch Deployment**: Deploy multiple namespaced Airflow environments simultaneously for testing and benchmarking
- **Job Management**: Handle immutable Kubernetes Job resources with reapplication capabilities
- **LoadBalancer Provisioning**: Automatically create external-facing services with IP tracking for agent access
- **Health Monitoring**: Built-in health check verification for deployed Airflow services
- **Programmatic API**: Python class API for integration with test frameworks and fixtures

## Configuration

Ensure your `.env` file is in the project root with your Azure credentials:

```bash
# Azure Service Principal Authentication
AZURE_CLIENT_ID="your-client-id"
AZURE_CLIENT_SECRET="your-client-secret"
AZURE_TENANT_ID="your-tenant-id"
AZURE_SUBSCRIPTION_ID="your-subscription-id"

# AKS Cluster Information
DE_BENCH_AKS_RESOURCE_GROUP="your-resource-group"
DE_BENCH_AKS_CLUSTER_NAME="your-cluster-name"
DE_BENCH_AKS_IMAGE_NAME="your-default-container-image"
```

**Security Note**: Never commit the `.env` file to version control. It contains sensitive credentials.

## Usage

### Programmatic Usage (Recommended for Fixtures)

The primary use case is programmatic integration with DEBench fixtures:

```python
from Environment.Kubernetes.ManifestManager import KubernetesManifestManager

# Initialize the manager
k8s_manager = KubernetesManifestManager(provider="AZURE")

# Deploy Airflow to Kubernetes
namespace = "my-airflow-test"
container = "airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base"

k8s_manager.generate_and_apply_manifest(
    namespace=namespace,
    container=container
)

# Get external IP from LoadBalancer
external_ip = k8s_manager.get_service_external_ip(namespace=namespace)
print(f"Airflow available at: http://{external_ip}:8080")

# Verify pod health
if k8s_manager.verify_pod_health(external_ip=external_ip, port=8080):
    print("Airflow is healthy and ready!")

# Cleanup when done
k8s_manager.delete_job(job_name=namespace, namespace=namespace)
```

### Integration with AirflowFixture

The ManifestManager integrates seamlessly with the AirflowFixture for Kubernetes-based deployments:

```python
from Fixtures.Airflow.airflow_fixture import AirflowFixture

# Configure Airflow to use Kubernetes
custom_airflow_config = {
    "resource_id": "my_test_12345",
    "use_kubernetes": True,  # Enable Kubernetes deployment
    "container_image": "airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base",
}

# Create fixture - ManifestManager is used internally
airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)

# After setup, access the Kubernetes deployment details
resource_data = airflow_fixture._resource_data
print(f"Airflow URL: {resource_data['base_url']}")
print(f"External IP: {resource_data['external_ip']}")
print(f"Namespace: {resource_data['k8s_namespace']}")
```

### CLI Usage

The ManifestManager also provides a command-line interface:

#### Generate Manifest Only
```bash
python Environment/Kubernetes/ManifestManager.py generate <namespace> \
  --container <image> \
  --output <output-file.yml>
```

Example:
```bash
python Environment/Kubernetes/ManifestManager.py generate my-airflow \
  --container airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base \
  --output my-manifest.yml
```

#### Apply Existing Manifest
```bash
python Environment/Kubernetes/ManifestManager.py apply <manifest-file.yml>
```

#### Generate and Apply in One Step
```bash
python Environment/Kubernetes/ManifestManager.py generate-and-apply <namespace> \
  --container <image> \
  [--save <output-file.yml>]
```

Example:
```bash
python Environment/Kubernetes/ManifestManager.py generate-and-apply test-airflow \
  --container airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base \
  --save test-manifest.yml
```

#### Reapply Job (Update Container)
```bash
python Environment/Kubernetes/ManifestManager.py reapply-job <namespace> <new-container-image>
```

This command handles the immutability of Kubernetes Job specs by:
1. Deleting the existing Job
2. Generating a new manifest with the updated container
3. Creating the new Job resource

## Profiling and Batch Deployment

The `profile()` function provides performance testing and benchmarking capabilities by creating multiple namespaced Airflow environments simultaneously.

### How It Works

```python
from Environment.Kubernetes.ManifestManager import profile

# Deploy 10 Airflow instances and measure performance
profile(
    number_of_instances=10,
    container="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base"
)
```

### Profile Behavior

- **Namespace Creation**: Generates namespaces named `profile-namespace-{1..N}`
- **Alternating Containers**:
  - **Even-numbered namespaces**: Use a failing container image for error testing
  - **Odd-numbered namespaces**: Use the default container image
- **Performance Metrics**: Tracks timing for each operation:
  - Initialization time
  - Manifest generation time
  - Manifest save time
  - Application time
  - External IP retrieval time
  - Health check time
- **Health Verification**: Sends HTTP GET requests to `http://{external_ip}:8080/health`
- **Reporting**: Outputs total time and average time per namespace

### Example Output

```
Profile Instance 1:
  Initialization: 2.34s
  Manifest Generation: 0.01s
  Manifest Save: 0.02s
  Application: 5.67s
  External IP Retrieval: 45.23s
  Health Check: 0.15s
  Total: 53.42s

Profile Instance 2:
  ...

Total Profile Time: 534.20s
Average Time per Instance: 53.42s
```

## Project Structure

```
DE-Bench/
├── Environment/
│   └── Kubernetes/
│       ├── ManifestManager.py       # KubernetesManifestManager class
│       ├── __init__.py             # Module initialization
│       └── README_ManifestManager.md  # This documentation
├── Fixtures/
│   └── Airflow/
│       └── airflow_fixture.py      # AirflowFixture with K8s support
├── Tests/
│   └── Airflow_Agent_Hello_World_Failure/
│       └── test_airflow_agent_hello_universe_pipeline.py
└── .env                            # Environment variables (not in repo)
```

## How It Works

### Architecture Flow

1. **Initialize Manager**
   - Set cloud provider (currently supports Azure)
   - Get cloud provider client (Azure ContainerServiceClient)

2. **Authenticate and Get Kubernetes Client**
   - Authenticate with Azure using service principal
   - Retrieve AKS cluster credentials
   - Write kubeconfig to `~/.kube/config`
   - Initialize Kubernetes API clients (CoreV1Api and BatchV1Api)

3. **Generate Manifest**
   - Use predefined MANIFEST_TEMPLATE
   - Replace placeholders:
     - `NAMESPACE_VALUE` → namespace name
     - `CONTAINER_NAME` → container image
   - Return generated YAML string

4. **Apply Manifest**
   - Parse multi-document YAML manifest
   - For each resource (Namespace, Job, Service):
     - Extract resource metadata (kind, name, namespace)
     - Create resource via appropriate Kubernetes API
     - Handle conflicts (409) for existing resources

5. **Monitor Deployment** (Optional)
   - Poll LoadBalancer service for external IP assignment
   - Perform health checks on deployed services

### Generated Resources

Each manifest creates three Kubernetes resources:

#### 1. Namespace
Isolated environment for the deployment

#### 2. Job
Batch workload with:
- **Active Deadline**: 12 hours (43200 seconds)
- **Container Port**: 8080
- **Environment Variable**: `IS_SANDBOX=1`
- **Backoff Limit**: 4 retries
- **Restart Policy**: Never
- **Labels**: `user-id` for tracking

#### 3. Service (LoadBalancer)
External access configuration:
- **Type**: LoadBalancer
- **Port Mapping**: 8080 → targetPort 8080
- **Selector**: `user-id` label

### Manifest Template

The tool uses a predefined template with configurable placeholders (from ManifestManager.py:23-74):

```yaml
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
```

## Error Handling

The tool includes robust error handling for common scenarios:

- **409 Conflicts**: Gracefully handles existing resources
- **404 Not Found**: Handles missing jobs during deletion
- **Provider Validation**: Validates cloud provider support
- **Authentication Failures**: Raises exceptions for credential issues

## Dependencies

- `kubernetes==33.1.0` - Kubernetes Python client
- `azure-common==1.1.28` - Azure common libraries
- `azure-core==1.35.0` - Azure core functionality
- `azure-identity==1.24.0` - Azure authentication
- `azure-mgmt-containerservice==39.1.0` - Azure AKS management
- `azure-mgmt-core==1.6.0` - Azure management core
- `python-dotenv==1.0.1` - Environment variable management

## Security Considerations

1. **Credential Management**:
   - Never commit `.env` file to version control
   - Use Azure Key Vault or similar secret management in production
   - Rotate service principal secrets regularly

2. **RBAC**:
   - Ensure service principal has minimum required permissions
   - Use dedicated service principals per environment

3. **Network Security**:
   - LoadBalancer services expose applications publicly
   - Consider using ingress controllers with authentication

4. **Container Images**:
   - Use trusted container registries
   - Scan images for vulnerabilities
   - Use specific image tags instead of `latest`

## Troubleshooting

### Authentication Issues
- Verify Azure service principal credentials in `.env`
- Check that service principal has appropriate permissions on the AKS cluster
- Ensure subscription ID and tenant ID are correct

### Deployment Failures
- Check AKS cluster status and resource quotas
- Verify container image is accessible from the cluster
- Review Kubernetes events: `kubectl get events -n <namespace>`

### LoadBalancer IP Not Assigned
- Check cloud provider quota for LoadBalancer services
- Verify AKS cluster has proper networking configuration
- Review Azure resource group for LoadBalancer resources

### Job Not Starting
- Check container image exists and is pullable
- Review pod logs: `kubectl logs -n <namespace> <pod-name>`
- Verify resource requests don't exceed node capacity
