# Airflow Container States - Using ACR Images

## Overview

Airflow container states are Docker images stored in Azure Container Registry (ACR) that contain pre-configured Airflow instances with specific DAGs, configurations, or data. Unlike GitHub state archives (`.zip` files), container states package the entire Airflow environment including:

- Airflow installation and configuration
- Pre-existing DAGs and data
- Python dependencies
- System packages
- Database state (metadata, connections, variables)

These container images are deployed to Azure Kubernetes Service (AKS) using the `ManifestManager` and serve as the starting point for AI agent testing.

## Two Approaches to Container States

### Approach 1: Create Custom Container State
Build a new container with specific Airflow state (DAGs, data, failures)

**Use When:**
- Testing AI agents fixing broken DAGs
- Pre-loading specific data or configurations
- Creating reproducible failure scenarios
- Capturing complex Airflow states

### Approach 2: Use Existing Container Image
Reference an existing image from ACR or Docker Hub

**Use When:**
- Using a base/empty Airflow installation
- Starting from a known good state
- Testing AI agent creation from scratch
- Using shared/standard configurations

## Prerequisites

### Required Tools
- Docker installed and running
- Azure CLI (`az`) installed and authenticated
- Access to Azure Container Registry (ACR)
- Environment variables configured (see below)

### Required Environment Variables

```bash
# Azure Authentication
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_SUBSCRIPTION_ID="your-subscription-id"

# Azure Resources
export AZURE_ACR_NAME="your-acr-name"
export DE_BENCH_AKS_RESOURCE_GROUP="your-resource-group"
export DE_BENCH_AKS_CLUSTER_NAME="your-cluster-name"

# Container Images (used in tests)
export AIRFLOW_CONTAINER_IMAGE="your-acr.azurecr.io/repository:tag"
export FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE="your-acr.azurecr.io/failure-state:tag"
```

## Approach 1: Creating Custom Container States

This approach creates a container with specific Airflow state by running Airflow, making changes, and capturing the state.

### Step 1: Prepare the Airflow Directory

Use a template or create your own:

```bash
mkdir my-airflow-state
cd my-airflow-state
```

Required files:
- `Dockerfile` - Container build configuration
- `airflow_init.sh` - Airflow initialization script
- `dags/` - Directory for DAG files
- `Requirements/requirements.txt` - Python dependencies
- `packages.txt` - System packages (optional)

### Step 2: Create the Dockerfile

Example `Dockerfile`:

```dockerfile
# allow --build-arg PYTHON_VERSION=x.x to specify python versions
# allow --build-arg AIRFLOW_VERSION=x.x to specify Airflow version
ARG PYTHON_VERSION=3.11
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Build arg to enable session cookie auth (default: basic auth only)
ARG USE_SESSION_AUTH

# Enable API authentication for programmatic access
ENV AIRFLOW__API__AUTH_BACKENDS=${USE_SESSION_AUTH:+airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth}${USE_SESSION_AUTH:-airflow.api.auth.backend.basic_auth}

# Copy DAGs, entrypoint and requirements
COPY Requirements/requirements.txt .
COPY dags/ /opt/airflow/dags
COPY airflow_init.sh /airflow_init.sh

# Install Python requirements
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Install system packages if provided
COPY packages.txt .
USER root
RUN if [ -s packages.txt ]; then \
    apt-get update && \
    xargs -a packages.txt apt-get install -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*; \
    fi && \
    chmod +x /airflow_init.sh

# Switch to the non-root airflow user and start Airflow
USER airflow
ENTRYPOINT [ "/airflow_init.sh" ]
```

### Step 3: Create the Initialization Script

Example `airflow_init.sh`:

```bash
#!/bin/bash
set -e

echo "Initializing Airflow database..."
airflow db migrate

# Initialize Flask AppBuilder roles
echo "Creating Flask AppBuilder roles..."
python3 << 'PYEOF'
import sys
from airflow.www.app import create_app

try:
    app = create_app()
    with app.app_context():
        appbuilder = app.appbuilder
        appbuilder.sm.sync_roles()
        print("Roles created successfully!")
except Exception as e:
    print(f"Error creating roles: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF

# Set admin password from environment variable or use default
ADMIN_PASSWORD="${AIRFLOW_PASSWORD:-YourDefault@irflowPassw0rd!}"

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password "$ADMIN_PASSWORD" 2>&1 | grep -v "already exist" || true

echo "Starting airflow in standalone mode..."
airflow standalone
```

**Important:** Make the script executable:
```bash
chmod +x airflow_init.sh
```

### Step 4: Add Your DAGs and Content

For **failure state** testing (broken DAG):
```bash
mkdir -p dags
cat > dags/broken_dag.py << 'EOF'
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "broken_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# This task will fail - AI must fix it
failing_task = BashOperator(
    task_id="failing_task",
    bash_command="cat /nonexistent/file.txt",  # Intentional error
    dag=dag
)
EOF
```

For **empty state** testing:
```bash
mkdir -p dags
touch dags/.gitkeep  # Empty dags directory
```

### Step 5: Create Requirements Files

```bash
# Create empty or minimal requirements
mkdir -p Requirements
echo "" > Requirements/requirements.txt
echo "" > packages.txt
```

### Step 6: Build the Base Image

```bash
docker build -t my-airflow-base:latest .
```

Build with custom arguments (optional):
```bash
docker build \
  --build-arg PYTHON_VERSION=3.11 \
  --build-arg AIRFLOW_VERSION=2.10.5 \
  --build-arg USE_SESSION_AUTH=true \
  -t my-airflow-base:latest .
```

### Step 7: Run Container and Capture State

**Option A: Capture State Immediately (No Changes Needed)**

If your DAGs and configuration are already in the image, you can skip to Step 8.

**Option B: Run Airflow and Make Changes**

This is useful for capturing runtime state, adding connections, or testing DAG execution:

```bash
# Run the container
docker run -d --name airflow-state-capture \
  -p 8080:8080 \
  -e AIRFLOW_PASSWORD=admin \
  my-airflow-base:latest

# Wait for Airflow to start (check logs)
docker logs -f airflow-state-capture

# Access Airflow UI at http://localhost:8080
# Username: admin, Password: admin

# Make your changes:
# - Add connections via UI or CLI
# - Trigger DAGs to populate metadata
# - Create variables
# - Add pools/XComs/etc.

# Example: Add connection via CLI
docker exec airflow-state-capture \
  airflow connections add 'my_conn' \
  --conn-type 'http' \
  --conn-host 'example.com'

# Example: Set variable via CLI
docker exec airflow-state-capture \
  airflow variables set my_var "my_value"

# Example: Trigger a DAG
docker exec airflow-state-capture \
  airflow dags trigger broken_pipeline
```

### Step 8: Commit the Container State

**Important:** This captures ALL changes made to the running container, including:
- Airflow database state (metadata, connections, variables)
- DAG execution history
- Any files created or modified
- Configuration changes

```bash
# Commit the running container to a new image
docker commit airflow-state-capture my-airflow-state:v1

# Verify the new image
docker images | grep my-airflow-state

# Stop and remove the running container
docker stop airflow-state-capture
docker rm airflow-state-capture
```

### Step 9: Tag for ACR

```bash
# Tag for your ACR
docker tag my-airflow-state:v1 ${AZURE_ACR_NAME}.azurecr.io/airflow-states/my-state:v1

# Or with specific naming
docker tag my-airflow-state:v1 ${AZURE_ACR_NAME}.azurecr.io/failure-hello-world:latest
```

### Step 10: Push to ACR

```bash
# Login to ACR
az acr login --name ${AZURE_ACR_NAME}

# Push the image
docker push ${AZURE_ACR_NAME}.azurecr.io/airflow-states/my-state:v1

# Verify the push
az acr repository show \
  --name ${AZURE_ACR_NAME} \
  --repository airflow-states/my-state
```

### Step 11: Reference in Tests

Update your test configuration to use the new container:

```python
# In Tests/My_Test/test_my_pipeline.py

import os

def get_fixtures():
    from Fixtures.Airflow.airflow_fixture import AirflowFixture

    custom_airflow_config = {
        "resource_id": f"my_test_{timestamp}_{uuid}",
        "airflow_provider": "ecs",
        "container_image": os.getenv("MY_AIRFLOW_STATE_IMAGE"),
        "ecs_namespace": f"my-test-{timestamp}-{uuid}",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    return [airflow_fixture]
```

Set environment variable:
```bash
export MY_AIRFLOW_STATE_IMAGE="${AZURE_ACR_NAME}.azurecr.io/airflow-states/my-state:v1"
```

## Approach 2: Using Existing Container Images

This is simpler - just reference an existing image without building a new one.

### Option A: Use Existing ACR Image

```python
# In your test configuration
custom_airflow_config = {
    "resource_id": f"test_{timestamp}_{uuid}",
    "airflow_provider": "ecs",
    "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow-base:latest",  # Existing image
    "ecs_namespace": f"test-{timestamp}-{uuid}",
}
```

### Option B: Use Docker Hub Image

```python
# Use official Airflow image
custom_airflow_config = {
    "resource_id": f"test_{timestamp}_{uuid}",
    "airflow_provider": "ecs",
    "container_image": "apache/airflow:2.10.5-python3.11",  # Docker Hub
    "ecs_namespace": f"test-{timestamp}-{uuid}",
}
```

**Note:** Docker Hub images may require authentication or rate limiting considerations.

### List Available ACR Images

```bash
# List all repositories in ACR
az acr repository list --name ${AZURE_ACR_NAME} --output table

# List tags for a specific repository
az acr repository show-tags \
  --name ${AZURE_ACR_NAME} \
  --repository airflow-states/my-state \
  --output table

# Get image details
az acr repository show \
  --name ${AZURE_ACR_NAME} \
  --repository airflow-states/my-state \
  --output json
```

## How Container States Are Used in Tests

### Test Workflow

1. **Fixture Setup** - AirflowFixture configures the container image
   ```python
   custom_airflow_config = {
       "container_image": os.getenv("AIRFLOW_CONTAINER_IMAGE"),
       "airflow_provider": "ecs",
   }
   ```

2. **Kubernetes Deployment** - ManifestManager deploys to AKS
   - Creates namespace
   - Creates Job with container image
   - Creates LoadBalancer service
   - Waits for external IP

3. **AI Agent Testing** - Agent interacts with Airflow
   - Access Airflow UI via external IP
   - View existing DAGs (from container state)
   - Make changes via GitHub PR
   - GitHub Actions rebuild and redeploy

4. **Validation** - Test verifies expected behavior
   - DAG exists
   - DAG runs successfully
   - Output is correct

5. **Cleanup** - Resources are deleted
   - Namespace deleted
   - ACR repository cleaned up (optional)

### Example Test Configuration

From `Tests/Airflow_Agent_Failed_Hello_World/`:

```python
def get_fixtures():
    from Fixtures.Airflow.airflow_fixture import AirflowFixture

    resource_id = f"hello_world_failure_test_{timestamp}_{uuid}"

    custom_airflow_config = {
        "resource_id": resource_id,
        "airflow_provider": "ecs",
        "container_image": os.getenv("FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE"),
        "ecs_namespace": resource_id.replace("_", "-"),
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    return [airflow_fixture]
```

Environment variable:
```bash
export FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE="myacr.azurecr.io/failure-hello-world:latest"
```

## Container State vs GitHub State Archives

| Feature | Container State | GitHub State Archive |
|---------|----------------|---------------------|
| **Format** | Docker image | .zip file |
| **Contains** | Full Airflow environment + runtime state | Source files + git history |
| **Storage** | Azure Container Registry (ACR) | Git repository |
| **Size** | Larger (500MB - 2GB) | Smaller (< 1MB) |
| **State Captured** | Database, connections, metadata | Files, DAGs, configs |
| **Use Case** | Kubernetes/AKS deployments | GitHub branch initialization |
| **Deployment** | ManifestManager → AKS | GitHubManager → GitHub |
| **Runtime State** | Preserved (DB, variables) | Not preserved |
| **Best For** | Complex Airflow states, live testing | File-based testing, PR workflows |

### When to Use Each

**Use Container States when:**
- Testing with Kubernetes/AKS
- Need preserved database state
- Require complex Airflow configurations
- Testing with connections/variables/pools
- Simulating production environments

**Use GitHub State Archives when:**
- Testing GitHub PR workflows
- Need git history
- Simple file-based changes
- Lightweight testing
- Focus on code changes, not runtime state

## Best Practices

### Container Image Management

1. **Naming Convention**
   ```
   ${AZURE_ACR_NAME}.azurecr.io/<purpose>-<state>:<version>

   Examples:
   - myacr.azurecr.io/airflow-base:latest
   - myacr.azurecr.io/failure-hello-world:v1
   - myacr.azurecr.io/airflow-states/empty:latest
   ```

2. **Tagging Strategy**
   - Use semantic versioning: `v1.0.0`, `v1.1.0`
   - Use descriptive tags: `failure-state`, `empty-state`
   - Always tag `latest` for current stable version
   - Use git commit SHA for traceability: `abc1234`

3. **Image Size Optimization**
   - Use multi-stage builds to reduce size
   - Clean up apt cache: `rm -rf /var/lib/apt/lists/*`
   - Remove build dependencies after installation
   - Use `.dockerignore` to exclude unnecessary files

4. **Documentation**
   - Document what state is captured in each image
   - Include README in repository
   - Tag images with metadata labels
   - Keep changelog of image versions

### Security

1. **Credentials**
   - Never hardcode credentials in Dockerfile
   - Use environment variables or secrets
   - Use ACR authentication (avoid public images)

2. **Base Images**
   - Use official Apache Airflow images
   - Keep base images updated
   - Scan images for vulnerabilities

3. **Access Control**
   - Use Azure RBAC for ACR access
   - Limit who can push images
   - Use service principals for automation

### Testing

1. **Local Testing**
   ```bash
   # Test image locally before pushing
   docker run -p 8080:8080 my-airflow-state:v1

   # Access UI and verify state
   open http://localhost:8080
   ```

2. **Validation**
   ```bash
   # Check image size
   docker images my-airflow-state:v1

   # Inspect image layers
   docker history my-airflow-state:v1

   # Verify Airflow version
   docker run --rm my-airflow-state:v1 airflow version
   ```

## Troubleshooting

### Container Won't Start

**Problem:** Container exits immediately or fails to start

**Solutions:**
```bash
# Check logs
docker logs <container-id>

# Run interactively to debug
docker run -it --entrypoint /bin/bash my-airflow-state:v1

# Check if init script has execute permissions
docker run --rm my-airflow-state:v1 ls -la /airflow_init.sh
```

### Cannot Push to ACR

**Problem:** Authentication or permission errors

**Solutions:**
```bash
# Re-authenticate
az acr login --name ${AZURE_ACR_NAME}

# Check ACR exists
az acr show --name ${AZURE_ACR_NAME}

# Verify credentials
az account show
```

### Image Too Large

**Problem:** Image is several GB in size

**Solutions:**
```dockerfile
# Use multi-stage build
FROM apache/airflow:2.10.5 as base
# ... build steps ...

FROM apache/airflow:2.10.5
COPY --from=base /opt/airflow/dags /opt/airflow/dags
# Only copy what's needed
```

```bash
# Remove build dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf ~/.cache/pip

# Clean apt cache
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
```

### State Not Preserved

**Problem:** Changes made in container are lost

**Solution:** Make sure you `docker commit` BEFORE stopping the container:
```bash
# Commit while running
docker commit <container-id> my-state:v1

# Then stop
docker stop <container-id>
```

### Kubernetes Pod Crashes

**Problem:** Pod crashes in AKS with CrashLoopBackOff

**Solutions:**
```bash
# Check pod logs
kubectl logs -n <namespace> <pod-name>

# Describe pod for events
kubectl describe pod -n <namespace> <pod-name>

# Check resource limits
kubectl get pod -n <namespace> <pod-name> -o yaml
```

## Complete Example: Creating a Failure State

This example creates a container with a broken DAG for testing AI agent remediation.

```bash
# 1. Create directory structure
mkdir -p my-failure-state/{dags,Requirements}
cd my-failure-state

# 2. Copy Dockerfile and init script from Approach 1 step 3
cp path/to/your/Dockerfile .
cp path/to/your/airflow_init.sh .
chmod +x airflow_init.sh

# 3. Create broken DAG
cat > dags/broken_dag.py << 'EOF'
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    "broken_pipeline",
    default_args={"owner": "airflow", "start_date": datetime(2024, 1, 1)},
    schedule_interval="@daily",
    catchup=False,
)

# Intentional failure - AI must fix this
failing_task = BashOperator(
    task_id="fail",
    bash_command="exit 1",  # Always fails
    dag=dag
)
EOF

# 4. Create empty requirements
echo "" > Requirements/requirements.txt
echo "" > packages.txt

# 5. Build image
docker build -t failure-state:v1 .

# 6. Test locally (optional)
docker run -d --name test-failure -p 8080:8080 failure-state:v1
# Wait for startup, then check http://localhost:8080
docker stop test-failure && docker rm test-failure

# 7. Commit and tag for ACR
docker tag failure-state:v1 ${AZURE_ACR_NAME}.azurecr.io/failure-hello-world:latest

# 8. Push to ACR
az acr login --name ${AZURE_ACR_NAME}
docker push ${AZURE_ACR_NAME}.azurecr.io/failure-hello-world:latest

# 9. Set environment variable
export FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE="${AZURE_ACR_NAME}.azurecr.io/failure-hello-world:latest"

# 10. Use in test (update test_*.py file)
# See "Example Test Configuration" section above
```

## Related Documentation

- **ManifestManager Code:** `Environment/Kubernetes/ManifestManager.py`
- **Airflow Fixture:** `Fixtures/Airflow/airflow_fixture.py`
- **Example Tests:**
  - `Tests/Airflow_Agent_Failed_Hello_World/test_airflow_agent_failed_hello_world_pipeline.py`
  - `Tests/Airflow_Agent_Hello_Universe_Pipeline/test_airflow_agent_hello_universe_pipeline.py`
- **GitHub State Archives:** `Fixtures/Airflow/GitHub_States/README.md`

## Quick Reference Commands

```bash
# Build image
docker build -t my-state:v1 .

# Run and capture state
docker run -d --name capture -p 8080:8080 my-state:v1
docker commit capture my-state:v2
docker stop capture && docker rm capture

# Push to ACR
az acr login --name ${AZURE_ACR_NAME}
docker tag my-state:v2 ${AZURE_ACR_NAME}.azurecr.io/my-state:v2
docker push ${AZURE_ACR_NAME}.azurecr.io/my-state:v2

# List ACR images
az acr repository list --name ${AZURE_ACR_NAME}

# Clean up local images
docker rmi my-state:v1 my-state:v2

# Clean up ACR (via ManifestManager)
# See delete_repo_from_acr() method
```
