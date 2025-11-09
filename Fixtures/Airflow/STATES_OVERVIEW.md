# Airflow Test States - Complete Overview

This document provides a high-level overview of the two types of test states used in DE-Bench: **GitHub State Archives** and **Airflow Container States**.

## Quick Comparison

| Feature | GitHub State Archives | Airflow Container States |
|---------|---------------------|------------------------|
| **Format** | `.zip` files | Docker images |
| **Storage Location** | `Fixtures/Airflow/GitHub_States/` | Azure Container Registry (ACR) |
| **Size** | Small (< 1 MB) | Large (500 MB - 2 GB) |
| **Contains** | Source files + git history | Full Airflow environment + runtime state |
| **Deployment Target** | GitHub branches | Azure Kubernetes Service (AKS) |
| **Manager** | `GitHubManager` | `ManifestManager` |
| **Primary Use** | Initialize GitHub repo state | Deploy Airflow to Kubernetes |
| **State Captured** | Files, DAGs, configs, git history | Database, connections, variables, metadata |
| **Setup Time** | Fast (seconds) | Slower (minutes) |
| **Runtime State** | ❌ Not preserved | ✅ Fully preserved |
| **Best For** | PR-based testing workflows | Live Airflow testing on K8s |

## When to Use Each

### Use GitHub State Archives When...

✅ Testing GitHub PR workflows
✅ AI agent modifies files and creates PRs
✅ Need git history for context
✅ Testing file-based changes (DAGs, configs)
✅ Want fast, lightweight test setup
✅ Focus is on code changes, not runtime behavior

**Example Tests:**
- `Tests/Airflow_Agent_Failed_Hello_World/` - Fix broken DAG via PR
- `Tests/Airflow_Agent_Hello_Universe_Pipeline/` - Create new DAG via PR

**Documentation:** [GitHub_States/README.md](GitHub_States/README.md)

### Use Airflow Container States When...

✅ Testing with Kubernetes/AKS deployment
✅ Need preserved Airflow database state
✅ Testing with connections, variables, pools
✅ Simulating production environments
✅ Complex Airflow configurations required
✅ Live Airflow API interaction needed

**Example Tests:**
- Same tests use BOTH - GitHub states for repo, Container states for Airflow deployment

**Documentation:** [AIRFLOW_CONTAINER_STATES.md](AIRFLOW_CONTAINER_STATES.md)

## How They Work Together

Most tests use **both** types of states:

### Combined Workflow

```
1. GitHub State Archive (files)
   ↓
   Initializes GitHub repository branch
   with DAGs, Dockerfile, configs

2. Container State (runtime)
   ↓
   Deploys Airflow to AKS
   with existing state/data

3. AI Agent Interaction
   ↓
   Makes changes to GitHub repo
   Creates PR with fixes/features

4. GitHub Actions CI/CD
   ↓
   Builds new container from updated code
   Redeploys to Airflow

5. Validation
   ↓
   Verifies DAGs work correctly
   Checks expected behavior
```

### Example: Failed Hello World Test

**GitHub State (`hello-world-failure-state.zip`):**
- Contains broken `hello_world.py` DAG
- Has Dockerfile for building container
- Includes git history

**Container State (`FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE`):**
- Pre-built Airflow image with the broken DAG
- Database initialized
- Connections configured
- Deployed to AKS for testing

**Test Flow:**
1. GitHub state initializes repository branch
2. Container deployed to AKS from ACR image
3. AI agent sees broken DAG in Airflow UI
4. AI creates PR to fix the DAG
5. PR merged triggers GitHub Actions
6. New container built and deployed
7. Test verifies DAG now works

## Directory Structure

```
DE-Bench/
├── Fixtures/
│   ├── Airflow/
│   │   ├── GitHub_States/              # GitHub state archives (.zip)
│   │   │   ├── README.md               # GitHub states documentation
│   │   │   ├── INDEX.md                # Navigation for GitHub states
│   │   │   ├── EXAMPLE_hello_world_failure.md
│   │   │   ├── empty-state.zip         # Empty repo state
│   │   │   └── hello-world-failure-state.zip  # Broken DAG state
│   │   │
│   │   ├── AIRFLOW_CONTAINER_STATES.md # Container states documentation (this doc)
│   │   ├── STATES_OVERVIEW.md          # This overview
│   │   └── airflow_fixture.py          # Airflow test fixture
│   │
│   └── GitHub/
│       ├── github_manager.py           # Manages GitHub states
│       └── github_fixture.py           # GitHub test fixture
│
├── Environment/
│   └── Kubernetes/
│       └── ManifestManager.py          # Manages K8s/Container deployment
│
└── Tests/
    ├── Airflow_Agent_Failed_Hello_World/    # Uses BOTH state types
    └── Airflow_Agent_Hello_Universe_Pipeline/  # Uses BOTH state types
```

## Creating States - Quick Guide

### GitHub State Archive

```bash
# 1. Prepare repository with desired files
cd my-repo
git add .
git commit -m "Test state"

# 2. Create zip including .git directory
zip -r my-state.zip . -x "*.pyc" -x "__pycache__/*"

# 3. Move to GitHub_States directory
mv my-state.zip DE-Bench/Fixtures/Airflow/GitHub_States/

# 4. Use in test
custom_github_config = {
    "state_archive_path": f"{root_dir}/Fixtures/Airflow/GitHub_States/my-state.zip"
}
```

**Full Guide:** [GitHub_States/README.md](GitHub_States/README.md)

### Container State

```bash
# 1. Build Airflow image
docker build -t my-airflow:v1 /path/to/Airflow-Test

# 2. Run and capture state (optional)
docker run -d --name capture -p 8080:8080 my-airflow:v1
# Make changes in Airflow UI/CLI
docker commit capture my-airflow:v2

# 3. Tag for ACR
docker tag my-airflow:v2 ${AZURE_ACR_NAME}.azurecr.io/my-airflow:v2

# 4. Push to ACR
az acr login --name ${AZURE_ACR_NAME}
docker push ${AZURE_ACR_NAME}.azurecr.io/my-airflow:v2

# 5. Use in test
custom_airflow_config = {
    "container_image": os.getenv("MY_AIRFLOW_IMAGE")
}
```

**Full Guide:** [AIRFLOW_CONTAINER_STATES.md](AIRFLOW_CONTAINER_STATES.md)

## Available States

### GitHub State Archives

Located in `Fixtures/Airflow/GitHub_States/`:

| Archive | Size | Purpose | Example Test |
|---------|------|---------|--------------|
| `empty-state.zip` | 908 KB | Blank repo for creation tests | Hello Universe Pipeline |
| `hello-world-failure-state.zip` | 698 KB | Broken DAG for fixing tests | Failed Hello World |

### Container Images (ACR)

Stored in Azure Container Registry:

| Image | Purpose | Environment Variable |
|-------|---------|---------------------|
| Base Airflow | Standard empty Airflow | `AIRFLOW_CONTAINER_IMAGE` |
| Failure State | Broken hello world DAG | `FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE` |

List available images:
```bash
az acr repository list --name ${AZURE_ACR_NAME}
```

## Common Workflows

### Workflow 1: AI Creates New DAG from Scratch

**States Used:**
- GitHub: `empty-state.zip` (blank repository)
- Container: Base Airflow image (no pre-existing DAGs)

**Process:**
1. Empty GitHub branch created
2. Empty Airflow deployed to AKS
3. AI creates new DAG files
4. AI commits to GitHub, creates PR
5. PR merged, GitHub Actions builds new container
6. New container deployed with AI-created DAGs
7. Test validates DAG works

**Example:** `Tests/Airflow_Agent_Hello_Universe_Pipeline/`

### Workflow 2: AI Fixes Broken DAG

**States Used:**
- GitHub: `hello-world-failure-state.zip` (contains broken DAG)
- Container: Failure state image (broken DAG already deployed)

**Process:**
1. GitHub branch initialized with broken code
2. Airflow deployed with broken DAG running
3. AI identifies the failure in Airflow UI
4. AI fixes the DAG code
5. AI creates PR with fix
6. PR merged, GitHub Actions rebuilds container
7. Fixed container deployed
8. Test validates DAG now succeeds

**Example:** `Tests/Airflow_Agent_Failed_Hello_World/`

### Workflow 3: AI Modifies Existing Configuration

**States Used:**
- GitHub: Custom state with configs
- Container: Pre-configured Airflow image

**Process:**
1. GitHub branch with existing configurations
2. Airflow deployed with current settings
3. AI modifies configurations (connections, variables)
4. Changes pushed via PR
5. Redeployment with new configs
6. Test validates changes work

## Environment Variables Reference

```bash
# Azure Authentication
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
export AZURE_TENANT_ID="..."
export AZURE_SUBSCRIPTION_ID="..."

# Azure Resources
export AZURE_ACR_NAME="your-acr-name"
export DE_BENCH_AKS_RESOURCE_GROUP="your-resource-group"
export DE_BENCH_AKS_CLUSTER_NAME="your-cluster-name"

# Container Images
export AIRFLOW_CONTAINER_IMAGE="your-acr.azurecr.io/airflow-base:latest"
export FAILURE_HELLO_WORLD_AIRFLOW_CONTAINER_IMAGE="your-acr.azurecr.io/failure-hello-world:latest"

# GitHub
export GITHUB_ACCESS_TOKEN="..."
export GITHUB_REPO_URL="..."
```

## Key Components

### GitHubManager
**Location:** `Fixtures/GitHub/github_manager.py`

**Responsibilities:**
- Unpacks GitHub state archives
- Creates test branches
- Pushes files to GitHub
- Manages PRs and merges
- Captures code snapshots

**Key Method:**
```python
def unpack_and_push_state_file(self, state_archive_path: Union[str, Path]) -> None:
    # Extracts .zip and pushes files to GitHub branch
```

### ManifestManager
**Location:** `Environment/Kubernetes/ManifestManager.py`

**Responsibilities:**
- Generates Kubernetes manifests
- Deploys containers to AKS
- Manages namespaces and services
- Retrieves external IPs
- Cleans up K8s resources
- Manages ACR repositories

**Key Methods:**
```python
def generate_and_apply_manifest(self, namespace: str, container: str) -> None:
    # Deploys container to AKS

def get_service_external_ip(self, namespace: str) -> Optional[str]:
    # Gets LoadBalancer external IP for accessing Airflow

def delete_repo_from_acr(self, acr_name: str, repo_name: str) -> bool:
    # Cleans up ACR repositories
```

### AirflowFixture
**Location:** `Fixtures/Airflow/airflow_fixture.py`

**Responsibilities:**
- Sets up Airflow test environment
- Coordinates GitHubManager and ManifestManager
- Waits for Airflow to be ready
- Verifies DAGs
- Cleans up resources

## Testing Best Practices

### State Archive Best Practices
1. ✅ Keep GitHub archives small (< 1 MB)
2. ✅ Include minimal git history
3. ✅ Document the purpose of each state
4. ✅ Version state archives
5. ✅ Test archives before committing

### Container State Best Practices
1. ✅ Use semantic versioning for images
2. ✅ Tag images with descriptive names
3. ✅ Optimize image size (multi-stage builds)
4. ✅ Document what state is captured
5. ✅ Test locally before pushing to ACR
6. ✅ Clean up old images from ACR

### Test Configuration Best Practices
1. ✅ Use environment variables for image names
2. ✅ Document required environment variables
3. ✅ Provide example `.env` files
4. ✅ Include validation steps in tests
5. ✅ Clean up resources in teardown

## Troubleshooting

### GitHub State Issues
**Problem:** Files not appearing on GitHub branch

**Solution:** Check [GitHub_States/README.md - Troubleshooting](GitHub_States/README.md#troubleshooting)

### Container State Issues
**Problem:** Container won't start or crashes

**Solution:** Check [AIRFLOW_CONTAINER_STATES.md - Troubleshooting](AIRFLOW_CONTAINER_STATES.md#troubleshooting)

### Integration Issues
**Problem:** GitHub Actions fails to build container

**Check:**
- Dockerfile exists in GitHub state archive
- ACR credentials configured in GitHub secrets
- GitHub Actions workflow file is present
- build-info.properties updated correctly

## Additional Resources

### Documentation
- **GitHub States:** [GitHub_States/README.md](GitHub_States/README.md)
- **Container States:** [AIRFLOW_CONTAINER_STATES.md](AIRFLOW_CONTAINER_STATES.md)
- **GitHub Manager:** `Fixtures/GitHub/github_manager.py` (lines 1-186)
- **Manifest Manager:** `Environment/Kubernetes/ManifestManager.py`

### Example Code
- **Test with Both States:** `Tests/Airflow_Agent_Failed_Hello_World/test_airflow_agent_failed_hello_world_pipeline.py`

### Tools and Commands
```bash
# GitHub States
unzip -l state.zip                    # Inspect archive
zip -r state.zip . -x "*.pyc"        # Create archive

# Container States
docker build -t image:tag .           # Build image
docker commit container image:tag     # Capture state
docker push registry/image:tag        # Push to ACR
az acr repository list --name acr     # List ACR images

# Kubernetes
kubectl get pods -n namespace         # Check pods
kubectl logs -n namespace pod-name    # View logs
kubectl describe pod -n namespace     # Debug pod
```

## FAQs

**Q: Can I use both types of states in the same test?**
A: Yes! Most tests use both - GitHub states for repository initialization and container states for Airflow deployment.

**Q: Which state type should I use for my test?**
A: It depends on your test focus:
- Testing code changes/PRs → GitHub states
- Testing live Airflow behavior → Container states
- Full E2E workflow → Both

**Q: How do I update an existing state?**
A:
- GitHub: Create new zip with updated files
- Container: Build new image or commit changes to running container, then push with new tag

**Q: What's the difference between committing a container and building a new image?**
A:
- `docker build`: Creates image from Dockerfile (code + static files)
- `docker commit`: Captures running container state (runtime data + changes)
- Use `commit` to preserve Airflow database state, connections, variables

**Q: How do I clean up old states?**
A:
- GitHub: Delete old .zip files from repository
- Container: Use ManifestManager's `delete_repo_from_acr()` or Azure CLI

**Q: Can I use Docker Hub images instead of ACR?**
A: Yes, but ACR is recommended for:
- Private images
- Better integration with Azure
- No rate limiting
- Faster pulls from AKS

---

**Last Updated:** 2025-11-07

**Related Documentation:**
- [GitHub States README](GitHub_States/README.md)
- [Container States Guide](AIRFLOW_CONTAINER_STATES.md)
- [GitHub States Index](GitHub_States/INDEX.md)
