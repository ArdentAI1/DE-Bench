# AKS Airflow Manifest Manager Documentation

## Overview

The `aks_airflow_manager.py` script is a command-line tool for managing Kubernetes manifests for Airflow deployments. It provides functionality to generate, apply, and manage Kubernetes resources with built-in profiling capabilities for performance testing.

## Features

- **Generate** Kubernetes manifests for Airflow deployments
- **Apply** existing manifest files to Kubernetes clusters
- **Generate and Apply** manifests in a single operation
- **Reapply Jobs** with new container images (handles immutable Job specs)
- **Profile** deployment operations for performance analysis

## Prerequisites

- Python 3.7+
- Kubernetes cluster access with proper kubectl configuration
- Required Python packages:
  - `python-dotenv`
  - `kubernetes` (Python client)
- Environment variables configured (loaded via `.env` file)

## Installation & Setup

1. Ensure you have access to a Kubernetes cluster and kubectl is configured
2. Install required dependencies:
   ```bash
   pip install python-dotenv kubernetes
   ```
3. Create a `.env` file in the project root with necessary configuration
4. Verify the `Environment.Kubernetes.ManifestManager` module is accessible

## Usage

The script provides five main commands, each with specific use cases:

### Basic Syntax
```bash
python scripts/aks_airflow_manager.py <command> [arguments] [options]
```

## Commands Reference

### 1. Generate Command

Generates a Kubernetes manifest and saves it to a file without applying it to the cluster.

```bash
python scripts/aks_airflow_manager.py generate <namespace> --output <output_file> [--container <container_image>]
```

**Arguments:**
- `namespace`: The Kubernetes namespace to create
- `--output`, `-o`: Output file path for the manifest (required)
- `--container`: Container image to use (optional)
  - Default: `airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base`

**Example:**
```bash
python scripts/aks_airflow_manager.py generate my-airflow-ns --output manifest.yml --container my-registry/airflow:latest
```

### 2. Apply Command

Applies an existing manifest file to the Kubernetes cluster.

```bash
python scripts/aks_airflow_manager.py apply <manifest_file>
```

**Arguments:**
- `manifest_file`: Path to the manifest file to apply

**Example:**
```bash
python scripts/aks_airflow_manager.py apply ./manifests/airflow-deployment.yml
```

### 3. Generate and Apply Command

Generates a manifest and immediately applies it to the cluster in a single operation.

```bash
python scripts/aks_airflow_manager.py generate-and-apply <namespace> [--container <container_image>] [--output <output_file>]
```

**Arguments:**
- `namespace`: The Kubernetes namespace to create
- `--container`: Container image to use (optional)
  - Default: `airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base`
- `--output`, `-o`: Optional file path to save the generated manifest

**Example:**
```bash
python scripts/aks_airflow_manager.py generate-and-apply production-airflow --container my-registry/airflow:v2.5.0 --output prod-manifest.yml
```

### 4. Reapply Job Command

Handles updating Kubernetes Jobs by deleting and recreating them with new container images. This is necessary because Job specifications are immutable in Kubernetes.

```bash
python scripts/aks_airflow_manager.py reapply-job <namespace> <container_image>
```

**Arguments:**
- `namespace`: The namespace where the Job exists
- `container`: The new container image to use

**Example:**
```bash
python scripts/aks_airflow_manager.py reapply-job my-job-namespace my-registry/updated-job:latest
```

### 5. Profile Command

Profiles the manifest generation and application process by creating multiple instances and measuring performance metrics.

```bash
python scripts/aks_airflow_manager.py profile <number_of_instances> <container_image>
```

**Arguments:**
- `number_of_instances`: Number of instances to create for profiling
- `container`: Container image to use for odd-numbered namespaces

**Example:**
```bash
python scripts/aks_airflow_manager.py profile 5 my-registry/airflow:test
```

**Profile Behavior:**
- Creates namespaces named `profile-namespace-1`, `profile-namespace-2`, etc.
- Odd-numbered namespaces use the specified container
- Even-numbered namespaces use a default failure container: `airflowstates-cudfbgfvekd7f0at.azurecr.io/hello-world-fail:base`
- Measures and reports timing for each operation:
  - Manifest generation
  - File saving
  - Manifest application
  - External IP retrieval
  - Pod health verification
- Provides summary statistics at the end

## Configuration

### Environment Variables

The script loads environment variables from a `.env` file. Ensure your `.env` file contains:

```env
# Kubernetes configuration
KUBECONFIG=/path/to/your/kubeconfig

# Container registry credentials (if needed)
REGISTRY_USERNAME=your_username
REGISTRY_PASSWORD=your_password

# Other environment-specific configurations
```

### Default Container Images

The script uses these default container images:
- **Primary**: `airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base`
- **Failure Testing**: `airflowstates-cudfbgfvekd7f0at.azurecr.io/hello-world-fail:base`

## Examples

### Quick Deployment
```bash
# Generate and deploy an Airflow instance
python scripts/aks_airflow_manager.py generate-and-apply my-airflow

# Check the deployment
kubectl get pods -n my-airflow
```

### Development Workflow
```bash
# Generate manifest for review
python scripts/aks_airflow_manager.py generate dev-airflow -o dev-manifest.yml

# Review the manifest
cat dev-manifest.yml

# Apply when ready
python scripts/aks_airflow_manager.py apply dev-manifest.yml
```

### Performance Testing
```bash
# Test deployment performance with 10 instances
python scripts/aks_airflow_manager.py profile 10 my-registry/airflow:performance-test
```

### Job Updates
```bash
# Update a job with a new container version
python scripts/aks_airflow_manager.py reapply-job my-job-ns my-registry/job:v1.2.3
```

## Output and Logging

The script provides detailed console output including:
- Operation success/failure messages
- File paths for generated manifests
- Timing information (especially in profile mode)
- External IP addresses when available
- Health check results

### Profile Output Example
```
Initialized KubernetesManifestManager in 0.15 seconds
Generated manifest in 0.23 seconds
Manifest generated and saved to: profile-namespace-1-manifest.yml
Saved manifest in 0.25 seconds
Manifest applied successfully for namespace: profile-namespace-1 in 2.45 seconds
External IP for namespace profile-namespace-1: 203.0.113.10
Retrieved external IP in 3.12 seconds
Pod in namespace profile-namespace-1 is healthy
Verified pod health in 3.87 seconds
...
Total time taken: 45.67 seconds for 5 namespace(s)
Time per namespace: 9.13 seconds
```

## Troubleshooting

### Common Issues

1. **"ModuleNotFoundError: No module named 'Environment.Kubernetes.ManifestManager'"**
   - Ensure the script is run from the project root directory
   - Verify the `Environment` module structure is correct

2. **"kubernetes.config.config_exception.ConfigException"**
   - Check your kubectl configuration: `kubectl config current-context`
   - Verify cluster connectivity: `kubectl cluster-info`

3. **"Permission denied" errors**
   - Ensure your Kubernetes user has sufficient permissions
   - Check RBAC settings for namespace creation and resource management

4. **Container image pull errors**
   - Verify the container image exists and is accessible
   - Check registry credentials and permissions

### Debug Steps

1. Test kubectl connectivity:
   ```bash
   kubectl get nodes
   ```

2. Verify environment variables:
   ```bash
   python -c "from dotenv import load_dotenv; load_dotenv(); import os; print('KUBECONFIG:', os.getenv('KUBECONFIG'))"
   ```

3. Test with a simple generation first:
   ```bash
   python scripts/aks_airflow_manager.py generate test-ns -o test.yml
   ```

## File Structure

```
scripts/
├── aks_airflow_manager.py          # Main script
Environment/
├── Kubernetes/
    └── ManifestManager.py       # Kubernetes manifest management logic
.env                             # Environment configuration
docs/
└── aks_airflow_manager_usage.md    # This documentation
```

## Contributing

When modifying the script:
1. Maintain backward compatibility with existing commands
2. Add appropriate error handling and logging
3. Update this documentation for any new features
4. Test with various Kubernetes environments

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify your Kubernetes cluster configuration
3. Review the `ManifestManager.py` implementation for underlying functionality
4. Check logs from the Kubernetes cluster for deployment-specific issues
