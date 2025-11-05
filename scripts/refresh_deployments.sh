#!/bin/bash

# Script to refresh all Astro deployments
# This script will delete, recreate, and hibernate all existing deployments in Astronomer

# Keep track for the number of deployments created, skipped and existing
num_deployments_created=0
num_deployments_skipped=0
num_deployments_existing=0

set -e  # Exit on any error

echo "Starting deployment refresh process..."

# Get all deployment names from astro deployment list
echo "Fetching deployment list..."
deployments=$(astro deployment list | tail -n +2 | awk '{print $1}' | grep -v '^$')

if [ -z "$deployments" ]; then
    echo "No deployments found."
    exit 0
fi

echo "Found deployments:"
echo "$deployments"
echo ""

# Process each deployment
for deployment_name in $deployments; do
    num_deployments_existing=$((num_deployments_existing + 1))
    echo "Processing deployment: $deployment_name"

    # Check if the deployment has a hibernating status already
    status=$(astro deployment inspect -n $deployment_name --key metadata.status || echo "UNKNOWN")
    echo "  Deployment $deployment_name status: $status"

    # If the deployment is already hibernating or unknown, skip it
    if [[ "$status" == "HIBERNATING" || "$status" == "UNKNOWN" || "$status" == "CREATING" ]]; then
        echo "  Deployment $deployment_name is already $status. Skipping..."
        num_deployments_skipped=$((num_deployments_skipped + 1))
        continue
    fi
    
    # Delete the deployment
    echo "  Deleting deployment: $deployment_name"
    astro deployment delete -n "$deployment_name" -f
    
    # Wait a moment for deletion to complete
    sleep 5
    
    # Recreate the deployment
    echo "  Recreating deployment: $deployment_name"
    astro deployment create \
        --workspace-id cmcnpmwr80l9601lyycmaep42 \
        --name "$deployment_name" \
        --runtime-version 13.1.0 \
        --development-mode enable \
        --cloud-provider aws \
        --region us-east-1 \
        -d "This deployment is used for airflow tests and will be recreated between runs." \
        --scheduler-size small
    
    # Increment the number of deployments created
    num_deployments_created=$((num_deployments_created + 1))
    
    # Hibernate the deployment
    echo "  Hibernating deployment: $deployment_name"
    astro deployment hibernate --deployment-name "$deployment_name" -f
    
    echo "  Completed processing: $deployment_name"
    echo ""
done

echo "All deployments have been refreshed successfully!"
echo "Analyzed deployments: $num_deployments_existing, $num_deployments_skipped deployments skipped, $num_deployments_created deployments refreshed"
