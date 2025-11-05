#!/bin/bash

# Script to create and hibernate test deployments in Astronomer
# Usage: ./create_deployments.sh <deployment_number1> [deployment_number2] [deployment_number3] ...
# Example: ./create_deployments.sh 1 2 3

set -e  # Exit on any error

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Error: No deployment numbers provided."
    echo "Usage: $0 <deployment_number1> [deployment_number2] [deployment_number3] ..."
    echo "Example: $0 1 2 3"
    exit 1
fi

echo "Starting deployment creation process..."
echo "Deployment numbers to create: $*"
echo ""

# Process each deployment number
for deployment_num in "$@"; do
    # Validate that the argument is a number
    if ! [[ "$deployment_num" =~ ^[0-9]+$ ]]; then
        echo "Error: '$deployment_num' is not a valid number. Skipping..."
        continue
    fi
    
    deployment_name="de_bench_test_runner_$deployment_num"
    echo "Processing deployment: $deployment_name"
    
    # Check if deployment already exists
    if astro deployment list | grep -q "$deployment_name"; then
        echo "  Deployment $deployment_name already exists. Skipping creation..."
    else
        # Create the deployment
        echo "  Creating deployment: $deployment_name"
        astro deployment create \
            --workspace-id cmcnpmwr80l9601lyycmaep42 \
            --name "$deployment_name" \
            --runtime-version 13.1.0 \
            --development-mode enable \
            --cloud-provider aws \
            --region us-east-1 \
            -d "This deployment is used for airflow tests and will be recreated between runs." \
            --scheduler-size small
    fi
    
    # Hibernate the deployment
    echo "  Hibernating deployment: $deployment_name"
    astro deployment hibernate --deployment-name "$deployment_name" -f
    
    echo "  Completed processing: $deployment_name"
    echo ""
done

echo "All specified deployments have been created and hibernated successfully!"
