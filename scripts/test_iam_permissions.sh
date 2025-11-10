#!/bin/bash
# Test if cicd-user has the necessary permissions

echo "Testing IAM permissions for cicd-user..."
echo ""

# Use the cicd-user credentials
export AWS_ACCESS_KEY_ID=$(grep ACCESS_KEY_ID_AWS .env | cut -d'=' -f2)
export AWS_SECRET_ACCESS_KEY=$(grep SECRET_ACCESS_KEY_AWS .env | cut -d'=' -f2)
export AWS_REGION=$(grep AWS_REGION .env | cut -d'=' -f2)

echo "Testing EC2 permissions..."
if aws ec2 describe-vpcs --max-items 1 2>/dev/null; then
    echo "✅ EC2 DescribeVpcs: OK"
else
    echo "❌ EC2 DescribeVpcs: FAILED"
fi

echo ""
echo "Testing IAM permissions..."
if aws iam list-roles --max-items 1 2>/dev/null; then
    echo "✅ IAM ListRoles: OK"
else
    echo "❌ IAM ListRoles: FAILED"
fi

echo ""
echo "Testing ECS permissions..."
if aws ecs list-clusters 2>/dev/null; then
    echo "✅ ECS ListClusters: OK"
else
    echo "❌ ECS ListClusters: FAILED"
fi

echo ""
echo "If all tests passed, you can now run the ECS deployment!"
