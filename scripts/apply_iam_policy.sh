#!/bin/bash
# Apply IAM policy to cicd-user for ECS infrastructure management

set -e

POLICY_NAME="DE-Bench-ECS-Infrastructure-Management"
USER_NAME="cicd-user"
POLICY_FILE="docs/required-iam-policy.json"

echo "Applying IAM policy to user: $USER_NAME"
echo "Policy file: $POLICY_FILE"
echo ""

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account ID: $ACCOUNT_ID"
echo ""

# Check if policy already exists
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
echo "Checking if policy exists: $POLICY_ARN"

if aws iam get-policy --policy-arn "$POLICY_ARN" 2>/dev/null; then
    echo "Policy already exists. Creating new version..."

    # Delete old versions if we're at the limit (5 versions max)
    VERSION_COUNT=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'length(Versions)' --output text)
    if [ "$VERSION_COUNT" -ge 5 ]; then
        echo "Deleting oldest policy version..."
        OLDEST_VERSION=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'Versions[?IsDefaultVersion==`false`] | [0].VersionId' --output text)
        aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$OLDEST_VERSION"
    fi

    # Create new policy version
    aws iam create-policy-version \
        --policy-arn "$POLICY_ARN" \
        --policy-document "file://$POLICY_FILE" \
        --set-as-default

    echo "✅ Policy updated successfully"
else
    echo "Policy does not exist. Creating new policy..."

    # Create new policy
    aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document "file://$POLICY_FILE" \
        --description "Allows DE-Bench ECS Manager to create and manage infrastructure"

    echo "✅ Policy created successfully"
fi

echo ""
echo "Attaching policy to user: $USER_NAME"

# Attach policy to user
aws iam attach-user-policy \
    --user-name "$USER_NAME" \
    --policy-arn "$POLICY_ARN"

echo "✅ Policy attached to user successfully"
echo ""
echo "Verifying policy attachment..."

# List user policies
aws iam list-attached-user-policies --user-name "$USER_NAME" --query 'AttachedPolicies[?PolicyName==`'"$POLICY_NAME"'`]'

echo ""
echo "✅ Done! User $USER_NAME now has permissions to manage ECS infrastructure"
echo ""
echo "You can now run:"
echo "  uv run python Environment/ECS/ManifestManager.py deploy ecs-testing --container <image> --service --load-balancer"
