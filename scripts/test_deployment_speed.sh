#!/bin/bash
# Compare deployment speeds: Standalone Task vs Load Balancer

set -e

CONTAINER="867765745132.dkr.ecr.us-east-2.amazonaws.com/airflow2-session-auth:base"

echo "=========================================="
echo "DEPLOYMENT SPEED COMPARISON"
echo "=========================================="
echo ""

# Test 1: Standalone Task (No Load Balancer)
echo "Test 1: Standalone Task (Public IP - No Load Balancer)"
echo "Expected time: ~45 seconds"
echo "------------------------------------------"
START=$(date +%s)

uv run python Environment/ECS/ManifestManager.py deploy speed-test-standalone \
  --container "$CONTAINER"

END=$(date +%s)
DURATION=$((END - START))
echo ""
echo "✅ Standalone task deployed in: ${DURATION} seconds"
echo ""

# Clean up
echo "Cleaning up standalone task..."
uv run python Environment/ECS/ManifestManager.py cleanup speed-test-standalone
echo ""

# Test 2: Service with Load Balancer
echo "Test 2: Service with Load Balancer"
echo "Expected time: ~3-5 minutes"
echo "------------------------------------------"
START=$(date +%s)

uv run python Environment/ECS/ManifestManager.py deploy speed-test-lb \
  --container "$CONTAINER" \
  --service \
  --load-balancer

END=$(date +%s)
DURATION=$((END - START))
echo ""
echo "✅ Service with LB deployed in: ${DURATION} seconds"
echo ""

# Clean up
echo "Cleaning up service..."
uv run python Environment/ECS/ManifestManager.py cleanup speed-test-lb
echo ""

echo "=========================================="
echo "COMPARISON COMPLETE"
echo "=========================================="
echo ""
echo "Key Takeaways:"
echo "- Standalone tasks are ~4-6x faster to deploy"
echo "- Both are accessible from your local machine"
echo "- Load balancer provides stable DNS and failover"
echo "- Choose based on your use case (dev vs prod)"
