#!/bin/bash
set -e

# Get parameters
REPO_URL=$1
DAG_FILE=$2

# Clone the repository
echo "Cloning repository: $REPO_URL"
git clone $REPO_URL /tmp/repo

# Find the DAG file
DAG_PATH="/tmp/repo/dags/$DAG_FILE"
echo "Testing DAG file: $DAG_PATH"

# Check if file exists
if [ ! -f "$DAG_PATH" ]; then
  echo "Error: DAG file not found at $DAG_PATH"
  exit 1
fi

# Add dag.test() to the file if it doesn't exist
if ! grep -q "if __name__ == \"__main__\":" "$DAG_PATH"; then
  echo -e "\n\nif __name__ == \"__main__\":\n    dag.test()" >> "$DAG_PATH"
  echo "Added dag.test() to the file"
fi

# Run the DAG test
echo "Running DAG test..."
python "$DAG_PATH"

# Exit with success
echo "Test completed successfully" 