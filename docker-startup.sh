#!/bin/bash

echo "🐳 DE-Bench Docker Container Starting..."

# Auto-convert localhost URLs to host.docker.internal for Docker networking
echo "🔄 Converting localhost URLs to host.docker.internal..."

# Create environment file for persistent variables
ENV_FILE="/app/.docker-env"
echo "# Auto-generated Docker environment variables" > "$ENV_FILE"

# Process all environment variables
for var in $(env | grep -E '(localhost|127\.0\.0\.1)' | cut -d= -f1); do
    old_value="${!var}"
    new_value=$(echo "$old_value" | sed -e 's/localhost/host.docker.internal/g' -e 's/127\.0\.0\.1/host.docker.internal/g')
    export "$var"="$new_value"
    echo "export $var=\"$new_value\"" >> "$ENV_FILE"
    echo "  ✅ $var: $old_value → $new_value"
done

# Source the environment file in bash profile for interactive shells
echo "source /app/.docker-env" >> /root/.bashrc

echo "🚀 Environment ready! Starting container..."

# Execute the original command (or keep container running)
exec "$@"
