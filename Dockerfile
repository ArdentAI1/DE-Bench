FROM python:3.11-slim

# Install PostgreSQL client tools (for psql), git (for Airflow fixtures), curl (for debugging), and Astro CLI
RUN apt-get update && \
    apt-get install -y postgresql-client git curl && \
    rm -rf /var/lib/apt/lists/*

# Install Astro CLI (pinned to version 1.34.1 to match local environment)
RUN curl -sSL https://install.astronomer.io | bash -s -- v1.34.1

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files and install Python dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy the rest of the application
COPY . .

# Copy and set up startup script
COPY docker-startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-startup.sh

# Use startup script as entrypoint
ENTRYPOINT ["/usr/local/bin/docker-startup.sh"]
CMD ["tail", "-f", "/dev/null"]
