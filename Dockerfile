FROM python:3.11-slim

# Install PostgreSQL client tools (for psql), git (for Airflow fixtures), curl (for debugging), and Astro CLI
RUN apt-get update && \
    apt-get install -y postgresql-client git curl && \
    rm -rf /var/lib/apt/lists/*

# Install Astro CLI (pinned to version 1.34.1 to match local environment)
RUN curl -sSL https://install.astronomer.io | bash -s -- v1.34.1

WORKDIR /app

# Copy project files and install Python dependencies with UV
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen

# Copy the rest of the application
COPY . .

# Copy and set up startup script
COPY docker-startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-startup.sh

# Use startup script as entrypoint
ENTRYPOINT ["/usr/local/bin/docker-startup.sh"]
CMD ["tail", "-f", "/dev/null"]
