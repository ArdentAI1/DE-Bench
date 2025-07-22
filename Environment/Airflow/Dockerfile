FROM apache/airflow:2.10.5

USER root

# Copy requirements.txt into the container
COPY /Requirements/requirements.txt /opt/airflow/requirements.txt

# Switch back to airflow user for pip install
USER airflow

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
