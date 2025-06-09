#!/bin/bash

# Check if requirements.txt exists
if [ -f /opt/airflow/requirements.txt ]; then
    echo "Found requirements.txt, installing dependencies..."
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
else
    echo "No requirements.txt found, skipping pip install"
fi 