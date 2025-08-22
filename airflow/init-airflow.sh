#!/bin/bash
set -e

pip install --user apache-airflow-providers-postgres kafka-python

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start the webserver
exec airflow webserver
