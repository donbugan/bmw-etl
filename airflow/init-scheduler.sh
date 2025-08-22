#!/bin/bash
set -e

pip install --user apache-airflow-providers-postgres kafka-python

exec airflow scheduler
