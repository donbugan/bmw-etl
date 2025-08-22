from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

with DAG("parts_pipeline", default_args=default_args, schedule="@hourly", catchup=False) as dag:

    create_audit = PostgresOperator(
        task_id="insert_audit",
        postgres_conn_id="postgres_default",
        sql="INSERT INTO audit_log (action, part_id) SELECT 'CHECK', part_id FROM parts;"
    )
