from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

KAFKA_TOPIC = "parts_topic"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

PG_HOST = "postgres"
PG_DB = "carparts"
PG_USER = "parts_admin"
PG_PASSWORD = "secret"

def consume_kafka_to_postgres():
    # Connect to Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="airflow-group"
    )

    # Connect to Postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parts (
            part_id UUID PRIMARY KEY,
            part_name TEXT,
            supplier TEXT,
            price NUMERIC
        );
    """)
    conn.commit()

    for message in consumer:
        part = message.value
        cursor.execute("""
            INSERT INTO parts (part_id, part_name, supplier, price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (part_id) DO NOTHING;
        """, (part["part_id"], part["part_name"], part["supplier"], part["price"]))
        conn.commit()
        print("Inserted into Postgres:", part)

    cursor.close()
    conn.close()

with DAG(
    'kafka_to_postgres',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='consume_kafka_to_postgres',
        python_callable=consume_kafka_to_postgres
    )

    ingest_task
