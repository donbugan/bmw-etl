from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Kafka consumer
consumer = KafkaConsumer(
    'parts_topic',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# PostgreSQL connection
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

print("Consumer started. Waiting for messages...")

for message in consumer:
    part = message.value
    print("Consumed:", part)

    # UPSERT into parts table
    cur.execute("""
        INSERT INTO parts (part_id, part_name, supplier, price, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (part_name, supplier)
        DO UPDATE SET price = EXCLUDED.price, created_at = NOW();
    """, (part["part_id"], part["part_name"], part["supplier"], part["price"]))
    conn.commit()
