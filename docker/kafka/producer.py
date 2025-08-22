from kafka import KafkaProducer
import json, uuid, time, random
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

parts = ["Turbocharger", "Exhaust Manifold", "Intercooler", "Fuel Injector"]
suppliers = ["Bosch", "Mahle", "Garrett", "Alpina"]

# PostgreSQL connection for deduplication
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

while True:
    part_name = random.choice(parts)
    supplier = random.choice(suppliers)
    price = round(random.uniform(500, 5000), 2)

    # UPSERT: insert if not exists, else update price
    cur.execute("""
        INSERT INTO parts (part_id, part_name, supplier, price, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (part_name, supplier)
        DO UPDATE SET price = EXCLUDED.price, created_at = NOW();
    """, (str(uuid.uuid4()), part_name, supplier, price))
    conn.commit()

    message = {
        "part_id": str(uuid.uuid4()),
        "part_name": part_name,
        "supplier": supplier,
        "price": price
    }
    producer.send("parts_topic", message)
    print("Produced:", message)
    time.sleep(5)
