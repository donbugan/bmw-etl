from kafka import KafkaProducer
import json, uuid, time, random
import psycopg2

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="bmw-etl-kafka-1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

parts = ["Turbocharger", "Exhaust Manifold", "Intercooler", "Fuel Injector"]
suppliers = ["Bosch", "Mahle", "Garrett", "Alpina"]

# PostgreSQL connection for deduplication
conn = psycopg2.connect(
    host="localhost",
    database="carparts",
    user="parts_admin",
    password="secret"
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
