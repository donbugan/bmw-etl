from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "parts_topic",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v)
)

for message in consumer:
    print("Consumed:", message.value)
