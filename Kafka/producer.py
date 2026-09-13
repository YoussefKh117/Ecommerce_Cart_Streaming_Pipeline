import csv
import json
import time

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"

# Path to Docker
CSV_FILE = "/app/data/100k.csv"

DELAY_SECONDS = 0.001


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


with open(CSV_FILE, mode="r", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    for row in reader:

        event = {
            "event_time_str": row["event_time"],
            "event_type": row["event_type"],
            "product_id": int(row["product_id"]),
            "category_code": row["category_code"] or None,
            "brand": row["brand"] or None,
            "price": float(row["price"]),
            "user_id": int(row["user_id"])
        }

        producer.send(KAFKA_TOPIC, value=event)

        print(f"Sent: {event}")

        time.sleep(DELAY_SECONDS)


producer.flush()
producer.close()

print("Finished sending all events.")