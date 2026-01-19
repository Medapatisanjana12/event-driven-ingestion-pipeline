import os
import json
import logging
from confluent_kafka import Consumer
from mongo import upsert_event

logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sensor-consumer")

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

def process_and_save_message(message_value: dict):
    try:
        upsert_event(message_value)
        logging.info(
            f"Saved event {message_value['sensor_id']} @ {message_value['timestamp']}"
        )
    except Exception as e:
        logging.error(f"MongoDB error: {e}")

if __name__ == "__main__":
    logging.info("Kafka consumer started")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        process_and_save_message(data)
