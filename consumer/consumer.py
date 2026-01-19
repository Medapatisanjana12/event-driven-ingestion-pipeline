import os
import json
import logging
from confluent_kafka import Consumer
from mongo import upsert_event

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps({
        "timestamp": "%(asctime)s",
        "level": "%(levelname)s",
        "service": "consumer",
        "message": "%(message)s"
    })
)

logger = logging.getLogger(__name__)

conf = {
    "bootstrap.servers": os.getenv("KAFKA_BROKER"),
    "group.id": os.getenv("KAFKA_GROUP_ID"),
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe([os.getenv("KAFKA_TOPIC")])

if __name__ == "__main__":
    logger.info("Consumer started")
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(msg.error())
                continue

            data = json.loads(msg.value().decode())
            upsert_event(data)
            logger.info("Event saved to MongoDB")

        except Exception as e:
            logger.error(f"Consumer error: {e}")
