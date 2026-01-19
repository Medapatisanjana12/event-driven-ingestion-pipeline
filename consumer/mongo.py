import os
from pymongo import MongoClient, ASCENDING

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/?authSource=admin"
)

client = MongoClient(MONGO_URI)
db = client.sensor_db
collection = db.events

# Unique compound index for idempotency
collection.create_index(
    [("sensor_id", ASCENDING), ("timestamp", ASCENDING)],
    unique=True
)

def upsert_event(event: dict):
    collection.update_one(
        {
            "sensor_id": event["sensor_id"],
            "timestamp": event["timestamp"]
        },
        {"$set": event},
        upsert=True
    )
