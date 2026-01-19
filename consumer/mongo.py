import os
from pymongo import MongoClient, ASCENDING

def get_collection():
    MONGO_URI = os.getenv(
        "MONGO_URI",
        "mongodb://root:rootpassword@mongodb:27017/?authSource=admin"
    )

    client = MongoClient(MONGO_URI)
    db = client.sensor_db
    collection = db.events

    collection.create_index(
        [("sensor_id", ASCENDING), ("timestamp", ASCENDING)],
        unique=True
    )

    return collection

def upsert_event(event: dict, collection=None):
    if collection is None:
        collection = get_collection()

    collection.update_one(
        {
            "sensor_id": event["sensor_id"],
            "timestamp": event["timestamp"]
        },
        {"$set": event},
        upsert=True
    )
