import os
from pymongo import MongoClient
from datetime import datetime

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/?authSource=admin"
)

client = MongoClient(MONGO_URI)
collection = client.sensor_db.events

def query_mongodb(sensor_id=None, start_time=None, end_time=None, limit=100):
    query = {}

    if sensor_id:
        query["sensor_id"] = sensor_id

    if start_time or end_time:
        query["timestamp"] = {}
        if start_time:
            query["timestamp"]["$gte"] = datetime.fromisoformat(start_time)
        if end_time:
            query["timestamp"]["$lte"] = datetime.fromisoformat(end_time)

    cursor = collection.find(query).limit(limit)
    results = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results
