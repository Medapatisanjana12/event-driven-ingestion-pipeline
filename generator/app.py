import os
import time
import uuid
import random
import requests
from datetime import datetime, timezone

INGESTION_URL = os.getenv("INGESTION_URL", "http://ingestion:8000/ingest")
INTERVAL = int(os.getenv("GENERATION_INTERVAL", 5))

def generate_sensor_data():
    return {
        "sensor_id": f"temp_sensor_{random.randint(1,5)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "location": {
            "latitude": round(random.uniform(-90, 90), 4),
            "longitude": round(random.uniform(-180, 180), 4)
        }
    }

if __name__ == "__main__":
    while True:
        data = generate_sensor_data()
        try:
            response = requests.post(INGESTION_URL, json=data)
            print(f"Sent: {data} | Status: {response.status_code}")
        except Exception as e:
            print(f"Error sending data: {e}")
        time.sleep(INTERVAL)
