import os
import time
import random
import requests
import logging
import json
from datetime import datetime, timezone
from fastapi import FastAPI
import threading

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps({
        "timestamp": "%(asctime)s",
        "level": "%(levelname)s",
        "service": "generator",
        "message": "%(message)s"
    })
)

logger = logging.getLogger(__name__)

INGESTION_URL = os.getenv("INGESTION_URL")
INTERVAL = int(os.getenv("GENERATION_INTERVAL", 5))

def generate_sensor_data():
    return {
        "sensor_id": f"temp_sensor_{random.randint(1,5)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "location": {
            "latitude": round(random.uniform(-90, 90), 4),
            "longitude": round(random.uniform(-180, 180), 4)
        }
    }

def run_generator():
    while True:
        try:
            data = generate_sensor_data()
            requests.post(INGESTION_URL, json=data, timeout=5)
            logger.info("Sensor data sent")
        except Exception as e:
            logger.error(f"Generator error: {e}")
        time.sleep(INTERVAL)

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    threading.Thread(target=run_generator).start()
