import logging
import json
from fastapi import FastAPI, HTTPException
from ingestion.schema import SensorData
from ingestion.kafka_producer import publish_to_kafka

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps({
        "timestamp": "%(asctime)s",
        "level": "%(levelname)s",
        "service": "ingestion",
        "message": "%(message)s"
    })
)

logger = logging.getLogger(__name__)
app = FastAPI()

@app.post("/ingest")
def ingest_data(sensor_data: SensorData):
    try:
        publish_to_kafka("sensor-data", sensor_data.dict())
        logger.info("Event published to Kafka")
        return {"status": "accepted"}
    except Exception as e:
        logger.error(f"Kafka publish failed: {e}")
        raise HTTPException(status_code=500, detail="Kafka publish failed")

@app.get("/health")
def health():
    return {"status": "healthy"}
