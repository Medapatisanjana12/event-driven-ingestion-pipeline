from fastapi import FastAPI, HTTPException
from schema import SensorData
from kafka_producer import publish_to_kafka

app = FastAPI()

@app.post("/ingest")
def ingest_data(sensor_data: SensorData):
    try:
        publish_to_kafka("sensor-data", sensor_data.dict())
        return {"status": "accepted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}
