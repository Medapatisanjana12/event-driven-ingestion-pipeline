from fastapi import FastAPI, Query
from mongo import query_mongodb

app = FastAPI()

@app.get("/data")
def get_data(
    sensor_id: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = Query(100, le=1000)
):
    return query_mongodb(sensor_id, start_time, end_time, limit)

@app.get("/health")
def health():
    return {"status": "ok"}
