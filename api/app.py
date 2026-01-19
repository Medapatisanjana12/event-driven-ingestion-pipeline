from fastapi import FastAPI

app = FastAPI()

@app.get("/data")
def get_data(sensor_id: str = None, start_time: str = None, end_time: str = None):
    return {"message": "MongoDB query placeholder"}

@app.get("/health")
def health():
    return {"status": "ok"}
