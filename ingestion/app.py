from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/ingest")
async def ingest_data(request: Request):
    data = await request.json()
    print("Received data:", data)
    return {"status": "received"}

@app.get("/health")
def health():
    return {"status": "ok"}
