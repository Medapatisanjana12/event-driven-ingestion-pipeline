from ingestion.schema import SensorData
from pydantic import ValidationError

def test_valid_sensor_data():
    data = {
        "sensor_id": "temp_1",
        "timestamp": "2026-01-19T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "location": {"latitude": 10.0, "longitude": 20.0}
    }
    sensor = SensorData(**data)
    assert sensor.sensor_id == "temp_1"

def test_invalid_latitude():
    data = {
        "sensor_id": "temp_1",
        "timestamp": "2026-01-19T10:00:00Z",
        "temperature": 25.5,
        "humidity": 60.0,
        "location": {"latitude": 200.0, "longitude": 20.0}
    }
    try:
        SensorData(**data)
        assert False
    except ValidationError:
        assert True
