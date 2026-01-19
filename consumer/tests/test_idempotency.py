from unittest.mock import MagicMock
from consumer.mongo import upsert_event

def test_upsert_event_idempotent():
    mock_collection = MagicMock()

    event = {
        "sensor_id": "temp_1",
        "timestamp": "2026-01-19T10:00:00Z",
        "temperature": 25,
        "humidity": 50,
        "location": {"latitude": 10, "longitude": 20}
    }

    upsert_event(event, collection=mock_collection)
    upsert_event(event, collection=mock_collection)

    assert mock_collection.update_one.call_count == 2
