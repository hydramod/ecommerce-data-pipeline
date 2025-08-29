from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()

    # App returns "healthy"/"unhealthy" and details for db + kafka
    assert data["status"] in ["healthy", "unhealthy"]
    assert "services" in data
    assert set(data["services"].keys()) == {"database", "kafka"}
    assert isinstance(data["services"]["database"], bool)
    assert isinstance(data["services"]["kafka"], bool)
