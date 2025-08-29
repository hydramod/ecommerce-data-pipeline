# services/payment/tests/test_health.py
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200

    data = r.json()
    # status should be "healthy" or "unhealthy"
    assert data["status"] in ["healthy", "unhealthy"]
    assert "services" in data
    assert "kafka" in data["services"]
    assert isinstance(data["services"]["kafka"], bool)
