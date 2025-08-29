from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200

    data = response.json()
    # Status should be either "healthy" or "unhealthy"
    assert "status" in data
    assert data["status"] in ["healthy", "unhealthy"]

    # Should include details about dependent services
    assert "services" in data
    for svc in ["database", "redis", "kafka"]:
        assert svc in data["services"]
        assert isinstance(data["services"][svc], bool)
