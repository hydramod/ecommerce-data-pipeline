from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_auth_health():
    r = client.get("/auth/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_info():
    r = client.get("/v1/_info")
    assert r.status_code == 200
    assert r.json()["service"] == "auth"
