"""Smoke tests generated from the blueprint contract."""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

SMOKE = [
  {
    "method": "post",
    "path": "/api/incidents",
    "json": {
      "title": "Checkout latency spike",
      "service": "payments",
      "severity": "high",
      "signal": "p99 latency timeout errors rising"
    }
  },
  {
    "method": "get",
    "path": "/api/incidents"
  }
]


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_smoke_endpoints():
    for case in SMOKE:
        fn = getattr(client, case["method"])
        kwargs = {"json": case["json"]} if "json" in case else {}
        r = fn(case["path"], **kwargs)
        assert r.status_code < 500, f"{case['path']} -> {r.status_code}: {r.text}"
