"""Engine runtime-contract + live-stream tests (shared template)."""
import math

from fastapi.testclient import TestClient

from app import realtime
from app.engine import Engine
from app.main import app

client = TestClient(app)
SEVERITIES = {"ok", "warn", "critical"}


def test_generate_event_returns_dict():
    e = Engine()
    ev = e.generate_event()
    assert isinstance(ev, dict) and ev


def test_process_enriches_with_severity_and_summary():
    e = Engine()
    out = e.process(e.generate_event())
    assert out["severity"] in SEVERITIES
    assert isinstance(out["summary"], str) and out["summary"]


def test_kpis_stay_finite_over_200_events():
    e = Engine()
    for _ in range(200):
        out = e.process(e.generate_event())
        assert out["severity"] in SEVERITIES
    k = e.kpis()
    assert isinstance(k, dict) and k
    for key, v in k.items():
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        assert math.isfinite(v), f"KPI {key} is not finite: {v}"


def test_live_events_seq_advances():
    r1 = client.get("/api/live/events", params={"since": 0})
    assert r1.status_code == 200
    body = r1.json()
    assert body["events"], "ring buffer should be primed at import"
    assert all(e["severity"] in SEVERITIES for e in body["events"])
    realtime.tick(3)
    r2 = client.get("/api/live/events", params={"since": body["seq"]})
    assert r2.status_code == 200
    assert r2.json()["seq"] > body["seq"]
    assert r2.json()["events"]


def test_live_kpis_endpoint():
    r = client.get("/api/live/kpis")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["kpis"], dict) and body["kpis"]
    assert isinstance(body["series"], dict)
