"""Shared real-time layer: ring buffer + lazy ticking + WebSocket broadcast.

The domain Engine produces and processes events; this module turns that into
a live stream that works in two deployment modes:

  * persistent server — app.main runs an asyncio ticker calling tick() and
    broadcast() so /ws/live subscribers get pushed updates;
  * serverless (Vercel) — no long-lived process exists, so the REST handlers
    advance the simulation lazily from elapsed wall time (~2 events/sec,
    capped per call). Clients polling /api/live/* therefore always see a
    genuinely moving system.
"""
import json
import time
from collections import deque

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .engine import engine

router = APIRouter()

TICK_SECONDS = 0.5            # lazy mode: ~2 simulated events per wall second
MAX_CATCHUP = 50              # cap per request so cold serverless calls stay fast
BUFFER: deque = deque(maxlen=600)   # ring buffer of processed events (with seq)
CLIENTS: set = set()

_seq = 0
_last = time.monotonic()


def tick(n: int = 1) -> list:
    """Advance the simulation n steps: generate -> process -> buffer."""
    global _seq, _last
    _last = time.monotonic()
    out = []
    for _ in range(n):
        result = engine.process(engine.generate_event())
        _seq += 1
        item = {"seq": _seq, **result}
        BUFFER.append(item)
        out.append(item)
    return out


def lazy_tick() -> None:
    """Catch the simulation up to wall time (serverless-friendly liveness)."""
    due = int((time.monotonic() - _last) / TICK_SECONDS)
    if due > 0:
        tick(min(due, MAX_CATCHUP))


@router.get("/api/live/events")
def live_events(since: int = 0, limit: int = 50):
    lazy_tick()
    events = [e for e in BUFFER if e["seq"] > since]
    return {"seq": _seq, "events": events[-max(1, min(limit, 100)):]}


@router.get("/api/live/kpis")
def live_kpis():
    lazy_tick()
    return {"seq": _seq, "kpis": engine.kpis(),
            "series": engine.snapshot().get("series", {})}


def _payload(events: list) -> str:
    return json.dumps({"type": "tick", "seq": _seq, "events": events,
                       "kpis": engine.kpis(),
                       "series": engine.snapshot().get("series", {})},
                      default=str)


async def broadcast(events: list) -> None:
    dead = []
    for ws in list(CLIENTS):
        try:
            await ws.send_text(_payload(events))
        except Exception:                      # noqa: BLE001 — drop dead sockets
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)


@router.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    CLIENTS.add(ws)
    try:
        await ws.send_text(_payload(list(BUFFER)[-30:]))   # backlog on connect
        while True:
            await ws.receive_text()            # keepalive; client msgs ignored
    except WebSocketDisconnect:
        pass
    finally:
        CLIENTS.discard(ws)


tick(6)   # prime the buffer so the dashboard has data on first paint
