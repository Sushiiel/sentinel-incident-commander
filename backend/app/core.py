"""Autonomous incident-commander core: plan -> act -> observe loop over a tool registry.

The deterministic policy is intentionally transparent; swap `plan()` for an
LLM planner (LangGraph) in M2 while keeping the same tool contracts.
"""
import time, uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

class Incident(BaseModel):
    title: str
    service: str
    severity: str = "medium"   # low|medium|high|critical
    signal: str = ""           # raw alert text

INCIDENTS: dict[str, dict] = {}

# ---------------------------- tool registry ----------------------------
def tool_diagnose(inc):
    sig = (inc["signal"] + " " + inc["title"]).lower()
    if "oom" in sig or "memory" in sig:   return "memory_pressure"
    if "timeout" in sig or "latency" in sig: return "latency_regression"
    if "5xx" in sig or "error rate" in sig:  return "error_spike"
    if "disk" in sig:                      return "disk_saturation"
    return "unknown"

def tool_mitigate(diagnosis):
    return {
        "memory_pressure": "restart pods with raised memory limits (+25%)",
        "latency_regression": "roll back to last green deploy; enable request hedging",
        "error_spike": "shift traffic to standby region; freeze deploys",
        "disk_saturation": "rotate logs; expand volume; alert data-retention owner",
    }.get(diagnosis, "page on-call human — no safe automated action")

def tool_escalate(inc):
    return f"paged on-call for {inc['service']} (severity={inc['severity']})"

TOOLS = {"diagnose": tool_diagnose, "mitigate": tool_mitigate, "escalate": tool_escalate}

def plan(inc) -> list[str]:
    if inc["severity"] == "critical":
        return ["diagnose", "escalate", "mitigate"]
    return ["diagnose", "mitigate"]

# ------------------------------ endpoints ------------------------------
@router.post("/incidents")
def create(inc: Incident):
    iid = uuid.uuid4().hex[:8]
    INCIDENTS[iid] = {"id": iid, **inc.model_dump(), "status": "open", "trace": []}
    return INCIDENTS[iid]

@router.get("/incidents")
def list_incidents():
    return sorted(INCIDENTS.values(), key=lambda i: i["id"])

@router.post("/incidents/{iid}/run")
def run_agent(iid: str):
    inc = INCIDENTS.get(iid)
    if not inc:
        raise HTTPException(404, "incident not found")
    diagnosis = None
    for step in plan(inc):
        t0 = time.perf_counter()
        if step == "diagnose":
            diagnosis = TOOLS["diagnose"](inc)
            out = diagnosis
        elif step == "mitigate":
            out = TOOLS["mitigate"](diagnosis or "unknown")
        else:
            out = TOOLS["escalate"](inc)
        inc["trace"].append({"tool": step, "output": out,
                             "ms": round((time.perf_counter() - t0) * 1000, 2)})
    inc["status"] = "mitigated" if diagnosis != "unknown" else "needs_human"
    return inc
