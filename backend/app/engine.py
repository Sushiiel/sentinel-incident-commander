"""SENTINEL domain engine — agentic incident-commander over a streaming alert feed.

Architecture (pure stdlib):
  generate_event()  Synthetic alert stream from services with independent health
                    state that drifts over time (Gaussian random walk). Signals:
                    cpu_spike, latency_p99, error_rate, disk_full, oom, cert_expiry
                    plus rarer compound signals. Cascading incidents: upstream
                    failure queues degraded-downstream alerts. Novel/unknown
                    signatures injected ~5% of events to exercise escalation.
  process()         Plan→act→observe agent loop:
                      1. diagnose — correlates signal evidence against a weighted
                         rulebook to produce (hypothesis, confidence).
                      2. mitigate — applies the matching playbook action; retries
                         once with the alternate action on first failure.
                      3. escalate — fires for novel signals or exhausted retries.
                    Returns severity (ok/warn/critical), summary, root_cause,
                    resolution, resolution_ms, and the full per-step trace.
  kpis()            incidents_handled, auto_resolved_pct, mean_resolution_ms,
                    escalations, active_incidents.
  snapshot()        recent + series: incident_rate, auto_resolve_rate.
"""
import random, time
from collections import deque

SERVICES = ["api-gateway", "auth-service", "payments", "cart", "inventory",
            "notifications", "search", "ml-inference", "data-pipeline", "cdn"]
DEPENDENCIES = {
    "auth-service": ["api-gateway", "payments", "cart"],
    "payments": ["cart"],
    "data-pipeline": ["ml-inference", "search"],
    "cdn": ["api-gateway"],
    "inventory": ["cart", "search"],
}
SIGNALS = ["cpu_spike", "latency_p99", "error_rate", "disk_full", "oom",
           "cert_expiry", "connection_pool_exhausted", "memory_leak",
           "deployment_rollback", "network_partition"]
NOVEL_SIGNALS = ["quantum_flux_anomaly", "shadow_replica_divergence",
                 "dark_traffic_surge", "entropy_cascade_fault"]
# (frozenset of signals that must be present) -> (root_cause, base_confidence)
DIAGNOSIS_RULES = [
    ({"oom", "memory_leak"},                    "memory_pressure",     0.92),
    ({"oom"},                                   "memory_pressure",     0.80),
    ({"latency_p99", "cpu_spike"},              "resource_contention", 0.88),
    ({"latency_p99", "connection_pool_exhausted"}, "db_saturation",   0.91),
    ({"latency_p99"},                           "latency_regression",  0.70),
    ({"error_rate", "deployment_rollback"},     "bad_deploy",          0.95),
    ({"error_rate", "network_partition"},       "network_fault",       0.89),
    ({"error_rate"},                            "error_spike",         0.72),
    ({"disk_full"},                             "disk_saturation",     0.97),
    ({"cpu_spike"},                             "resource_contention", 0.75),
    ({"cert_expiry"},                           "expired_certificate", 0.99),
    ({"connection_pool_exhausted"},             "db_saturation",       0.82),
    ({"network_partition"},                     "network_fault",       0.85),
    ({"memory_leak"},                           "memory_pressure",     0.78),
    ({"deployment_rollback"},                   "bad_deploy",          0.80),
]
# root_cause -> [primary_action, fallback_action]
PLAYBOOKS = {
    "memory_pressure":     ["restart_pods_higher_limits",   "evict_caches_restart"],
    "resource_contention": ["horizontal_scale_out",         "throttle_non_critical"],
    "latency_regression":  ["rollback_last_green_deploy",   "enable_request_hedging"],
    "db_saturation":       ["scale_connection_pool",        "failover_read_replica"],
    "bad_deploy":          ["rollback_deployment",          "freeze_deploys_page_eng"],
    "error_spike":         ["shift_traffic_standby_region", "enable_circuit_breakers"],
    "disk_saturation":     ["rotate_compress_logs",         "expand_pv_claim"],
    "expired_certificate": ["rotate_cert_cert_manager",     "manual_cert_renewal"],
    "network_fault":       ["failover_secondary_az",        "activate_degraded_mode"],
}
METRIC_RANGES = {
    "cpu_spike": (72.0, 99.5), "latency_p99": (800.0, 15000.0),
    "error_rate": (3.0, 42.0), "disk_full": (85.0, 99.8),
    "oom": (0.0, 1.0), "cert_expiry": (0.0, 6.0),
    "connection_pool_exhausted": (90.0, 100.0), "memory_leak": (78.0, 97.0),
    "deployment_rollback": (1.0, 1.0), "network_partition": (1.0, 1.0),
}
ON_CALL = {
    "payments": "payments-oncall", "cart": "checkout-oncall",
    "auth-service": "platform-oncall", "api-gateway": "platform-oncall",
    "ml-inference": "ml-oncall", "data-pipeline": "data-oncall",
    "search": "search-oncall", "notifications": "platform-oncall",
    "inventory": "inventory-oncall", "cdn": "infra-oncall",
}


def _metric_val(signal, rng):
    lo, hi = METRIC_RANGES.get(signal, (0.0, 1.0))
    return round(rng.uniform(lo, hi), 2)


class Engine:
    """Stream-processing facade implementing the shared runtime contract."""

    def __init__(self):
        self.rng = random.Random(time.time_ns() & 0xFFFFFFFF)
        self.health = {s: 1.0 for s in SERVICES}   # per-service health [0,1]
        self.active: dict[str, dict] = {}
        self.cascade_q: deque = deque()
        self.recent: deque = deque(maxlen=120)
        self.res_ms_win: deque = deque(maxlen=200)
        self.minute_ts: deque = deque(maxlen=900)
        self.auto_win: deque = deque(maxlen=200)    # 1=auto-resolved
        self.series = {"incident_rate":    deque(maxlen=90),
                       "auto_resolve_rate": deque(maxlen=90)}
        self.incidents_handled = 0
        self.escalations = 0
        self._seq = 0
        self.health[self.rng.choice(SERVICES)] = 0.6  # seed one degraded svc

    # --------------------------------------------------- event stream
    def generate_event(self) -> dict:
        if self.cascade_q:
            return self.cascade_q.popleft()
        # Gaussian health drift on a random service
        svc = self.rng.choice(SERVICES)
        self.health[svc] = max(0.05, min(1.0,
            self.health[svc] + self.rng.gauss(0.0, 0.06)))
        r = self.rng.random()
        if r < 0.05:                        # novel/unknown signal
            return self._alert(svc, self.rng.choice(NOVEL_SIGNALS), "critical",
                               novel=True)
        if r < 0.12:                        # cascading multi-service incident
            return self._cascade()
        # Weight alert probability inversely to health
        weights = [max(0.01, 1.0 - self.health[s]) for s in SERVICES]
        total = sum(weights); roll = self.rng.uniform(0, total)
        chosen = SERVICES[0]
        for s, w in zip(SERVICES, weights):
            roll -= w
            if roll <= 0:
                chosen = s; break
        h = self.health[chosen]
        if h < 0.25:
            sig = self.rng.choice(["oom", "error_rate", "disk_full",
                                   "connection_pool_exhausted", "network_partition"])
        elif h < 0.55:
            sig = self.rng.choice(["cpu_spike", "latency_p99", "error_rate",
                                   "memory_leak", "deployment_rollback"])
        else:
            sig = self.rng.choice(SIGNALS[:6])
        hint = "critical" if h < 0.3 else "warn" if h < 0.7 else "ok"
        return self._alert(chosen, sig, hint)

    def _alert(self, svc, signal, hint="warn", novel=False, parent=None):
        self._seq += 1
        return {"incident_id": "INC-%05d" % self._seq, "ts": time.time(),
                "service": svc, "signal": signal, "severity_hint": hint,
                "metric_value": _metric_val(signal, self.rng),
                "novel": novel, "cascade_parent": parent,
                "environment": self.rng.choice(["prod","prod","prod","staging"]),
                "region": self.rng.choice(["us-east-1","eu-west-1","ap-south-1"])}

    def _cascade(self):
        up = self.rng.choice(list(DEPENDENCIES))
        self.health[up] = max(0.05, self.health[up] - 0.4)
        root = self._alert(up, self.rng.choice(["error_rate","latency_p99",
                                                 "cpu_spike"]), "critical")
        for ds in self.rng.sample(DEPENDENCIES[up],
                                   k=min(2, len(DEPENDENCIES[up]))):
            self.health[ds] = max(0.1, self.health[ds] - 0.2)
            self.cascade_q.append(self._alert(
                ds, self.rng.choice(["latency_p99","error_rate",
                                     "connection_pool_exhausted"]),
                "warn", parent=root["incident_id"]))
        return root

    # ------------------------------------------------------- process
    def process(self, event: dict) -> dict:
        t0 = time.perf_counter()
        inc_id = event.get("incident_id", "INC-?")
        svc = event.get("service", "unknown")
        signal = event.get("signal", "")
        novel = event.get("novel", False)
        self.active[inc_id] = event
        trace: list = []
        root_cause = "unknown"
        resolution = "unresolved"
        severity = "warn"
        escalated = False
        plan = (["diagnose", "escalate"] if novel else
                ["diagnose", "escalate", "mitigate"]
                if event.get("severity_hint") == "critical"
                else ["diagnose", "mitigate"])
        diag = miti = None
        for step in plan:
            st = time.perf_counter()
            if step == "diagnose":
                diag = self._diagnose(event)
                root_cause = diag["hypothesis"]
                trace.append({"step": len(trace)+1, "tool": "diagnose",
                               "input": {"service": svc, "signal": signal,
                                         "novel": novel},
                               "output": diag,
                               "latency_ms": round((time.perf_counter()-st)*1000,3),
                               "ts": time.time()})
            elif step == "mitigate":
                if diag is None:
                    diag = self._diagnose(event); root_cause = diag["hypothesis"]
                miti = self._mitigate(root_cause, svc, attempt=1)
                trace.append({"step": len(trace)+1, "tool": "mitigate",
                               "input": {"hypothesis": root_cause, "service": svc},
                               "output": miti,
                               "latency_ms": round((time.perf_counter()-st)*1000,3),
                               "ts": time.time()})
                if not miti["success"]:
                    st2 = time.perf_counter()
                    miti = self._mitigate(root_cause, svc, attempt=2)
                    trace.append({"step": len(trace)+1, "tool": "mitigate_retry",
                                   "input": {"hypothesis": root_cause, "attempt": 2},
                                   "output": miti,
                                   "latency_ms": round((time.perf_counter()-st2)*1000,3),
                                   "ts": time.time()})
            elif step == "escalate":
                esc = self._escalate(event, root_cause)
                escalated = True
                trace.append({"step": len(trace)+1, "tool": "escalate",
                               "input": {"service": svc, "root_cause": root_cause},
                               "output": esc,
                               "latency_ms": round((time.perf_counter()-st)*1000,3),
                               "ts": time.time()})
        res_ms = round((time.perf_counter()-t0)*1000, 3)
        if escalated:
            resolution = "escalated"; severity = "critical"
        elif miti and miti.get("success"):
            retried = sum(1 for s in trace if s["tool"] == "mitigate_retry")
            resolution = "auto_resolved"
            severity = "warn" if retried else "ok"
            self.health[svc] = min(1.0, self.health.get(svc, 0.5) + 0.15)
        else:
            resolution = "unresolved"; severity = "critical"
        self.incidents_handled += 1
        self.minute_ts.append(time.time())
        self.auto_win.append(1 if resolution == "auto_resolved" else 0)
        if resolution in ("escalated", "unresolved"):
            self.escalations += 1
        if resolution == "auto_resolved":
            self.res_ms_win.append(res_ms)
        self.active.pop(inc_id, None)
        summary = "[%s] %s/%s -> %s (%s) in %.1fms" % (
            severity.upper(), svc, signal, resolution,
            root_cause.replace("_", " "), res_ms)
        out = {**event, "severity": severity, "summary": summary,
               "root_cause": root_cause, "resolution": resolution,
               "resolution_ms": res_ms, "trace": trace, "plan": plan,
               "score": {"ok": 0, "warn": 50, "critical": 100}.get(severity, 50)}
        self.recent.append(out)
        k = self.kpis()
        self.series["incident_rate"].append(k["incidents_handled"])
        self.series["auto_resolve_rate"].append(k["auto_resolved_pct"])
        return out

    # ---------------------------------------------------- tool registry
    def _diagnose(self, event: dict) -> dict:
        """Correlate signal evidence against weighted rulebook."""
        signal = event.get("signal", "")
        evidence = {signal}
        if event.get("cascade_parent"):
            evidence.add("network_partition")
        best, best_conf, matched = "unknown_anomaly", 0.30, []
        for rule_sigs, hypothesis, base_conf in DIAGNOSIS_RULES:
            overlap = evidence & rule_sigs
            if not overlap:
                continue
            conf = base_conf * (0.6 + 0.4 * len(overlap) / len(rule_sigs))
            if conf > best_conf:
                best, best_conf, matched = hypothesis, conf, list(overlap)
        return {"hypothesis": best, "confidence": round(best_conf, 3),
                "evidence": list(evidence), "matched_signals": matched}

    def _mitigate(self, hypothesis: str, svc: str, attempt: int = 1) -> dict:
        """Apply playbook action; success modelled by service health."""
        pb = PLAYBOOKS.get(hypothesis, [])
        if not pb:
            return {"success": False, "action": "no_playbook",
                    "reason": "No playbook for: %s" % hypothesis}
        action = pb[min(attempt-1, len(pb)-1)]
        h = self.health.get(svc, 0.5)
        prob = min(0.97, (0.85 if attempt == 1 else 0.75) * max(0.5, h + 0.3))
        ok = self.rng.random() < prob
        return {"success": ok, "action": action, "attempt": attempt,
                "hypothesis": hypothesis,
                "reason": "Applied successfully." if ok else
                           "Action failed; retrying alternate."}

    def _escalate(self, event: dict, root_cause: str) -> dict:
        """Page on-call when no safe automated action exists."""
        svc = event.get("service", "unknown")
        return {"paged_team": ON_CALL.get(svc, "sre-oncall"),
                "channel": "pagerduty",
                "root_cause": root_cause,
                "service": svc,
                "region": event.get("region", "unknown"),
                "runbook_url": "https://runbooks.internal/%s" % root_cause,
                "message": "SENTINEL escalation: %s/%s — %s. On-call engaged." % (
                    svc, event.get("signal", "?"),
                    root_cause.replace("_", " "))}

    # -------------------------------------------------- API helpers
    def submit_incident(self, incident_id: str, service: str, signal: str,
                        severity_hint: str = "warn",
                        region: str = "us-east-1") -> dict:
        self._seq += 1
        event = {"incident_id": incident_id or ("INC-%05d" % self._seq),
                 "ts": time.time(), "service": service, "signal": signal,
                 "severity_hint": severity_hint,
                 "metric_value": _metric_val(signal, self.rng),
                 "novel": signal in NOVEL_SIGNALS,
                 "cascade_parent": None, "environment": "prod", "region": region}
        return self.process(event)

    def get_trace(self, incident_id: str) -> dict | None:
        for item in self.recent:
            if item.get("incident_id") == incident_id:
                return item
        return None

    def list_tools(self) -> list:
        return [
            {"name": "diagnose",
             "description": "Correlate multi-signal evidence to root-cause hypothesis",
             "inputs": ["service", "signal", "metric_value", "novel"],
             "outputs": ["hypothesis", "confidence", "evidence", "matched_signals"]},
            {"name": "mitigate",
             "description": "Apply playbook action for hypothesis (with retry)",
             "inputs": ["hypothesis", "service"],
             "outputs": ["success", "action", "attempt", "reason"]},
            {"name": "escalate",
             "description": "Page on-call when automation cannot resolve",
             "inputs": ["service", "root_cause", "severity_hint"],
             "outputs": ["paged_team", "channel", "runbook_url", "message"]},
        ]

    # ---------------------------------------------------------------- kpis
    def kpis(self) -> dict:
        now = time.time()
        rate = sum(1 for t in self.minute_ts if now - t <= 60)
        n = len(self.auto_win)
        auto_pct = round(100.0 * sum(self.auto_win) / n, 1) if n else 0.0
        mean_ms = (round(sum(self.res_ms_win) / len(self.res_ms_win), 2)
                   if self.res_ms_win else 0.0)
        return {"incidents_handled":  self.incidents_handled,
                "auto_resolved_pct":  auto_pct,
                "mean_resolution_ms": mean_ms,
                "escalations":        self.escalations,
                "active_incidents":   len(self.active)}

    def snapshot(self) -> dict:
        return {"recent": list(self.recent)[-30:],
                "series": {k: list(v) for k, v in self.series.items()}}


engine = Engine()
