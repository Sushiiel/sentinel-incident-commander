"""Real-time data connectors — LIVE third-party feeds, no API keys needed.

Every connector fetches REAL data from a public endpoint, caches it briefly to
respect rate limits, and serves the last good value on error (stale-on-error).
If a feed is unreachable it returns [] so the engine can emit a clearly-tagged
synthetic fallback event — the engine marks each event source="live" vs
"fallback", so the dashboard always shows whether it is on real data.
"""
import threading
import time

import httpx

_UA = {"User-Agent": "aicos-project/1.0 (+https://github.com)"}
_cache = {}
_lock = threading.Lock()


def _get_json(url, timeout=8):
    # httpx ships certifi's CA bundle, so TLS verification works on every host
    r = httpx.get(url, headers=_UA, timeout=timeout, follow_redirects=True)
    r.raise_for_status()
    return r.json()


def cached(key, ttl, producer):
    """Run producer() at most once per ttl seconds; stale-on-error."""
    now = time.time()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        val = producer()
    except Exception:
        val = None
    if val:
        with _lock:
            _cache[key] = (now, val)
        return val
    return hit[1] if hit else None


def hacker_news(limit=15):
    """Real Hacker News front-page stories (title + text + url)."""
    def fetch():
        ids = _get_json("https://hacker-news.firebaseio.com/v0/topstories.json")[:limit]
        out = []
        for i in ids:
            it = _get_json("https://hacker-news.firebaseio.com/v0/item/%d.json" % i, 6)
            if it and it.get("title"):
                out.append({"id": str(it["id"]), "title": it.get("title", ""),
                            "text": it.get("text", "") or it.get("title", ""),
                            "url": it.get("url", ""), "by": it.get("by", ""),
                            "score": it.get("score", 0),
                            "comments": it.get("descendants", 0),
                            "ts": it.get("time", time.time())})
        return out
    return cached("hn", 120, fetch) or []


def usgs_quakes(window="hour"):
    """Real earthquakes from the USGS live feed (last hour/day)."""
    def fetch():
        url = ("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_%s.geojson"
               % window)
        feats = _get_json(url).get("features", [])
        out = []
        for f in feats:
            p, g = f.get("properties", {}), f.get("geometry", {})
            c = (g.get("coordinates") or [0, 0, 0])
            out.append({"id": f.get("id", ""), "place": p.get("place", "unknown"),
                        "mag": p.get("mag") or 0.0, "tsunami": p.get("tsunami", 0),
                        "lon": c[0], "lat": c[1], "depth": c[2],
                        "ts": (p.get("time", 0) or 0) / 1000.0})
        return out
    return cached("usgs_%s" % window, 60, fetch) or []


def coinbase_prices(symbols=("BTC", "ETH", "SOL", "DOGE", "ADA", "XRP")):
    """Real spot prices (USD) from Coinbase's public API."""
    def fetch():
        out = []
        for s in symbols:
            d = _get_json("https://api.coinbase.com/v2/prices/%s-USD/spot" % s, 6)
            amt = ((d or {}).get("data", {}) or {}).get("amount")
            if amt:
                out.append({"symbol": s, "price": float(amt), "ts": time.time()})
        return out
    return cached("coinbase", 5, fetch) or []


def wikipedia_changes(limit=30):
    """Real recent edits to Wikipedia (live change stream)."""
    def fetch():
        url = ("https://en.wikipedia.org/w/api.php?action=query&list=recentchanges"
               "&rcprop=title|timestamp|user|sizes|type&rclimit=%d&format=json" % limit)
        rc = _get_json(url).get("query", {}).get("recentchanges", [])
        out = []
        for c in rc:
            delta = (c.get("newlen", 0) or 0) - (c.get("oldlen", 0) or 0)
            out.append({"id": str(c.get("rcid", "")), "title": c.get("title", ""),
                        "user": c.get("user", ""), "type": c.get("type", "edit"),
                        "delta": delta, "ts": time.time()})
        return out
    return cached("wiki", 8, fetch) or []


def open_meteo(stations):
    """Real current weather/air readings per (name, lat, lon) station."""
    def fetch():
        out = []
        for name, lat, lon in stations:
            url = ("https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s"
                   "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,"
                   "pressure_msl,apparent_temperature" % (lat, lon))
            cur = (_get_json(url, 6) or {}).get("current", {})
            if cur:
                out.append({"station": name, "lat": lat, "lon": lon,
                            "temp": cur.get("temperature_2m"),
                            "humidity": cur.get("relative_humidity_2m"),
                            "wind": cur.get("wind_speed_10m"),
                            "pressure": cur.get("pressure_msl"),
                            "feels": cur.get("apparent_temperature"),
                            "ts": time.time()})
        return out
    return cached("meteo", 30, fetch) or []


def github_incidents():
    """Real incidents from GitHub's public status page (statuspage.io)."""
    def fetch():
        inc = _get_json("https://www.githubstatus.com/api/v2/incidents.json").get(
            "incidents", [])
        out = []
        for i in inc:
            comps = [c.get("name", "") for c in i.get("components", [])]
            out.append({"id": i.get("id", ""), "name": i.get("name", ""),
                        "status": i.get("status", ""), "impact": i.get("impact", "none"),
                        "components": comps, "created_at": i.get("created_at", ""),
                        "ts": time.time()})
        return out
    return cached("ghstatus", 120, fetch) or []
