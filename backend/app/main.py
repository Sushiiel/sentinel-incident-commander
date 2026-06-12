"""Application entrypoint — mounts the domain core under /api."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .core import router

app = FastAPI(title="SENTINEL — Agentic Incident Commander", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"])
app.include_router(router, prefix="/api")

FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "index.html"


@app.get("/health")
def health():
    return {"status": "ok", "service": "sentinel-incident-commander"}


@app.get("/")
def index():
    if FRONTEND.exists():
        return FileResponse(FRONTEND)
    return {"service": "sentinel-incident-commander", "docs": "/docs"}
