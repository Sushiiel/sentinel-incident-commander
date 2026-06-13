"""In-project OpenRouter client for live LLM reasoning.

The key is read from the OPENROUTER_API_KEY environment variable (injected as a
deployment env var by the platform). Every call degrades to None on any failure
so the engine's deterministic algorithm always keeps the stream alive.
"""
import os

import httpx

_KEY = os.environ.get("OPENROUTER_API_KEY", "")
_MODEL = os.environ.get("OPENROUTER_MODEL", "deepseek/deepseek-chat-v3-0324:free")


def available():
    return bool(_KEY)


def model():
    return _MODEL


def complete(prompt, system="You are a precise real-time analyst. Be concise.",
             max_tokens=350):
    if not _KEY:
        return None
    try:
        r = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": "Bearer " + _KEY,
                     "Content-Type": "application/json",
                     "HTTP-Referer": "https://ai-career-os.local",
                     "X-Title": "AICOS Live Project"},
            json={"model": _MODEL, "max_tokens": max_tokens,
                  "messages": [{"role": "system", "content": system},
                               {"role": "user", "content": prompt}]},
            timeout=30)
        r.raise_for_status()
        return (r.json()["choices"][0]["message"]["content"] or "").strip() or None
    except Exception:
        return None
