from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:11434/v1"
DEFAULT_MODEL = "deepseek-r1"


def _load_local_env() -> None:
    env_path = Path(os.getenv("QUANT_BASE_DIR", "/Users/eudis/ths")) / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_local_env()


@dataclass(frozen=True)
class OllamaConfig:
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    timeout: int = 30


def ollama_config() -> OllamaConfig:
    return OllamaConfig(
        base_url=_resolve_base_url(),
        model=os.getenv("AI_AGENT_OLLAMA_MODEL") or os.getenv("OLLAMA_MODEL") or DEFAULT_MODEL,
        timeout=int(os.getenv("AI_AGENT_OLLAMA_TIMEOUT", "30")),
    )


def chat_completion(
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float = 0.2,
    max_tokens: int = 900,
    config: OllamaConfig | None = None,
) -> dict[str, Any]:
    """Call local Ollama's OpenAI-compatible chat endpoint without any API key."""
    cfg = config or ollama_config()
    url = f"{cfg.base_url}/chat/completions"
    payload = {
        "model": cfg.model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    try:
        response = _post_json(url, payload, timeout=cfg.timeout)
        response.raise_for_status()
        data = response.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        content = strip_reasoning(str(content)).strip()
        return {"ok": True, "model": cfg.model, "content": content, "raw": data}
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 404:
            return _generate_completion(system_prompt, user_prompt, temperature=temperature, max_tokens=max_tokens, config=cfg)
        return {
            "ok": False,
            "model": cfg.model,
            "content": "",
            "error": str(exc),
            "base_url": cfg.base_url,
        }
    except Exception as exc:
        return {
            "ok": False,
            "model": cfg.model,
            "content": "",
            "error": str(exc),
            "base_url": cfg.base_url,
        }


def _generate_completion(
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float,
    max_tokens: int,
    config: OllamaConfig,
) -> dict[str, Any]:
    """Fallback for older Ollama instances exposing only /api/generate."""
    api_base = config.base_url[:-3] if config.base_url.endswith("/v1") else config.base_url
    url = f"{api_base}/api/generate"
    prompt = f"{system_prompt}\n\n{user_prompt}"
    payload = {
        "model": config.model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens,
        },
    }
    try:
        response = _post_json(url, payload, timeout=config.timeout)
        response.raise_for_status()
        data = response.json()
        content = strip_reasoning(str(data.get("response") or "")).strip()
        return {"ok": True, "model": config.model, "content": content, "raw": data, "endpoint": "/api/generate"}
    except Exception as exc:
        return {
            "ok": False,
            "model": config.model,
            "content": "",
            "error": str(exc),
            "base_url": api_base,
            "endpoint": "/api/generate",
        }


def _resolve_base_url() -> str:
    explicit = os.getenv("AI_AGENT_OLLAMA_BASE_URL")
    if explicit:
        return explicit.rstrip("/")

    legacy_api = os.getenv("OLLAMA_API", "").strip()
    if legacy_api:
        return _base_url_from_legacy_api(legacy_api).rstrip("/")
    return DEFAULT_BASE_URL


def _base_url_from_legacy_api(value: str) -> str:
    cleaned = value.rstrip("/")
    for suffix in ("/api/generate", "/api/chat"):
        if cleaned.endswith(suffix):
            return f"{cleaned[: -len(suffix)]}/v1"
    if cleaned.endswith("/v1/chat/completions"):
        return cleaned[: -len("/chat/completions")]
    if cleaned.endswith("/v1"):
        return cleaned
    return DEFAULT_BASE_URL


def _post_json(url: str, payload: dict[str, Any], *, timeout: int) -> requests.Response:
    if _is_loopback_url(url):
        session = requests.Session()
        session.trust_env = False
        return session.post(url, json=payload, timeout=timeout, proxies={"http": None, "https": None})
    return requests.post(url, json=payload, timeout=timeout)


def _is_loopback_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


def strip_reasoning(text: str) -> str:
    """DeepSeek-R1 may return <think> blocks; remove them before JSON parsing or pushing."""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()


def extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = strip_reasoning(text)
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None
