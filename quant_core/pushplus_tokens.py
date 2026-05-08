from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union


BASE_DIR = Path(os.getenv("QUANT_BASE_DIR", "/Users/eudis/ths"))
SQLITE_PATH = Path(
    os.getenv(
        "QUANT_SQLITE_PATH",
        str(BASE_DIR / "data" / "core_db" / "quant_workstation.sqlite3"),
    )
)
TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{16,128}")


def ensure_pushplus_token_table() -> None:
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pushplus_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_sent_at TEXT,
                last_status TEXT,
                last_error TEXT,
                send_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pushplus_tokens_enabled ON pushplus_tokens(enabled)")


def list_pushplus_tokens(include_env: bool = True) -> list[dict[str, Any]]:
    ensure_pushplus_token_table()
    rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT * FROM pushplus_tokens ORDER BY enabled DESC, id ASC").fetchall():
            raw = dict(row)
            item = _public_token_row(raw, source="db")
            rows.append(item)
            seen_hashes.add(_token_hash(str(raw.get("token") or "")))
    if include_env:
        env_token = legacy_env_token()
        env_hash = _token_hash(env_token)
        if env_token and env_hash not in seen_hashes:
            env_valid = bool(TOKEN_RE.fullmatch(env_token))
            rows.append(
                {
                    "id": "env",
                    "name": "ENV PUSHPLUS_TOKEN",
                    "token_mask": mask_token(env_token),
                    "enabled": env_valid,
                    "note": ".env 兼容 token；建议迁移到可管理 token 表" if env_valid else ".env token 格式非法",
                    "source": "env",
                    "editable": False,
                    "created_at": "",
                    "updated_at": "",
                    "last_sent_at": "",
                    "last_status": "legacy_env",
                    "last_error": "",
                    "send_count": None,
                    "fail_count": None,
                }
            )
    return rows


def list_active_pushplus_tokens() -> list[dict[str, Any]]:
    ensure_pushplus_token_table()
    rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            "SELECT * FROM pushplus_tokens WHERE enabled = 1 ORDER BY id ASC"
        ).fetchall():
            item = dict(row)
            token_hash = _token_hash(item["token"])
            seen_hashes.add(token_hash)
            rows.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "token": item["token"],
                    "token_mask": mask_token(item["token"]),
                    "token_hash": token_hash,
                    "source": "db",
                }
            )
    env_token = legacy_env_token()
    env_hash = _token_hash(env_token)
    if env_token and TOKEN_RE.fullmatch(env_token) and env_hash not in seen_hashes:
        rows.append(
            {
                "id": "env",
                "name": "ENV PUSHPLUS_TOKEN",
                "token": env_token,
                "token_mask": mask_token(env_token),
                "token_hash": env_hash,
                "source": "env",
            }
        )
    return rows


def create_pushplus_token(name: str, token: str, enabled: bool = True, note: str = "") -> dict[str, Any]:
    clean_name = _clean_name(name)
    clean_token = validate_pushplus_token(token)
    now = _now()
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            cursor = conn.execute(
                """
                INSERT INTO pushplus_tokens(name, token, enabled, note, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (clean_name, clean_token, 1 if enabled else 0, str(note or "").strip(), now, now),
            )
            token_id = int(cursor.lastrowid)
    except sqlite3.IntegrityError as exc:
        raise ValueError("该 PushPlus token 已存在") from exc
    return get_pushplus_token(token_id)


def update_pushplus_token(
    token_id: Union[int, str],
    name: Optional[str] = None,
    token: Optional[str] = None,
    enabled: Optional[bool] = None,
    note: Optional[str] = None,
) -> dict[str, Any]:
    clean_id = _clean_id(token_id)
    fields: list[str] = []
    values: list[Any] = []
    if name is not None:
        fields.append("name = ?")
        values.append(_clean_name(name))
    if token is not None and str(token).strip():
        fields.append("token = ?")
        values.append(validate_pushplus_token(token))
    if enabled is not None:
        fields.append("enabled = ?")
        values.append(1 if enabled else 0)
    if note is not None:
        fields.append("note = ?")
        values.append(str(note or "").strip())
    if not fields:
        return get_pushplus_token(clean_id)
    fields.append("updated_at = ?")
    values.append(_now())
    values.append(clean_id)
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            cursor = conn.execute(
                f"UPDATE pushplus_tokens SET {', '.join(fields)} WHERE id = ?",
                values,
            )
            if cursor.rowcount == 0:
                raise KeyError(clean_id)
    except sqlite3.IntegrityError as exc:
        raise ValueError("该 PushPlus token 已存在") from exc
    return get_pushplus_token(clean_id)


def delete_pushplus_token(token_id: Union[int, str]) -> dict[str, Any]:
    clean_id = _clean_id(token_id)
    row = get_pushplus_token(clean_id)
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.execute("DELETE FROM pushplus_tokens WHERE id = ?", (clean_id,))
    return row


def get_pushplus_token(token_id: Union[int, str]) -> dict[str, Any]:
    ensure_pushplus_token_table()
    clean_id = _clean_id(token_id)
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM pushplus_tokens WHERE id = ?", (clean_id,)).fetchone()
    if not row:
        raise KeyError(clean_id)
    return _public_token_row(dict(row), source="db")


def record_pushplus_send_result(token_ref: Any, status: str, error: str = "") -> None:
    if token_ref == "env":
        return
    try:
        token_id = _clean_id(token_ref)
    except Exception:
        return
    with sqlite3.connect(SQLITE_PATH) as conn:
        if status == "sent":
            conn.execute(
                """
                UPDATE pushplus_tokens
                SET last_sent_at = ?, last_status = ?, last_error = '', send_count = send_count + 1, updated_at = ?
                WHERE id = ?
                """,
                (_now(), status, _now(), token_id),
            )
        else:
            conn.execute(
                """
                UPDATE pushplus_tokens
                SET last_status = ?, last_error = ?, fail_count = fail_count + 1, updated_at = ?
                WHERE id = ?
                """,
                (status or "failed", str(error or "")[:500], _now(), token_id),
            )


def pushplus_config_status(print_warning: bool = True) -> dict[str, Any]:
    rows = list_pushplus_tokens(include_env=True)
    active = [row for row in rows if row.get("enabled")]
    db_active = [row for row in active if row.get("source") == "db"]
    invalid_env = ""
    env_token = legacy_env_token()
    if env_token and not TOKEN_RE.fullmatch(env_token):
        invalid_env = "PUSHPLUS_TOKEN 格式非法，请检查是否包含空格或错误字符"
    if active and (db_active or not invalid_env):
        status = {
            "ok": True,
            "status": "ok",
            "reason": f"PushPlus token 已配置：启用 {len(active)} 个"
            + ("；.env token 格式异常但已由数据库 token 接管" if invalid_env else ""),
            "token_count": len(rows),
            "active_token_count": len(active),
            "token_length": 0,
        }
    else:
        reason = invalid_env or "PushPlus token 未配置，所有微信推送都会跳过"
        status = {
            "ok": False,
            "status": "critical",
            "reason": reason,
            "token_count": len(rows),
            "active_token_count": 0,
            "token_length": len(env_token) if env_token else 0,
        }
    if print_warning and not status["ok"]:
        print(f"\033[91m[CRITICAL] PushPlus 配置异常：{status['reason']}\033[0m")
    return status


def validate_pushplus_token(token: str) -> str:
    clean = str(token or "").strip()
    if not TOKEN_RE.fullmatch(clean):
        raise ValueError("PushPlus token 格式非法，应为 16-128 位字母/数字/_/-")
    return clean


def legacy_env_token() -> str:
    token = os.getenv("PUSHPLUS_TOKEN", "").strip()
    if token:
        return token
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == "PUSHPLUS_TOKEN":
            return value.strip().strip('"').strip("'")
    return ""


def mask_token(token: str) -> str:
    clean = str(token or "").strip()
    if len(clean) <= 8:
        return "*" * len(clean)
    return f"{clean[:4]}...{clean[-4:]}"


def _public_token_row(row: dict[str, Any], source: str) -> dict[str, Any]:
    token = str(row.get("token") or "")
    return {
        "id": row.get("id"),
        "name": row.get("name") or f"PushPlus #{row.get('id')}",
        "token_mask": mask_token(token),
        "enabled": bool(row.get("enabled")),
        "note": row.get("note") or "",
        "source": source,
        "editable": source == "db",
        "created_at": row.get("created_at") or "",
        "updated_at": row.get("updated_at") or "",
        "last_sent_at": row.get("last_sent_at") or "",
        "last_status": row.get("last_status") or "",
        "last_error": row.get("last_error") or "",
        "send_count": int(row.get("send_count") or 0),
        "fail_count": int(row.get("fail_count") or 0),
    }


def _token_hash(token: str) -> str:
    clean = str(token or "").strip()
    if not clean:
        return ""
    return hashlib.sha256(clean.encode("utf-8")).hexdigest()[:16]


def _clean_name(name: str) -> str:
    clean = str(name or "").strip()
    if not clean:
        raise ValueError("token 名称不能为空")
    if len(clean) > 80:
        raise ValueError("token 名称不能超过 80 个字符")
    return clean


def _clean_id(token_id: Union[int, str]) -> int:
    if str(token_id) == "env":
        raise ValueError(".env 兼容 token 不能在管理页修改")
    try:
        clean = int(token_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"非法 token id：{token_id}") from exc
    if clean <= 0:
        raise ValueError(f"非法 token id：{token_id}")
    return clean


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")
