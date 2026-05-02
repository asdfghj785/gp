from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


DATA_DIR = BASE_DIR / "data"
CONCEPT_STOCK_MAP_PATH = DATA_DIR / "concept_stock_map.json"
CONCEPT_CATALOG_PATH = DATA_DIR / "concept_catalog.json"
CONCEPT_CONSTITUENTS_PATH = DATA_DIR / "concept_constituents.json"
CONCEPT_PRIMARY_MAP_PATH = DATA_DIR / "concept_stock_primary.parquet"
CONCEPT_PRIMARY_CSV_PATH = DATA_DIR / "concept_stock_primary.csv"
SINA_CONCEPT_LIST_URL = "http://vip.stock.finance.sina.com.cn/q/view/newFLJK.php"
SINA_CONSTITUENTS_URL = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
EXCLUDED_CONCEPT_NAME_KEYWORDS = (
    "重仓",
    "QFII",
    "MSCI",
    "融资融券",
    "含H股",
    "央企50",
    "证金",
    "社保",
    "参股",
    "业绩预",
    "股权激励",
)


def fetch_sina_concepts(limit: int = 80, min_members: int = 10) -> list[dict[str, Any]]:
    session = _session()
    response = session.get(SINA_CONCEPT_LIST_URL, params={"param": "class"}, timeout=12)
    response.raise_for_status()
    text = response.text
    match = re.search(r"var\s+S_Finance_bankuai_class\s*=\s*(\{.*\});?\s*$", text, re.S)
    if not match:
        raise RuntimeError("新浪概念板块列表解析失败：没有找到 S_Finance_bankuai_class")
    payload = json.loads(match.group(1))
    concepts: list[dict[str, Any]] = []
    for value in payload.values():
        parts = str(value).split(",")
        if len(parts) < 9:
            continue
        concept = {
            "concept_code": parts[0],
            "concept_name": parts[1],
            "member_count": _safe_int(parts[2]),
            "avg_price": _safe_float(parts[3]),
            "change": _safe_float(parts[4]),
            "change_pct": _safe_float(parts[5]),
            "volume": _safe_float(parts[6]),
            "amount": _safe_float(parts[7]),
            "leader_symbol": parts[8] if len(parts) > 8 else "",
            "leader_change_pct": _safe_float(parts[9] if len(parts) > 9 else 0),
            "source": "sina.newFLJK.class",
        }
        if (
            concept["concept_code"].startswith("gn_")
            and concept["member_count"] >= min_members
            and not _excluded_concept_name(str(concept["concept_name"]))
        ):
            concept["activity_score"] = float(concept["amount"]) * (1.0 + abs(float(concept["change_pct"])) / 100.0)
            concepts.append(concept)
    concepts.sort(key=lambda item: (float(item["activity_score"]), int(item["member_count"])), reverse=True)
    return concepts[:limit] if limit > 0 else concepts


def fetch_concept_constituents(
    concept: dict[str, Any],
    *,
    session: requests.Session,
    page_size: int = 80,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    code = str(concept["concept_code"])
    for page in range(1, max_pages + 1):
        params = {
            "page": page,
            "num": page_size,
            "sort": "symbol",
            "asc": 1,
            "node": code,
            "symbol": "",
            "_s_r_a": "page",
        }
        response = session.get(SINA_CONSTITUENTS_URL, params=params, timeout=12)
        response.raise_for_status()
        try:
            items = response.json()
        except Exception as exc:
            raise RuntimeError(f"{code} 成分股 JSON 解析失败: {exc}") from exc
        if not isinstance(items, list) or not items:
            break
        for item in items:
            stock_code = normalize_stock_code(item.get("code") or item.get("symbol"))
            if not stock_code:
                continue
            rows.append(
                {
                    "stock_code": stock_code,
                    "stock_symbol": str(item.get("symbol") or ""),
                    "stock_name": str(item.get("name") or ""),
                    "concept_code": code,
                    "concept_name": str(concept["concept_name"]),
                    "trade": _safe_float(item.get("trade")),
                    "change_pct": _safe_float(item.get("changepercent")),
                    "amount": _safe_float(item.get("amount")),
                    "mktcap": _safe_float(item.get("mktcap")),
                    "nmc": _safe_float(item.get("nmc")),
                    "turnover_ratio": _safe_float(item.get("turnoverratio")),
                    "ticktime": str(item.get("ticktime") or ""),
                    "source": "sina.Market_Center.getHQNodeData",
                }
            )
        if len(items) < page_size:
            break
        time.sleep(random.uniform(0.2, 0.6))
    return rows


def build_concept_map(
    *,
    limit: int = 80,
    min_members: int = 10,
    sleep_min: float = 0.8,
    sleep_max: float = 1.8,
    log_first: int = 3,
) -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    concepts = fetch_sina_concepts(limit=limit, min_members=min_members)
    session = _session()
    all_constituents: dict[str, list[dict[str, Any]]] = {}
    membership_rows: list[dict[str, Any]] = []
    started_at = datetime.now().isoformat(timespec="seconds")

    for idx, concept in enumerate(concepts, start=1):
        rows = fetch_concept_constituents(concept, session=session)
        all_constituents[str(concept["concept_code"])] = rows
        membership_rows.extend(rows)
        if idx <= log_first:
            sample = ", ".join(f"{item['stock_code']} {item['stock_name']}" for item in rows[:8])
            print(
                f"[concept-map] {idx}/{len(concepts)} {concept['concept_code']} {concept['concept_name']} "
                f"members={len(rows)} sample={sample}"
            )
        if idx < len(concepts):
            time.sleep(random.uniform(float(sleep_min), float(sleep_max)))

    primary = choose_primary_concepts(membership_rows, concepts)
    concept_stock_map = {row["stock_code"]: [row["concept_code"]] for row in primary}
    primary_frame = pd.DataFrame(primary).sort_values(["concept_code", "stock_code"]).reset_index(drop=True)

    CONCEPT_STOCK_MAP_PATH.write_text(json.dumps(concept_stock_map, ensure_ascii=False, indent=2), encoding="utf-8")
    CONCEPT_CATALOG_PATH.write_text(json.dumps(concepts, ensure_ascii=False, indent=2), encoding="utf-8")
    CONCEPT_CONSTITUENTS_PATH.write_text(json.dumps(all_constituents, ensure_ascii=False, indent=2), encoding="utf-8")
    primary_frame.to_parquet(CONCEPT_PRIMARY_MAP_PATH, index=False)
    primary_frame.to_csv(CONCEPT_PRIMARY_CSV_PATH, index=False)

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "concept_count": len(concepts),
        "membership_rows": len(membership_rows),
        "mapped_stocks": len(concept_stock_map),
        "concept_stock_map_path": str(CONCEPT_STOCK_MAP_PATH),
        "concept_catalog_path": str(CONCEPT_CATALOG_PATH),
        "concept_constituents_path": str(CONCEPT_CONSTITUENTS_PATH),
        "primary_map_path": str(CONCEPT_PRIMARY_MAP_PATH),
    }


def choose_primary_concepts(rows: list[dict[str, Any]], concepts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    concept_score = {str(item["concept_code"]): float(item.get("activity_score") or 0.0) for item in concepts}
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        stock_code = str(row.get("stock_code") or "")
        if not stock_code:
            continue
        score = concept_score.get(str(row.get("concept_code") or ""), 0.0)
        enriched = dict(row)
        enriched["concept_activity_score"] = score
        old = best.get(stock_code)
        if old is None or score > float(old.get("concept_activity_score") or 0.0):
            best[stock_code] = enriched
    out = []
    for item in best.values():
        out.append(
            {
                "stock_code": item["stock_code"],
                "stock_name": item.get("stock_name") or "",
                "concept_code": item["concept_code"],
                "concept_name": item["concept_name"],
                "concept_activity_score": item["concept_activity_score"],
                "stock_nmc": item.get("nmc") or 0.0,
                "source": item.get("source") or "",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
    return out


def normalize_stock_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "http://vip.stock.finance.sina.com.cn/mkt/",
        }
    )
    return session


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _excluded_concept_name(name: str) -> bool:
    return any(keyword in name for keyword in EXCLUDED_CONCEPT_NAME_KEYWORDS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Sina concept stock map with randomized polite sleeps.")
    parser.add_argument("--limit", type=int, default=80, help="Number of active concept boards to fetch. 0 means all.")
    parser.add_argument("--min-members", type=int, default=10)
    parser.add_argument("--sleep-min", type=float, default=0.8)
    parser.add_argument("--sleep-max", type=float, default=1.8)
    parser.add_argument("--log-first", type=int, default=3)
    args = parser.parse_args()
    summary = build_concept_map(
        limit=args.limit,
        min_members=args.min_members,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        log_first=args.log_first,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
