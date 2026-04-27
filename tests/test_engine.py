from __future__ import annotations

import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.engine.predictor import scan_market  # noqa: E402


def main() -> None:
    result = scan_market(
        limit=10,
        persist_snapshot=False,
        cache_prediction=False,
        async_persist=False,
    )
    rows = result.get("rows") or []
    top_by_strategy: dict[str, dict] = {}
    for row in rows:
        strategy = row.get("strategy_type") or "尾盘突破"
        if strategy not in top_by_strategy:
            top_by_strategy[strategy] = row

    print("=== 策略引擎连通性测试 ===")
    print(f"model_status: {result.get('model_status')}")
    market_gate = result.get("market_gate") or {}
    print(f"market_gate: {market_gate.get('mode')} / blocked={market_gate.get('blocked')}")
    print(f"returned_rows: {len(rows)}")
    print("")

    for strategy in ["右侧主升浪", "中线超跌反转", "尾盘突破"]:
        row = top_by_strategy.get(strategy)
        if not row:
            print(f"[{strategy}] Top1: 空仓 / 未达到门槛")
            continue
        payload = {
            "code": row.get("code"),
            "name": row.get("name"),
            "strategy_type": row.get("strategy_type"),
            "price": row.get("price"),
            "change": row.get("change"),
            "expected_premium": row.get("expected_premium"),
            "expected_t3_max_gain_pct": row.get("expected_t3_max_gain_pct"),
            "composite_score": row.get("composite_score"),
            "sort_score": row.get("sort_score"),
            "score_threshold": row.get("score_threshold"),
        }
        print(f"[{strategy}] Top1:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
