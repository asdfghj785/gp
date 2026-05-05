from __future__ import annotations

import json
from typing import Any

from .news_fetcher import format_news_context


SYSTEM_PROMPT = """你是本地 A 股量化工作站的 14:46 舆情风控访谈官。
你只做定性风险排查，不预测价格，不替代 XGBoost 排序。
你必须基于输入中的候选股、量化特征和新闻线索输出严格 JSON。
若新闻为空或无法验证，必须明确说明“未发现可验证突发风险”，不能编造事实。"""


def build_interview_prompt(candidates: list[dict[str, Any]], news_by_code: dict[str, list[dict[str, str]]]) -> str:
    compact_candidates = []
    for row in candidates:
        compact_candidates.append(
            {
                "code": row.get("code"),
                "name": row.get("name"),
                "strategy_type": row.get("strategy_type"),
                "price": row.get("price") or row.get("snapshot_price") or row.get("selection_price"),
                "change_pct": row.get("change") or row.get("selection_change"),
                "expected_premium": row.get("expected_premium"),
                "expected_t3_max_gain_pct": row.get("expected_t3_max_gain_pct"),
                "composite_score": row.get("composite_score"),
                "sort_score": row.get("sort_score"),
                "volume_ratio": row.get("volume_ratio"),
                "market_gate_mode": row.get("market_gate_mode"),
            }
        )

    return f"""请对以下 14:45/14:50 候选股做“舆情与风险排查定论”。

候选股量化摘要:
{json.dumps(compact_candidates, ensure_ascii=False, indent=2)}

今日新闻/搜索线索:
{format_news_context(news_by_code)}

输出格式必须是严格 JSON，不要 Markdown，不要解释 JSON 外的文字:
{{
  "summary": "一句话总评，20-40字",
  "items": [
    {{
      "code": "股票代码",
      "name": "股票名称",
      "risk_level": "低/中/高",
      "verdict": "一句话定性判断",
      "reason": "用新闻和量化摘要解释，不能编造",
      "action_hint": "通过/谨慎/放弃"
    }}
  ]
}}
"""
