from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

from .llm_engine import chat_completion, extract_json_object
from .news_fetcher import fetch_batch_news
from .prompts_repo import SYSTEM_PROMPT, build_interview_prompt


def run_1446_ai_interview(
    stock_codes: list[str],
    stock_names: list[str],
    candidate_rows: list[dict[str, Any]] | None = None,
    *,
    use_llm: bool = True,
) -> dict[str, Any]:
    """Run the local 14:46 AI risk interview for selected XGBoost candidates."""
    codes = [str(code).strip() for code in stock_codes if str(code).strip()]
    names = [str(name).strip() for name in stock_names]
    if not codes:
        return {"status": "empty", "items": [], "markdown": "### AI舆情与风险排查\n- 无候选股。"}

    candidate_map = _candidate_rows(codes, names, candidate_rows)
    news_by_code = fetch_batch_news(codes, [candidate_map[code].get("name", code) for code in codes])
    prompt = build_interview_prompt([candidate_map[code] for code in codes], news_by_code)

    if not use_llm:
        parsed = _fallback_payload(codes, candidate_map, "演示模式：未调用本地 Ollama。")
        return _build_result(parsed, news_by_code, "dry_run", model="none", raw_content="")

    response = chat_completion(SYSTEM_PROMPT, prompt)
    if response.get("ok"):
        parsed = extract_json_object(str(response.get("content") or ""))
        if parsed:
            return _build_result(parsed, news_by_code, "ok", model=str(response.get("model") or ""), raw_content=str(response.get("content") or ""))
        parsed = _fallback_payload(codes, candidate_map, "模型未返回可信 JSON，已降级为新闻线索摘要。")
        return _build_result(parsed, news_by_code, "invalid_json", model=str(response.get("model") or ""), raw_content=str(response.get("content") or ""))

    parsed = _fallback_payload(codes, candidate_map, f"Ollama 不可用：{response.get('error')}")
    return _build_result(parsed, news_by_code, "llm_unavailable", model=str(response.get("model") or ""), raw_content="")


def attach_ai_interview(rows: list[dict[str, Any]], interview: dict[str, Any]) -> list[dict[str, Any]]:
    by_code = {str(item.get("code")): item for item in interview.get("items", []) if item.get("code")}
    for row in rows:
        item = by_code.get(str(row.get("code")))
        if item:
            row["ai_interview"] = item
    return rows


def _candidate_rows(
    codes: list[str],
    names: list[str],
    candidate_rows: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    rows_by_code = {str(row.get("code")): dict(row) for row in candidate_rows or [] if row.get("code")}
    out: dict[str, dict[str, Any]] = {}
    for index, code in enumerate(codes):
        row = rows_by_code.get(code, {})
        row.setdefault("code", code)
        row.setdefault("name", names[index] if index < len(names) and names[index] else code)
        out[code] = row
    return out


def _fallback_payload(codes: list[str], candidate_map: dict[str, dict[str, Any]], reason: str) -> dict[str, Any]:
    return {
        "summary": reason,
        "items": [
            {
                "code": code,
                "name": candidate_map[code].get("name", code),
                "risk_level": "中",
                "verdict": "AI舆情结论未完全生成，按量化规则与人工复核执行。",
                "reason": reason,
                "action_hint": "谨慎",
            }
            for code in codes
        ],
    }


def _build_result(
    parsed: dict[str, Any],
    news_by_code: dict[str, list[dict[str, str]]],
    status: str,
    *,
    model: str,
    raw_content: str,
) -> dict[str, Any]:
    items = parsed.get("items")
    if not isinstance(items, list):
        items = []
    normalized = [_normalize_item(item) for item in items if isinstance(item, dict)]
    result = {
        "status": status,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "model": model,
        "summary": str(parsed.get("summary") or ""),
        "items": normalized,
        "news": news_by_code,
        "raw_content": raw_content,
    }
    result["markdown"] = format_ai_markdown(result)
    return result


def _normalize_item(item: dict[str, Any]) -> dict[str, str]:
    return {
        "code": str(item.get("code") or ""),
        "name": str(item.get("name") or ""),
        "risk_level": str(item.get("risk_level") or "中"),
        "verdict": str(item.get("verdict") or "未给出结论"),
        "reason": str(item.get("reason") or ""),
        "action_hint": str(item.get("action_hint") or "谨慎"),
    }


def format_ai_markdown(result: dict[str, Any]) -> str:
    status = result.get("status") or "-"
    model = result.get("model") or "-"
    lines = [
        "### AI舆情与风险排查",
        f"- 状态：{status} / 模型：{model}",
    ]
    summary = str(result.get("summary") or "").strip()
    if summary:
        lines.append(f"- 总评：{summary}")
    for item in result.get("items", []):
        level = item.get("risk_level", "中")
        hint = item.get("action_hint", "谨慎")
        lines.append(
            f"- 【{item.get('name')}({item.get('code')})】风险:{level} / 建议:{hint} / "
            f"{item.get('verdict')}；{item.get('reason')}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="14:46 local AI risk interview")
    parser.add_argument("--demo", action="store_true", help="Print a demo Markdown report")
    parser.add_argument("--no-llm", action="store_true", help="Do not call local Ollama")
    args = parser.parse_args()
    if args.demo:
        rows = [
            {"code": "002709", "name": "天赐材料", "strategy_type": "右侧主升浪", "price": 21.36, "expected_t3_max_gain_pct": 6.8, "composite_score": 6.8},
            {"code": "600865", "name": "百大集团", "strategy_type": "中线超跌反转", "price": 10.25, "expected_t3_max_gain_pct": 4.2, "composite_score": 4.2},
        ]
        result = run_1446_ai_interview(["002709", "600865"], ["天赐材料", "百大集团"], rows, use_llm=not args.no_llm)
        print(result["markdown"])
        print(json.dumps({"status": result["status"], "items": result["items"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
