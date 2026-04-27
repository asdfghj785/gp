from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import roc_auc_score

from .config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE, OLLAMA_API, OLLAMA_MODEL
from quant_core.strategies.labs.strategy_lab import prepare_evaluated_candidates


REASON_FEATURES = [
    "涨跌幅",
    "换手率",
    "量比",
    "实体比例",
    "上影线比例",
    "下影线比例",
    "日内振幅",
    "最新价",
    "AI胜率",
    "预期溢价",
    "风险评分",
    "流动性评分",
    "综合评分",
    "5日累计涨幅",
    "3日累计涨幅",
    "5日均线乖离率",
    "20日均线乖离率",
    "3日平均换手率",
    "5日量能堆积",
    "10日量比",
    "3日红盘比例",
    "5日地量标记",
    "缩量下跌标记",
    "振幅换手比",
    "缩量大涨标记",
    "极端下影线标记",
    "近3日断头铡刀标记",
    "60日高位比例",
    "market_up_rate",
    "market_avg_change",
    "market_down_count",
]


def analyze_next_day_up_reasons(months: int = 12, refresh: bool = False) -> dict[str, Any]:
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    df = prepared["evaluated"]
    if df.empty:
        return _empty_result(months, prepared["model_status"])

    df = _add_market_context(df)
    df = df[np.isfinite(df["open_premium"])].copy()
    if "strategy_type" not in df.columns:
        df["strategy_type"] = "尾盘突破"
    df["next_day_up"] = df["open_premium"] > 0
    up = df[df["next_day_up"]].copy()
    down = df[~df["next_day_up"]].copy()
    baseline_up_rate = float(df["next_day_up"].mean() * 100)

    factor_rows = _factor_lift_rows(df, baseline_up_rate)
    model_report = _train_reason_model(df)
    llm_summary = _llm_summarize(months, baseline_up_rate, factor_rows[:8], model_report)

    summary = {
        "months": months,
        "start_date": prepared["start_date"],
        "end_date": prepared["end_date"],
        "candidate_rows": int(len(df)),
        "up_count": int(len(up)),
        "down_count": int(len(down)),
        "up_rate": round(baseline_up_rate, 4),
        "avg_up_premium": round(float(up["open_premium"].mean()), 4) if len(up) else 0.0,
        "avg_down_premium": round(float(down["open_premium"].mean()), 4) if len(down) else 0.0,
        "model_status": prepared["model_status"],
        "sentiment_status": "历史库未保存逐日舆情快照，历史归因不能把舆情当作可验证因子；舆情仅用于当天候选的实时风控。",
        "method": "先统计所有候选的次日开盘上涨标签，再用因子提升度和 XGBoost 解释上涨概率。",
    }
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "factor_lifts": factor_rows,
        "model_report": model_report,
        "llm_summary": llm_summary,
        "up_examples": _example_rows(up, ascending=False),
        "down_examples": _example_rows(down, ascending=True),
    }


def _add_market_context(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.drop(columns=["market_up_rate", "market_down_count", "market_avg_change", "market_avg_turnover"], errors="ignore")
    daily = out.groupby("date").agg(
        market_up_rate=("涨跌幅", lambda values: float((values > 0).mean() * 100)),
        market_down_count=("涨跌幅", lambda values: int((values < 0).sum())),
        market_avg_change=("涨跌幅", "mean"),
        market_avg_turnover=("换手率", "mean"),
    )
    out = out.join(daily, on="date")
    return out


def _factor_lift_rows(df: pd.DataFrame, baseline_up_rate: float) -> list[dict[str, Any]]:
    conditions = [
        ("温和上涨 0%-3%", "技术", lambda x: x["涨跌幅"].between(0, 3), "上涨不极端，获利盘压力较轻。"),
        ("上涨 3%-7%", "技术", lambda x: x["涨跌幅"].between(3, 7), "趋势明确但未进入过热区。"),
        ("大跌反抽 <-5%", "技术", lambda x: x["涨跌幅"] < -5, "尾盘弱势反抽，次日承接不确定。"),
        ("涨幅过热 >=7%", "技术", lambda x: x["涨跌幅"] >= 7, "短线获利盘重，容易次日低开消化。"),
        ("缩量/低量比 <0.8", "技术", lambda x: x["量比"] < 0.8, "量能不足，延续性需要警惕。"),
        ("放量 1.2-3", "技术", lambda x: x["量比"].between(1.2, 3), "有资金参与但未极端拥挤。"),
        ("异常放量 >=3", "技术", lambda x: x["量比"] >= 3, "资金分歧大，容易冲高回落。"),
        ("换手适中 2%-10%", "技术", lambda x: x["换手率"].between(2, 10), "承接活跃且不过热。"),
        ("换手过热 >=15%", "技术", lambda x: x["换手率"] >= 15, "短线筹码交换剧烈。"),
        ("实体阳线 >1%", "技术", lambda x: x["实体比例"] > 1, "收盘强于开盘，尾盘结构偏强。"),
        ("长上影 >=2%", "技术", lambda x: x["上影线比例"] >= 2, "上方抛压明显。"),
        ("下影承接 >=1%", "技术", lambda x: x["下影线比例"] >= 1, "盘中下探后有资金承接。"),
        ("振幅适中 3%-8%", "技术", lambda x: x["日内振幅"].between(3, 8), "有波动但未严重分歧。"),
        ("振幅过大 >=8%", "技术", lambda x: x["日内振幅"] >= 8, "日内分歧过大。"),
        ("5日趋势向上 >3%", "趋势", lambda x: x["5日累计涨幅"] > 3, "短线趋势处于上行段。"),
        ("3日动量 >2%", "趋势", lambda x: x["3日累计涨幅"] > 2, "短线动量连续。"),
        ("贴近5日线 -3%到5%", "趋势", lambda x: x["5日均线乖离率"].between(-3, 5), "短线位置没有明显脱离5日线。"),
        ("贴近20日线 -5%到5%", "趋势", lambda x: x["20日均线乖离率"].between(-5, 5), "价格没有明显脱离中期均线。"),
        ("5日量能堆积 >=1.5", "主力行为", lambda x: x["5日量能堆积"] >= 1.5, "成交量相对5日均量明显放大。"),
        ("10日放量 >=1.2", "主力行为", lambda x: x["10日量比"] >= 1.2, "成交量相对10日均量放大。"),
        ("高位爆量", "负面清单", lambda x: (x["60日高位比例"] >= 97) & (x["5日量能堆积"] > 3), "接近60日高位且爆量，容易变成兑现盘。"),
        ("近3日红盘 >=67%", "主力行为", lambda x: x["3日红盘比例"] >= 67, "连续红盘代表资金持续参与。"),
        ("5日地量", "主力行为", lambda x: x["5日地量标记"] >= 0.5, "可能对应缩量洗盘或流动性不足，需要结合趋势判断。"),
        ("缩量下跌", "主力行为", lambda x: x["缩量下跌标记"] >= 0.5, "下跌时成交萎缩，抛压可能较轻。"),
        ("振幅换手比 >3", "诱多风险", lambda x: x["振幅换手比"] > 3, "振幅大但换手消耗不足，价格可能被轻量资金拉动。"),
        ("缩量大涨", "诱多风险", lambda x: x["缩量大涨标记"] >= 0.5, "涨幅较大但成交量低于5日均量，上涨承接可能不足。"),
        ("极端下影线", "诱多风险", lambda x: x["极端下影线标记"] >= 0.5, "深水拉起后留下极端下影线，需要警惕尾盘偷袭。"),
        ("近3日断头铡刀", "风险", lambda x: x["近3日断头铡刀标记"] >= 0.5, "过去3日出现过-7%以上下杀，短线资金记忆偏负面。"),
        ("市场红盘率 >=55%", "市场", lambda x: x["market_up_rate"] >= 55, "市场环境偏暖。"),
        ("市场红盘率 <40%", "市场", lambda x: x["market_up_rate"] < 40, "市场环境偏弱。"),
        ("下跌家数 >=3500", "市场", lambda x: x["market_down_count"] >= 3500, "系统性风险高，适合作为空仓门控。"),
        ("模型预期溢价 >0", "模型", lambda x: x["预期溢价"] > 0, "结构化模型认为次日有正溢价。"),
        (f"综合评分达到策略门槛", "模型", lambda x: x["综合评分"] >= x["strategy_type"].map({"首阴低吸": DIPBUY_MIN_SCORE}).fillna(BREAKOUT_MIN_SCORE), "综合信号强度达到对应策略门槛。"),
    ]
    rows = []
    total = len(df)
    for name, category, predicate, explanation in conditions:
        subset = df[predicate(df)].copy()
        if subset.empty:
            continue
        up_rate = float(subset["next_day_up"].mean() * 100)
        rows.append(
            {
                "factor": name,
                "category": category,
                "sample_count": int(len(subset)),
                "sample_ratio": round(float(len(subset) / total * 100), 4) if total else 0.0,
                "up_rate": round(up_rate, 4),
                "lift": round(up_rate - baseline_up_rate, 4),
                "avg_open_premium": round(float(subset["open_premium"].mean()), 4),
                "explanation": explanation,
            }
        )
    rows.sort(key=lambda row: (row["lift"], row["sample_count"]), reverse=True)
    return rows


def _train_reason_model(df: pd.DataFrame) -> dict[str, Any]:
    clean = df.dropna(subset=REASON_FEATURES + ["next_day_up"]).copy()
    if len(clean) < 1000:
        return {"status": "样本不足", "auc": 0.0, "feature_importance": []}
    clean = clean.sort_values("date").reset_index(drop=True)
    split = int(len(clean) * 0.8)
    train = clean.iloc[:split]
    test = clean.iloc[split:]
    try:
        import xgboost as xgb

        model = xgb.XGBClassifier(
            n_estimators=180,
            learning_rate=0.04,
            max_depth=4,
            min_child_weight=10,
            subsample=0.82,
            colsample_bytree=0.82,
            eval_metric="logloss",
            random_state=42,
            n_jobs=4,
        )
        model.fit(train[REASON_FEATURES], train["next_day_up"].astype(int))
        probabilities = model.predict_proba(test[REASON_FEATURES])[:, 1]
        auc = float(roc_auc_score(test["next_day_up"].astype(int), probabilities))
        importances = [
            {"feature": feature, "importance": round(float(value) * 100, 4)}
            for feature, value in zip(REASON_FEATURES, model.feature_importances_)
        ]
        importances.sort(key=lambda item: item["importance"], reverse=True)
        top_decile = test.assign(probability=probabilities).sort_values("probability", ascending=False).head(max(1, len(test) // 10))
        return {
            "status": "ready",
            "auc": round(auc, 4),
            "test_rows": int(len(test)),
            "top_decile_up_rate": round(float(top_decile["next_day_up"].mean() * 100), 4),
            "top_decile_avg_premium": round(float(top_decile["open_premium"].mean()), 4),
            "feature_importance": importances[:12],
        }
    except Exception as exc:
        return {"status": f"模型训练失败: {exc}", "auc": 0.0, "feature_importance": []}


def _llm_summarize(months: int, baseline_up_rate: float, factors: list[dict[str, Any]], model_report: dict[str, Any]) -> dict[str, Any]:
    prompt = f"""
你是一个A股量化研究员。请根据下面的历史统计结果，判断次日开盘上涨更可能由技术数据、市场环境还是舆论情绪驱动。
注意：历史库没有逐日舆情快照，所以不能臆造舆情结论；只能说明舆情目前不可验证，并建议如何采集。

统计周期：{months}个月
基准次日上涨率：{baseline_up_rate:.2f}%
因子提升度Top：{json.dumps(factors, ensure_ascii=False)}
模型报告：{json.dumps(model_report, ensure_ascii=False)}

输出严格JSON：
{{
  "primary_driver": "技术数据/市场环境/舆论不可验证",
  "confidence": 0到100,
  "conclusion": "一句话结论",
  "technical_findings": ["要点1", "要点2", "要点3"],
  "sentiment_findings": ["要点1", "要点2"],
  "next_algorithm": ["建议1", "建议2", "建议3"]
}}
"""
    try:
        response = requests.post(
            OLLAMA_API,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
            timeout=60,
        )
        response.raise_for_status()
        parsed = json.loads(response.json().get("response", "{}"))
        if isinstance(parsed, dict):
            return parsed
    except Exception as exc:
        return {
            "primary_driver": "技术数据",
            "confidence": 60,
            "conclusion": f"本地大模型总结不可用，已退回规则结论：{exc}",
            "technical_findings": [item["factor"] for item in factors[:3]],
            "sentiment_findings": ["历史库没有逐日舆情快照，不能验证舆情因果。"],
            "next_algorithm": ["先保存每日候选股的公告/新闻/股吧情绪快照", "再把情绪标签加入训练集", "用技术模型排序、舆情模型做风控门控"],
        }
    return {
        "primary_driver": "技术数据",
        "confidence": 60,
        "conclusion": "模型返回格式异常，已退回规则结论。",
        "technical_findings": [item["factor"] for item in factors[:3]],
        "sentiment_findings": ["历史库没有逐日舆情快照，不能验证舆情因果。"],
        "next_algorithm": ["补齐舆情快照后再训练混合模型"],
    }


def _example_rows(df: pd.DataFrame, ascending: bool) -> list[dict[str, Any]]:
    rows = []
    selected = df.sort_values("open_premium", ascending=ascending).head(12)
    for _, row in selected.iterrows():
        rows.append(
            {
                "date": str(row["date"]),
                "code": str(row["纯代码"]),
                "name": str(row["名称"]),
                "open_premium": round(float(row["open_premium"]), 4),
                "change": round(float(row["涨跌幅"]), 4),
                "turnover": round(float(row["换手率"]), 4),
                "volume_ratio": round(float(row["量比"]), 4),
                "body_ratio": round(float(row["实体比例"]), 4),
                "upper_shadow": round(float(row["上影线比例"]), 4),
                "amplitude": round(float(row["日内振幅"]), 4),
                "market_up_rate": round(float(row["market_up_rate"]), 4),
            }
        )
    return rows


def _empty_result(months: int, reason: str) -> dict[str, Any]:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "months": months,
            "start_date": None,
            "end_date": None,
            "candidate_rows": 0,
            "up_count": 0,
            "down_count": 0,
            "up_rate": 0.0,
            "model_status": reason,
            "sentiment_status": "无可用数据",
            "method": "无可用数据",
        },
        "factor_lifts": [],
        "model_report": {"status": reason, "feature_importance": []},
        "llm_summary": {},
        "up_examples": [],
        "down_examples": [],
    }
