from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import xgboost as xgb

from quant_core.cache_utils import CACHE_DIR
from quant_core.config import (
    BASE_DIR,
    BREAKOUT_MIN_SCORE,
    GLOBAL_DAILY_META_PATH,
    GLOBAL_DAILY_MODEL_PATH,
    GLOBAL_MIN_SCORE,
    MAIN_WAVE_MIN_SCORE,
    MAIN_WAVE_MODEL_PATH,
    PREMIUM_MODEL_PATH,
    REVERSAL_MIN_SCORE,
    REVERSAL_MODEL_PATH,
)
from quant_core.engine.daily_factor_factory import THEME_FACTOR_COLUMNS, generate_daily_factors
from quant_core.engine.predictor import (
    BREAKOUT_STRATEGY_TYPE,
    FEATURE_COLS,
    GLOBAL_MOMENTUM_STRATEGY_TYPE,
    MAIN_WAVE_FEATURE_COLS,
    MAIN_WAVE_STRATEGY_TYPE,
    REVERSAL_FEATURE_COLS,
    REVERSAL_STRATEGY_TYPE,
    _align_global_daily_features,
    apply_production_filters,
)
from quant_core.storage import connect, init_db


DATASET_DIR = BASE_DIR / "data" / "ml_dataset"
SMART_OVERNIGHT_DATASET = DATASET_DIR / "smart_overnight_data.parquet"
REVERSAL_DATASET = DATASET_DIR / "reversal_train_data.parquet"
MAIN_WAVE_DATASET = DATASET_DIR / "main_wave_train_data.parquet"


@dataclass(frozen=True)
class StrategyExplainConfig:
    strategy_type: str
    model_path: Path
    dataset_path: Optional[Path]
    target: str
    target_label: str
    model_type: str
    threshold: float
    feature_columns: tuple[str, ...]
    prediction_label: str
    prediction_unit: str


STRATEGY_REGISTRY: dict[str, StrategyExplainConfig] = {
    BREAKOUT_STRATEGY_TYPE: StrategyExplainConfig(
        strategy_type=BREAKOUT_STRATEGY_TYPE,
        model_path=PREMIUM_MODEL_PATH,
        dataset_path=SMART_OVERNIGHT_DATASET,
        target="next_day_premium",
        target_label="T+1 次日开盘溢价",
        model_type="XGBRegressor",
        threshold=BREAKOUT_MIN_SCORE,
        feature_columns=tuple(FEATURE_COLS),
        prediction_label="预测开盘溢价",
        prediction_unit="pct",
    ),
    REVERSAL_STRATEGY_TYPE: StrategyExplainConfig(
        strategy_type=REVERSAL_STRATEGY_TYPE,
        model_path=REVERSAL_MODEL_PATH,
        dataset_path=REVERSAL_DATASET,
        target="t3_max_gain_pct",
        target_label="T+3 最大涨幅",
        model_type="XGBRegressor",
        threshold=REVERSAL_MIN_SCORE,
        feature_columns=tuple(REVERSAL_FEATURE_COLS),
        prediction_label="预测 T+3 最大涨幅",
        prediction_unit="pct",
    ),
    MAIN_WAVE_STRATEGY_TYPE: StrategyExplainConfig(
        strategy_type=MAIN_WAVE_STRATEGY_TYPE,
        model_path=MAIN_WAVE_MODEL_PATH,
        dataset_path=MAIN_WAVE_DATASET,
        target="t3_max_gain_pct",
        target_label="T+3 最大涨幅",
        model_type="XGBRegressor",
        threshold=MAIN_WAVE_MIN_SCORE,
        feature_columns=tuple(MAIN_WAVE_FEATURE_COLS),
        prediction_label="预测 T+3 最大涨幅",
        prediction_unit="pct",
    ),
    GLOBAL_MOMENTUM_STRATEGY_TYPE: StrategyExplainConfig(
        strategy_type=GLOBAL_MOMENTUM_STRATEGY_TYPE,
        model_path=GLOBAL_DAILY_MODEL_PATH,
        dataset_path=None,
        target="future_3d_max_return_gt_4pct",
        target_label="未来 3 个交易日最高收益率是否超过 4%",
        model_type="XGBClassifier",
        threshold=GLOBAL_MIN_SCORE,
        feature_columns=tuple(),
        prediction_label="三日强势概率",
        prediction_unit="probability",
    ),
}


STRATEGY_ALIASES = {
    "全局日线XGB": GLOBAL_MOMENTUM_STRATEGY_TYPE,
    "全局狙击": GLOBAL_MOMENTUM_STRATEGY_TYPE,
    "顺势主升浪": MAIN_WAVE_STRATEGY_TYPE,
    "尾盘突破": BREAKOUT_STRATEGY_TYPE,
    "中线超跌反转": REVERSAL_STRATEGY_TYPE,
    "右侧主升浪": MAIN_WAVE_STRATEGY_TYPE,
    "全局动量狙击": GLOBAL_MOMENTUM_STRATEGY_TYPE,
}


FEATURE_LABELS = {
    "turn": "换手率",
    "量比": "量比",
    "真实涨幅点数": "当日涨跌幅",
    "实体比例": "K线实体比例",
    "上影线比例": "上影线比例",
    "下影线比例": "下影线比例",
    "日内振幅": "日内振幅",
    "5日累计涨幅": "5日累计涨幅",
    "3日累计涨幅": "3日累计涨幅",
    "5日均线乖离率": "5日均线乖离率",
    "10日均线乖离率": "10日均线乖离率",
    "20日均线乖离率": "20日均线乖离率",
    "3日平均换手率": "3日平均换手率",
    "5日量能堆积": "5日量能堆积",
    "10日量比": "10日量比",
    "3日红盘比例": "3日红盘比例",
    "5日地量标记": "5日地量标记",
    "缩量下跌标记": "缩量下跌标记",
    "振幅换手比": "振幅换手比",
    "缩量大涨标记": "缩量大涨标记",
    "极端下影线标记": "极端下影线标记",
    "近3日断头铡刀标记": "近3日断头铡刀标记",
    "60日高位比例": "60日高位比例",
    "market_up_rate": "全市场上涨占比",
    "market_avg_change": "全市场平均涨跌幅",
    "market_down_count": "全市场下跌家数",
    "body_pct": "实体涨跌幅",
    "upper_shadow_pct": "上影线",
    "lower_shadow_pct": "下影线",
    "amplitude_pct": "日内振幅",
    "change_pct": "当日涨跌幅",
    "return_5d": "5日收益",
    "return_10d": "10日收益",
    "return_20d": "20日收益",
    "return_60d": "60日收益",
    "ma5_bias": "5日均线乖离",
    "ma10_bias": "10日均线乖离",
    "ma20_bias": "20日均线乖离",
    "ma60_bias": "60日均线乖离",
    "ma60_bias_prev": "昨日60日均线乖离",
    "ma20_ma60_spread": "20/60日均线差",
    "pullback_from_60d_high": "距60日高点回撤",
    "contraction_amplitude_5d": "5日平台收敛振幅",
    "prev_volume_ratio_to_5d": "昨日量能/5日均量",
    "breakout_strength": "平台突破强度",
    "volume_burst_ratio": "突破放量倍数",
    "volume_ratio_to_10d": "量能/10日均量",
    "volume_ratio_to_20d": "量能/20日均量",
    "volume_ratio_to_60d": "量能/60日均量",
    "amount_ratio_to_10d": "成交额/10日均额",
    "amount_ratio_to_20d": "成交额/20日均额",
    "drawdown_60d": "60日最大回撤",
    "low_position_60d": "距60日低点位置",
    "min_volume_5d_ratio_to_60d": "5日地量/60日均量",
    "ma_convergence_pct": "均线收敛度",
    "turnover": "换手率",
    "theme_pct_chg_1": "主题1日涨幅",
    "theme_pct_chg_3": "主题3日动量",
    "theme_volatility_5": "主题5日波动",
    "rs_stock_vs_theme": "个股相对主题强度",
    "rs_theme_ema_5": "主题相对强度EMA5",
}


def explain_models() -> dict[str, Any]:
    return {
        "created_at": pd.Timestamp.now().isoformat(),
        "models": [_model_card(config) for config in STRATEGY_REGISTRY.values()],
        "note": "特征重要性来自 XGBoost 模型内部统计；单票解释请使用 /api/explain/pick 获取 pred_contribs 局部贡献。",
    }


def explain_pick(payload: dict[str, Any]) -> dict[str, Any]:
    code = _normalize_code(payload.get("code"))
    date_text = _normalize_date(payload.get("date") or payload.get("selection_date"))
    strategy_type = _normalize_strategy(payload.get("strategy_type"))
    months = _safe_int(payload.get("months"), 12)
    source = str(payload.get("source") or "").strip() or "unknown"
    request_row = payload.get("row") if isinstance(payload.get("row"), dict) else {}

    if not code:
        raise ValueError("缺少有效股票代码")
    if not date_text:
        date_text = _normalize_date(request_row.get("selection_date") or request_row.get("date"))
    if not date_text:
        raise ValueError("缺少有效日期")
    if strategy_type not in STRATEGY_REGISTRY:
        raise ValueError(f"暂不支持策略解释: {strategy_type or '-'}")

    config = _config_with_global_features(STRATEGY_REGISTRY[strategy_type])
    located = _locate_feature_row(code, date_text, strategy_type, months, request_row)
    aligned, row_status = _aligned_feature_frame(config, code, date_text, located.get("row"), request_row)
    prediction, bias, contributions = _local_contributions(config, aligned)
    contribution_rows = _contribution_rows(contributions, aligned.iloc[0], _global_importance_map(config))
    model_card = _model_card(config)
    rank = _same_day_rank(code, date_text, strategy_type, months, located.get("candidate_frame"))

    partial_reasons = []
    partial_reasons.extend(located.get("partial_reasons") or [])
    partial_reasons.extend(row_status.get("partial_reasons") or [])
    missing_features = row_status.get("missing_features") or []
    if missing_features and row_status.get("missing_features_are_partial", True):
        partial_reasons.append(f"输入向量缺失 {len(missing_features)} 个特征，已按当前推理口径补 NaN/0。")

    return {
        "status": "partial" if partial_reasons else "ok",
        "partial": bool(partial_reasons),
        "partial_reasons": partial_reasons,
        "request": {
            "code": code,
            "date": date_text,
            "strategy_type": strategy_type,
            "source": source,
            "months": months,
        },
        "identity": {
            "code": code,
            "name": _safe_text(located.get("name") or request_row.get("name") or code),
            "date": date_text,
            "strategy_type": strategy_type,
        },
        "selection": _selection_summary(config, located.get("row"), request_row, prediction, rank),
        "prediction": {
            "label": config.prediction_label,
            "value": _round_float(prediction, 6),
            "formatted": _format_prediction(config, prediction),
            "bias": _round_float(bias, 6),
            "contribution_unit": "logit" if config.model_type == "XGBClassifier" else "prediction_points",
        },
        "feature_contributions": contribution_rows[:24],
        "positive_contributions": [item for item in contribution_rows if item["contribution"] > 0][:10],
        "negative_contributions": [item for item in contribution_rows if item["contribution"] < 0][:10],
        "feature_values": _feature_value_rows(aligned.iloc[0], contribution_rows, limit=36),
        "model": model_card,
        "data_lineage": _data_lineage(config, located, row_status),
        "notes": [
            "局部贡献来自 XGBoost pred_contribs，是模型内部边际影响，不等于因果证明。",
            "LLM/Ollama 舆情风控不参与结构化模型分数和本解释贡献排序。",
            "解释接口只读计算，不写入 daily_picks，也不会改动 snapshot_price/snapshot_time。",
        ],
    }


def _config_with_global_features(config: StrategyExplainConfig) -> StrategyExplainConfig:
    if config.strategy_type != GLOBAL_MOMENTUM_STRATEGY_TYPE:
        return config
    meta = _read_json(GLOBAL_DAILY_META_PATH)
    feature_columns = tuple(meta.get("feature_columns") or [])
    if not feature_columns:
        raise RuntimeError(f"全局日线模型元数据缺少 feature_columns: {GLOBAL_DAILY_META_PATH}")
    return StrategyExplainConfig(
        strategy_type=config.strategy_type,
        model_path=config.model_path,
        dataset_path=config.dataset_path,
        target=config.target,
        target_label=config.target_label,
        model_type=config.model_type,
        threshold=config.threshold,
        feature_columns=feature_columns,
        prediction_label=config.prediction_label,
        prediction_unit=config.prediction_unit,
    )


def _locate_feature_row(
    code: str,
    date_text: str,
    strategy_type: str,
    months: int,
    request_row: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "row": None,
        "candidate_frame": None,
        "name": request_row.get("name"),
        "partial_reasons": [],
        "source": "request_row",
    }
    candidate_frame = _load_evaluated_candidates(months)
    if candidate_frame is not None and not candidate_frame.empty:
        result["candidate_frame"] = candidate_frame
        match = _matching_candidate_rows(candidate_frame, code, date_text, strategy_type)
        if not match.empty:
            row = match.iloc[0].copy()
            result["row"] = row
            result["name"] = row.get("名称") or row.get("name") or request_row.get("name")
            result["source"] = f"evaluated_candidates_m{months}"
            return result
    db_pick = _load_daily_pick_row(code, date_text, strategy_type)
    if db_pick:
        result["name"] = db_pick.get("name") or result.get("name")
        result["daily_pick"] = db_pick
        result["source"] = "daily_picks"
    result["partial_reasons"].append("未在 evaluated_candidates 缓存中找到精确输入向量，已使用可重建字段解释。")
    return result


def _aligned_feature_frame(
    config: StrategyExplainConfig,
    code: str,
    date_text: str,
    row: Optional[pd.Series],
    request_row: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if config.strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        return _global_aligned_frame(config, code, date_text, request_row)

    partial_reasons: list[str] = []
    if row is not None:
        frame = pd.DataFrame([row])
        source = "evaluated_candidates"
    else:
        frame = pd.DataFrame([_sparse_feature_row_from_payload(request_row)])
        source = "request_row_sparse"
        partial_reasons.append("非全局策略未找到完整缓存行，本次贡献基于前端行内字段稀疏重建。")

    missing = [col for col in config.feature_columns if col not in frame.columns]
    out = frame.copy()
    for col in config.feature_columns:
        if col not in out.columns:
            out[col] = 0.0
    aligned = out[list(config.feature_columns)].apply(pd.to_numeric, errors="coerce")
    aligned = aligned.replace([np.inf, -np.inf], 0.0).fillna(0.0).astype("float32", copy=False)
    return aligned, {
        "source": source,
        "partial_reasons": partial_reasons,
        "missing_features": missing,
        "missing_features_are_partial": source != "evaluated_candidates",
    }


def _global_aligned_frame(
    config: StrategyExplainConfig,
    code: str,
    date_text: str,
    request_row: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw = _load_stock_daily_window(code, date_text)
    partial_reasons: list[str] = []
    if raw.empty:
        sparse = pd.DataFrame([_sparse_feature_row_from_payload(request_row)])
        aligned = _align_global_daily_features(sparse, list(config.feature_columns))
        partial_reasons.append("stock_daily 中未找到该票该日历史窗口，全局动量解释降级为稀疏字段。")
        missing = [col for col in config.feature_columns if col not in sparse.columns]
        return aligned, {
            "source": "request_row_sparse",
            "partial_reasons": partial_reasons,
            "missing_features": missing,
            "missing_features_are_partial": True,
        }

    factors = generate_daily_factors(raw)
    if factors.empty:
        sparse = pd.DataFrame([_sparse_feature_row_from_payload(request_row)])
        aligned = _align_global_daily_features(sparse, list(config.feature_columns))
        partial_reasons.append("Theme Alpha 因子表为空，全局动量解释降级为稀疏字段。")
        missing = [col for col in config.feature_columns if col not in sparse.columns]
        return aligned, {
            "source": "request_row_sparse",
            "partial_reasons": partial_reasons,
            "missing_features": missing,
            "missing_features_are_partial": True,
        }

    factor_dates = _factor_dates(factors)
    match = factors[factor_dates == date_text]
    if match.empty:
        match = factors.tail(1)
        partial_reasons.append("未匹配到精确日期因子行，使用该票窗口内最后一行因子。")
    original_cols = set(match.columns)
    aligned = _align_global_daily_features(match.tail(1), list(config.feature_columns))
    missing = [col for col in config.feature_columns if col not in original_cols]
    return aligned, {
        "source": "stock_daily_theme_factors",
        "partial_reasons": partial_reasons,
        "missing_features": missing,
        "missing_features_are_partial": False,
    }


def _local_contributions(config: StrategyExplainConfig, aligned: pd.DataFrame) -> tuple[float, float, list[dict[str, float]]]:
    model = _load_model(str(config.model_path), config.model_type)
    feature_cols = list(config.feature_columns)
    dmatrix = xgb.DMatrix(aligned, feature_names=feature_cols)
    contrib = model.get_booster().predict(dmatrix, pred_contribs=True)[0]
    bias = float(contrib[-1])
    if config.model_type == "XGBClassifier":
        prediction = float(model.predict_proba(aligned)[:, 1][0])
    else:
        prediction = float(model.predict(aligned)[0])
    rows = [{"feature": feature, "contribution": float(value)} for feature, value in zip(feature_cols, contrib[:-1])]
    return prediction, bias, rows


def _contribution_rows(
    contributions: list[dict[str, float]],
    values: pd.Series,
    importance: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ordered = sorted(contributions, key=lambda item: abs(item["contribution"]), reverse=True)
    for index, item in enumerate(ordered, start=1):
        feature = item["feature"]
        value = values.get(feature)
        contribution = float(item["contribution"])
        rows.append(
            {
                "rank": index,
                "feature": feature,
                "label": _feature_label(feature),
                "group": _feature_group(feature),
                "value": _optional_float(value),
                "formatted_value": _format_feature_value(feature, value),
                "contribution": _round_float(contribution, 6),
                "abs_contribution": _round_float(abs(contribution), 6),
                "direction": "positive" if contribution > 0 else "negative" if contribution < 0 else "neutral",
                "global_importance": _round_float(importance.get(feature), 8),
            }
        )
    return rows


def _feature_value_rows(values: pd.Series, contribution_rows: list[dict[str, Any]], limit: int = 36) -> list[dict[str, Any]]:
    contribution_map = {item["feature"]: item for item in contribution_rows}
    ordered_features = [item["feature"] for item in contribution_rows[:limit]]
    rows = []
    for feature in ordered_features:
        item = contribution_map[feature]
        value = values.get(feature)
        rows.append(
            {
                "feature": feature,
                "label": item["label"],
                "group": item["group"],
                "value": _optional_float(value),
                "formatted": _format_feature_value(feature, value),
                "contribution": item["contribution"],
                "global_importance": item.get("global_importance"),
                "is_missing": bool(pd.isna(value)),
            }
        )
    return rows


def _selection_summary(
    config: StrategyExplainConfig,
    row: Optional[pd.Series],
    request_row: dict[str, Any],
    prediction: float,
    rank: dict[str, Any],
) -> dict[str, Any]:
    source = row if row is not None else request_row
    score_value = _first_float(source, ("score", "selection_score", "排序评分", "sort_score", "综合评分", "composite_score"))
    expected = _first_float(source, ("预期溢价", "expected_premium", "expected_t3_max_gain_pct", "global_probability"))
    threshold = _first_float(source, ("生产门槛", "score_threshold")) or config.threshold
    chain = [
        {"label": "模型输出", "value": _format_prediction(config, prediction)},
        {"label": "策略门槛", "value": _format_threshold(config, threshold)},
        {"label": "同日同策略排名", "value": rank.get("text", "-")},
    ]
    if score_value is not None:
        chain.append({"label": "排序/综合分", "value": f"{score_value:.4f}"})
    if expected is not None:
        chain.append({"label": "缓存预测值", "value": _format_prediction(config, expected)})
    tier = _safe_text(_get_value(source, "selection_tier") or _get_value(source, "tier"))
    if tier:
        chain.append({"label": "分档", "value": tier})
    suggested_position = _first_float(source, ("suggested_position",))
    if suggested_position is not None:
        chain.append({"label": "建议仓位", "value": f"{suggested_position * 100:.0f}%" if suggested_position <= 1 else f"{suggested_position:.2f}%"})
    risk_warning = _safe_text(_get_value(source, "risk_warning"))
    return {
        "score": _round_float(score_value, 6),
        "expected": _round_float(expected, 6),
        "threshold": _round_float(threshold, 6),
        "rank": rank,
        "chain": chain,
        "risk_warning": risk_warning,
    }


def _same_day_rank(
    code: str,
    date_text: str,
    strategy_type: str,
    months: int,
    candidate_frame: Optional[pd.DataFrame],
) -> dict[str, Any]:
    if candidate_frame is None or candidate_frame.empty:
        return {"rank": None, "total": 0, "percentile": None, "text": "无候选池缓存"}
    pool = _matching_candidate_rows(candidate_frame, "", date_text, strategy_type, code_required=False)
    if pool.empty:
        return {"rank": None, "total": 0, "percentile": None, "text": "无同日同策略候选"}
    try:
        filtered = apply_production_filters(pool.copy())
        if not filtered.empty:
            pool = filtered
    except Exception:
        pass
    sort_cols = [col for col in ("排序评分", "预期溢价", "综合评分", "global_probability_pct") if col in pool.columns]
    if not sort_cols:
        return {"rank": None, "total": int(len(pool)), "percentile": None, "text": f"候选 {len(pool)} 只"}
    ranked = pool.sort_values(sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)
    codes = _code_series(ranked)
    positions = np.flatnonzero(codes.eq(code).to_numpy())
    if len(positions) == 0:
        return {"rank": None, "total": int(len(ranked)), "percentile": None, "text": f"候选 {len(ranked)} 只，目标未在过滤池"}
    rank = int(positions[0] + 1)
    total = int(len(ranked))
    percentile = 1.0 - ((rank - 1) / max(1, total))
    return {
        "rank": rank,
        "total": total,
        "percentile": round(percentile, 6),
        "text": f"第 {rank} / {total} 名",
    }


def _model_card(config: StrategyExplainConfig) -> dict[str, Any]:
    config = _config_with_global_features(config)
    dataset = _dataset_summary(config)
    importance = _global_importance_rows(config, topn=20)
    return {
        "strategy_type": config.strategy_type,
        "model_type": config.model_type,
        "model_path": str(config.model_path),
        "model_mtime": _mtime_text(config.model_path),
        "target": config.target,
        "target_label": config.target_label,
        "prediction_label": config.prediction_label,
        "threshold": config.threshold,
        "feature_count": len(config.feature_columns),
        "dataset": dataset,
        "global_importance": importance,
    }


@lru_cache(maxsize=16)
def _dataset_summary_cached(
    strategy_type: str,
    dataset_path_text: str,
    dataset_mtime: float,
    target: str,
) -> dict[str, Any]:
    path = Path(dataset_path_text)
    if not path.exists():
        return {"path": str(path), "exists": False}
    columns = ["date"]
    if target:
        columns.append(target)
    try:
        df = pd.read_parquet(path, columns=[col for col in columns if col])
    except Exception as exc:
        return {"path": str(path), "exists": True, "error": str(exc)}
    rows = int(len(df))
    dates = pd.to_datetime(df.get("date"), errors="coerce")
    target_values = pd.to_numeric(df.get(target), errors="coerce") if target in df.columns else pd.Series(dtype=float)
    return {
        "path": str(path),
        "exists": True,
        "rows": rows,
        "start_date": dates.min().date().isoformat() if dates.notna().any() else None,
        "end_date": dates.max().date().isoformat() if dates.notna().any() else None,
        "target_mean": _round_float(float(target_values.mean()), 6) if len(target_values.dropna()) else None,
        "target_positive_rate_pct": _round_float(float((target_values > 0).mean() * 100), 4) if len(target_values.dropna()) else None,
        "mtime": _mtime_text(path),
    }


def _dataset_summary(config: StrategyExplainConfig) -> dict[str, Any]:
    if config.strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        meta = _read_json(GLOBAL_DAILY_META_PATH)
        return {
            "path": "data/all_kline + Theme Pipeline",
            "exists": True,
            "train_rows": meta.get("train_rows"),
            "test_rows": meta.get("test_rows"),
            "split_date": meta.get("split_date"),
            "created_at": meta.get("created_at"),
            "positive_rows": meta.get("train_positive_rows"),
            "negative_rows": meta.get("train_negative_rows"),
            "purged_target_horizon_days": meta.get("purged_target_horizon_days"),
            "metrics": meta.get("metrics") or {},
        }
    path = config.dataset_path
    if path is None:
        return {"path": "", "exists": False}
    return _dataset_summary_cached(config.strategy_type, str(path), _mtime(path) or 0.0, config.target)


def _global_importance_rows(config: StrategyExplainConfig, topn: int = 20) -> list[dict[str, Any]]:
    importance = _global_importance_map(config)
    rows = [
        {
            "rank": index,
            "feature": feature,
            "label": _feature_label(feature),
            "group": _feature_group(feature),
            "importance": _round_float(value, 8),
        }
        for index, (feature, value) in enumerate(sorted(importance.items(), key=lambda item: item[1], reverse=True)[:topn], start=1)
    ]
    return rows


@lru_cache(maxsize=16)
def _global_importance_map_cached(model_path_text: str, model_type: str, feature_columns: tuple[str, ...], model_mtime: float) -> dict[str, float]:
    if model_type == "XGBClassifier" and Path(model_path_text) == GLOBAL_DAILY_MODEL_PATH:
        meta = _read_json(GLOBAL_DAILY_META_PATH)
        top = meta.get("feature_importance_top15")
        if isinstance(top, dict):
            return {str(key): float(value) for key, value in top.items()}
    model = _load_model(model_path_text, model_type)
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    return {feature: float(value) for feature, value in zip(feature_columns, values)}


def _global_importance_map(config: StrategyExplainConfig) -> dict[str, float]:
    return _global_importance_map_cached(
        str(config.model_path),
        config.model_type,
        tuple(config.feature_columns),
        _mtime(config.model_path) or 0.0,
    )


@lru_cache(maxsize=16)
def _load_model(model_path_text: str, model_type: str) -> Any:
    path = Path(model_path_text)
    if not path.exists():
        raise RuntimeError(f"模型文件不存在: {path}")
    if model_type == "XGBClassifier":
        model = xgb.XGBClassifier()
    else:
        model = xgb.XGBRegressor()
    model.load_model(str(path))
    try:
        model.set_params(n_jobs=1)
    except Exception:
        pass
    return model


def _load_evaluated_candidates(months: int) -> Optional[pd.DataFrame]:
    path = CACHE_DIR / f"evaluated_candidates_m{int(months)}.parquet"
    if not path.exists() and int(months) != 12:
        path = CACHE_DIR / "evaluated_candidates_m12.parquet"
    if not path.exists():
        return None
    return _load_evaluated_candidates_cached(str(path), _mtime(path) or 0.0)


@lru_cache(maxsize=4)
def _load_evaluated_candidates_cached(path_text: str, mtime: float) -> pd.DataFrame:
    return pd.read_parquet(path_text)


def _matching_candidate_rows(
    df: pd.DataFrame,
    code: str,
    date_text: str,
    strategy_type: str,
    code_required: bool = True,
) -> pd.DataFrame:
    if df.empty:
        return df
    dates = pd.to_datetime(df.get("date"), errors="coerce").dt.strftime("%Y-%m-%d")
    strategies = df.get("strategy_type", pd.Series(BREAKOUT_STRATEGY_TYPE, index=df.index)).fillna(BREAKOUT_STRATEGY_TYPE).map(_normalize_strategy)
    mask = dates.eq(date_text) & strategies.eq(strategy_type)
    if code_required:
        mask &= _code_series(df).eq(code)
    return df[mask].copy()


def _load_daily_pick_row(code: str, date_text: str, strategy_type: str) -> Optional[dict[str, Any]]:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM daily_picks
            WHERE code = ? AND selection_date = ? AND COALESCE(strategy_type, '') = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (code, date_text, strategy_type),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    try:
        item["raw"] = json.loads(item.get("raw_json") or "{}")
    except Exception:
        item["raw"] = {}
    item.pop("raw_json", None)
    return item


def _load_stock_daily_window(code: str, date_text: str, lookback_days: int = 180) -> pd.DataFrame:
    end = pd.Timestamp(date_text)
    start = (end - pd.DateOffset(days=lookback_days)).strftime("%Y-%m-%d")
    init_db()
    with connect() as conn:
        raw = pd.read_sql_query(
            """
            SELECT *
            FROM stock_daily
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            conn,
            params=(code, start, date_text),
        )
    if raw.empty:
        return raw
    raw["code"] = code
    raw["symbol"] = code
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    numeric_cols = [col for col in raw.columns if col not in {"date", "code", "symbol", "name"}]
    for col in numeric_cols:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    return raw.dropna(subset=["date", "open", "high", "low", "close"]).copy()


def _sparse_feature_row_from_payload(row: dict[str, Any]) -> dict[str, Any]:
    winner = ((row.get("raw") or {}).get("winner") if isinstance(row.get("raw"), dict) else {}) or {}
    tech = winner.get("tech_features") or row.get("tech_features") or {}
    trend = winner.get("trend_features") or row.get("trend_features") or {}
    market = winner.get("market_context") or row.get("market_context") or {}
    return {
        "turn": _first_float(row, ("turnover", "turn")) or _first_float(winner, ("turnover", "turn")) or 0.0,
        "量比": _first_float(row, ("volume_ratio", "snapshot_vol_ratio")) or _first_float(winner, ("volume_ratio",)) or 0.0,
        "真实涨幅点数": _first_float(row, ("change", "selection_change", "pct_chg")) or _first_float(winner, ("change",)) or 0.0,
        "实体比例": _first_float(tech, ("body_ratio", "body_pct")) or 0.0,
        "上影线比例": _first_float(tech, ("upper_shadow", "upper_shadow_pct")) or 0.0,
        "下影线比例": _first_float(tech, ("lower_shadow", "lower_shadow_pct")) or 0.0,
        "日内振幅": _first_float(tech, ("amplitude", "amplitude_pct")) or 0.0,
        "5日累计涨幅": _first_float(trend, ("return_5d",)) or 0.0,
        "3日累计涨幅": _first_float(trend, ("return_3d",)) or 0.0,
        "5日均线乖离率": _first_float(trend, ("bias_5d",)) or 0.0,
        "10日均线乖离率": _first_float(trend, ("bias_10d",)) or 0.0,
        "20日均线乖离率": _first_float(trend, ("bias_20d",)) or 0.0,
        "3日平均换手率": _first_float(trend, ("avg_turnover_3d",)) or 0.0,
        "5日量能堆积": _first_float(trend, ("volume_stack_5d",)) or 0.0,
        "10日量比": _first_float(trend, ("volume_ratio_10d",)) or 0.0,
        "3日红盘比例": _first_float(trend, ("red_ratio_3d",)) or 0.0,
        "5日地量标记": 1.0 if trend.get("is_5d_low_volume") else 0.0,
        "缩量下跌标记": 1.0 if trend.get("is_shrink_down") else 0.0,
        "振幅换手比": _first_float(trend, ("amplitude_turnover_ratio",)) or 0.0,
        "缩量大涨标记": 1.0 if trend.get("is_low_volume_rally") else 0.0,
        "极端下影线标记": 1.0 if trend.get("is_extreme_lower_shadow") else 0.0,
        "近3日断头铡刀标记": 1.0 if trend.get("is_recent_guillotine") else 0.0,
        "60日高位比例": _first_float(trend, ("high_position_60d",)) or 0.0,
        "market_up_rate": _first_float(market, ("up_rate",)) or 0.0,
        "market_avg_change": _first_float(market, ("avg_change",)) or 0.0,
        "market_down_count": _first_float(market, ("down_count",)) or 0.0,
        "body_pct": _first_float(tech, ("body_ratio", "body_pct")) or 0.0,
        "upper_shadow_pct": _first_float(tech, ("upper_shadow", "upper_shadow_pct")) or 0.0,
        "lower_shadow_pct": _first_float(tech, ("lower_shadow", "lower_shadow_pct")) or 0.0,
        "amplitude_pct": _first_float(tech, ("amplitude", "amplitude_pct")) or 0.0,
        "change_pct": _first_float(row, ("change", "selection_change", "pct_chg")) or _first_float(winner, ("change",)) or 0.0,
        "return_5d": _first_float(trend, ("return_5d",)) or 0.0,
        "return_10d": _first_float(trend, ("return_10d",)) or 0.0,
        "return_20d": _first_float(trend, ("return_20d",)) or 0.0,
        "return_60d": _first_float(trend, ("return_60d",)) or 0.0,
        "ma5_bias": _first_float(trend, ("bias_5d",)) or 0.0,
        "ma10_bias": _first_float(trend, ("bias_10d",)) or 0.0,
        "ma20_bias": _first_float(trend, ("bias_20d",)) or 0.0,
        "ma60_bias": _first_float(trend, ("ma60_bias",)) or 0.0,
        "ma60_bias_prev": _first_float(trend, ("ma60_bias_prev",)) or 0.0,
        "drawdown_60d": _first_float(trend, ("reversal_drawdown_60d", "drawdown_60d")) or 0.0,
        "min_volume_5d_ratio_to_60d": _first_float(trend, ("reversal_min_volume_ratio", "min_volume_5d_ratio_to_60d")) or 0.0,
        "volume_ratio_to_10d": _first_float(trend, ("reversal_volume_ratio_10d", "volume_ratio_to_10d")) or 0.0,
        "ma_convergence_pct": _first_float(trend, ("reversal_ma_convergence", "ma_convergence_pct")) or 0.0,
        "turnover": _first_float(row, ("turnover", "turn")) or _first_float(winner, ("turnover", "turn")) or 0.0,
        "theme_pct_chg_3": _first_float(row, ("theme_momentum_3d", "theme_momentum", "theme_pct_chg_3")) or _first_float(winner, ("theme_momentum_3d", "theme_momentum", "theme_pct_chg_3")) or np.nan,
    }


def _data_lineage(config: StrategyExplainConfig, located: dict[str, Any], row_status: dict[str, Any]) -> list[dict[str, str]]:
    lineage = [
        {"label": "模型文件", "value": str(config.model_path)},
        {"label": "模型更新时间", "value": _mtime_text(config.model_path) or "-"},
        {"label": "输入来源", "value": str(located.get("source") or row_status.get("source") or "-")},
    ]
    dataset = _dataset_summary(config)
    if dataset.get("path"):
        lineage.append({"label": "训练数据", "value": str(dataset.get("path"))})
    if dataset.get("rows"):
        lineage.append({"label": "训练样本", "value": f"{dataset.get('rows')} 行"})
    if dataset.get("train_rows"):
        lineage.append({"label": "训练/测试样本", "value": f"{dataset.get('train_rows')} / {dataset.get('test_rows')}"})
    return lineage


def _feature_label(feature: str) -> str:
    return FEATURE_LABELS.get(feature, feature)


def _feature_group(feature: str) -> str:
    if feature.startswith("theme_") or feature.startswith("rs_"):
        return "主题强度"
    if feature.startswith("market_"):
        return "大盘环境"
    if "volume" in feature or "amount" in feature or feature in {"量比", "10日量比", "5日量能堆积", "turn", "turnover", "3日平均换手率"}:
        return "量能流动性"
    if "bias" in feature or feature.startswith("ma") or feature.startswith("ema"):
        return "均线位置"
    if "return" in feature or "momentum" in feature or "涨幅" in feature or feature.startswith("ret_"):
        return "价格动量"
    if "atr" in feature or "volatility" in feature or "range" in feature or "振幅" in feature or "amplitude" in feature:
        return "波动率"
    if "shadow" in feature or "body" in feature or "实体" in feature or "影线" in feature:
        return "K线结构"
    return "技术因子"


def _format_feature_value(feature: str, value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "-"
    if feature.endswith("标记") or feature.startswith("is_"):
        return "是" if parsed >= 0.5 else "否"
    if any(key in feature for key in ("pct", "return", "涨幅", "乖离", "振幅", "回撤", "premium", "change")):
        return f"{parsed:.4f}%"
    return f"{parsed:.4f}"


def _format_prediction(config: StrategyExplainConfig, value: Optional[float]) -> str:
    if value is None:
        return "-"
    if config.prediction_unit == "probability":
        return f"{value * 100:.2f}%" if value <= 1 else f"{value:.2f}%"
    return f"{value:.4f}%"


def _format_threshold(config: StrategyExplainConfig, value: float) -> str:
    if config.prediction_unit == "probability":
        return f"{value * 100:.2f}%"
    if config.strategy_type == BREAKOUT_STRATEGY_TYPE:
        return f"{value:.2f} 分"
    return f"{value:.2f}%"


def _factor_dates(factors: pd.DataFrame) -> pd.Series:
    if "datetime" in factors.columns:
        return pd.to_datetime(factors["datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
    return pd.to_datetime(factors.get("date"), errors="coerce").dt.strftime("%Y-%m-%d")


def _code_series(df: pd.DataFrame) -> pd.Series:
    source = df["纯代码"] if "纯代码" in df.columns else df["code"] if "code" in df.columns else pd.Series("", index=df.index)
    return source.astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)


def _normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if len(digits) >= 1 else ""


def _normalize_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    parsed = pd.to_datetime(raw, errors="coerce")
    return parsed.strftime("%Y-%m-%d") if pd.notna(parsed) else ""


def _normalize_strategy(value: Any) -> str:
    raw = str(value or "").strip()
    return STRATEGY_ALIASES.get(raw, raw or BREAKOUT_STRATEGY_TYPE)


def _first_float(source: Any, keys: tuple[str, ...]) -> Optional[float]:
    for key in keys:
        value = _get_value(source, key)
        parsed = _optional_float(value)
        if parsed is not None:
            return parsed
    return None


def _get_value(source: Any, key: str) -> Any:
    if isinstance(source, pd.Series):
        return source.get(key)
    if isinstance(source, dict):
        if key in source:
            return source.get(key)
        raw = source.get("raw") if isinstance(source.get("raw"), dict) else {}
        winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
        if key in winner:
            return winner.get(key)
    return None


def _optional_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _round_float(value: Any, digits: int = 4) -> Optional[float]:
    parsed = _optional_float(value)
    return round(parsed, digits) if parsed is not None else None


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"none", "nan", "null"} else text


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _mtime(path: Path) -> Optional[float]:
    try:
        return round(path.stat().st_mtime, 3)
    except OSError:
        return None


def _mtime_text(path: Path) -> Optional[str]:
    try:
        return pd.Timestamp.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except OSError:
        return None
