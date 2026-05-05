from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - environment guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.config import BASE_DIR, INTRADAY_EXIT_META_PATH, INTRADAY_EXIT_MODEL_PATH, MIN_KLINE_DIR
from quant_core.engine.backtest import top_pick_open_backtest
from quant_core.storage import connect, init_db
from quant_core.strategies.labs.strategy_lab import prepare_evaluated_candidates


SWING_STRATEGY_TYPES = {"全局动量狙击", "右侧主升浪", "中线超跌反转"}
REPORT_NAMESPACE = "intraday_exit_backtest"
REPORT_PATH_TEMPLATE = BASE_DIR / "data" / "strategy_cache" / f"{REPORT_NAMESPACE}_m{{months}}.json"
EVALUATED_CANDIDATE_TEMPLATE = BASE_DIR / "data" / "strategy_cache" / "evaluated_candidates_m{months}.parquet"

LABEL_HOLD = 0
LABEL_TAKE_PROFIT = 1
LABEL_STOP_LOSS = 2
LABEL_NAMES = {
    LABEL_HOLD: "hold",
    LABEL_TAKE_PROFIT: "take_profit",
    LABEL_STOP_LOSS: "stop_loss",
}

STRATEGY_CODE = {
    "全局动量狙击": 3.0,
    "右侧主升浪": 2.0,
    "中线超跌反转": 1.0,
}
MARKET_GATE_CODE = {
    "晴天": 0.0,
    "震荡": 1.0,
    "阴天": 2.0,
    "缩量下跌": 3.0,
    "雷暴": 4.0,
}

MIN_FULL_DAY_BARS = 45
DEFAULT_MAX_TRAIN_TRADES = int(os.getenv("QUANT_INTRADAY_EXIT_MAX_TRAIN_TRADES", "4500"))
DEFAULT_MAX_PER_DATE_STRATEGY = int(os.getenv("QUANT_INTRADAY_EXIT_MAX_PER_DATE_STRATEGY", "12"))
DEFAULT_HOLD_SAMPLE_STRIDE = int(os.getenv("QUANT_INTRADAY_EXIT_HOLD_SAMPLE_STRIDE", "12"))
TAKE_PROFIT_THRESHOLD = float(os.getenv("QUANT_INTRADAY_EXIT_TAKE_THRESHOLD", "0.52"))
STOP_LOSS_THRESHOLD = float(os.getenv("QUANT_INTRADAY_EXIT_STOP_THRESHOLD", "0.52"))
MIN_TAKE_PROFIT_PCT = float(os.getenv("QUANT_INTRADAY_EXIT_MIN_TAKE_PCT", "0.80"))
MIN_STOP_LOSS_PCT = float(os.getenv("QUANT_INTRADAY_EXIT_MIN_STOP_PCT", "-2.50"))

FEATURE_COLUMNS = [
    "current_gain_pct",
    "bar_high_gain_pct",
    "bar_low_gain_pct",
    "running_high_gain_pct",
    "running_low_gain_pct",
    "drawdown_from_high_pct",
    "trading_day_index",
    "bar_index",
    "bar_index_day",
    "minutes_from_open",
    "is_morning",
    "is_afternoon",
    "is_last_hour",
    "ret_1",
    "ret_3",
    "ret_6",
    "ret_12",
    "volume_ratio_3",
    "volume_ratio_6",
    "volume_ratio_12",
    "amount_ratio_6",
    "amount_ratio_12",
    "vwap_dev_pct",
    "range_pct",
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "close_location",
    "rolling_volatility_6",
    "rolling_volatility_12",
    "ma_slope_3",
    "ma_slope_6",
    "ma_slope_12",
    "close_ma_bias_6",
    "close_ma_bias_12",
    "consecutive_up",
    "consecutive_down",
    "strategy_code",
    "is_global_momentum",
    "is_main_wave",
    "is_reversal",
    "selection_tier_code",
    "selection_score",
    "sort_score",
    "expected_t3_max_gain_pct",
    "theme_momentum_3d",
    "market_gate_code",
]

_CANDIDATE_COLUMNS = [
    "date",
    "纯代码",
    "code",
    "name",
    "名称",
    "strategy_type",
    "综合评分",
    "排序评分",
    "预期溢价",
    "生产门槛",
    "market_gate_mode",
    "theme_momentum_3d",
    "theme_pct_chg_3",
    "t3_max_gain_pct",
    "最新价",
    "close",
]


def run_intraday_exit_backtest(
    months: int = 12,
    refresh: bool = False,
    retrain: bool = False,
    max_train_trades: int = DEFAULT_MAX_TRAIN_TRADES,
) -> dict[str, Any]:
    """Train/replay the 5m T+3 intraday exit model without mutating daily_picks."""
    report_path = REPORT_PATH_TEMPLATE.with_name(REPORT_PATH_TEMPLATE.name.format(months=int(months)))
    if not refresh and not retrain and report_path.exists() and INTRADAY_EXIT_MODEL_PATH.exists() and INTRADAY_EXIT_META_PATH.exists():
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload["cache"] = {"hit": True, "namespace": REPORT_NAMESPACE}
                return payload
        except Exception:
            pass

    started = time.perf_counter()
    train_result = train_intraday_exit_model(
        months=months,
        refresh_candidates=refresh,
        retrain=retrain,
        max_train_trades=max_train_trades,
    )
    replay = replay_top_pick_intraday_exit(
        months=months,
        model=train_result["model"],
        feature_columns=train_result["feature_columns"],
        refresh_backtest=False,
    )
    breakout_sweep = _breakout_threshold_sweep_summary(months=months, refresh=False)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "months": int(months),
        "status": "ready",
        "model_path": str(INTRADAY_EXIT_MODEL_PATH),
        "meta_path": str(INTRADAY_EXIT_META_PATH),
        "summary": {
            **replay["summary"],
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        },
        "model": train_result["metadata"],
        "coverage": train_result["coverage"],
        "strategy_performance": replay["strategy_performance"],
        "rows": replay["rows"],
        "breakout_threshold_sweep": breakout_sweep,
        "rule": (
            "T+3 波段策略 5m 盘中卖点研究：特征只使用当前及历史 5m bar；"
            "回放触发后以下一根 5m open 作为执行价，最后一根使用 close；"
            "本报告不写回 daily_picks，也不修改 snapshot_price/snapshot_time。"
        ),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False), encoding="utf-8")
    return payload


def train_intraday_exit_model(
    months: int = 12,
    refresh_candidates: bool = False,
    retrain: bool = False,
    max_train_trades: int = DEFAULT_MAX_TRAIN_TRADES,
) -> dict[str, Any]:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}")
    if not retrain and INTRADAY_EXIT_MODEL_PATH.exists() and INTRADAY_EXIT_META_PATH.exists():
        model, meta = load_intraday_exit_model()
        return {
            "model": model,
            "feature_columns": list(meta.get("feature_columns") or FEATURE_COLUMNS),
            "metadata": meta,
            "coverage": meta.get("coverage", {}),
        }

    dataset, coverage = build_intraday_exit_dataset(
        months=months,
        refresh_candidates=refresh_candidates,
        max_train_trades=max_train_trades,
    )
    if dataset.empty:
        raise RuntimeError("没有可训练的 5m 盘中卖点样本：本地分钟线完整覆盖不足")

    label_counts = dataset["label"].value_counts().to_dict()
    missing_labels = [name for value, name in LABEL_NAMES.items() if int(label_counts.get(value, 0)) <= 0]
    if missing_labels:
        raise RuntimeError(f"训练集 label 缺少类别：{missing_labels}，无法训练三分类卖点模型")

    frame = dataset.sort_values(["selection_date", "code", "datetime"]).reset_index(drop=True)
    dates = sorted(frame["selection_date"].astype(str).unique().tolist())
    split_index = max(1, min(len(dates) - 1, int(len(dates) * 0.8)))
    split_date = dates[split_index - 1]
    train_mask = frame["selection_date"].astype(str) <= split_date
    test_mask = ~train_mask
    if not test_mask.any():
        raise RuntimeError("训练/测试时间切分失败：测试集为空")

    X_train = _align_features(frame.loc[train_mask], FEATURE_COLUMNS)
    y_train = frame.loc[train_mask, "label"].astype(int)
    X_test = _align_features(frame.loc[test_mask], FEATURE_COLUMNS)
    y_test = frame.loc[test_mask, "label"].astype(int)

    model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        max_depth=4,
        learning_rate=0.06,
        n_estimators=220,
        subsample=0.88,
        colsample_bytree=0.88,
        min_child_weight=3,
        reg_lambda=1.8,
        eval_metric="mlogloss",
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    sample_weight = y_train.map({LABEL_HOLD: 1.0, LABEL_TAKE_PROFIT: 2.4, LABEL_STOP_LOSS: 2.8}).astype(float)
    model.fit(X_train, y_train, sample_weight=sample_weight, eval_set=[(X_test, y_test)], verbose=False)

    probabilities = model.predict_proba(X_test)
    predictions = probabilities.argmax(axis=1)
    metrics = _classification_metrics(y_test.to_numpy(), predictions)
    importance = _top_feature_importance(model, FEATURE_COLUMNS)
    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "model_path": str(INTRADAY_EXIT_MODEL_PATH),
        "meta_path": str(INTRADAY_EXIT_META_PATH),
        "feature_columns": FEATURE_COLUMNS,
        "label_names": LABEL_NAMES,
        "months": int(months),
        "split_date": split_date,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "label_counts": {LABEL_NAMES[int(key)]: int(value) for key, value in sorted(label_counts.items())},
        "metrics": metrics,
        "feature_importance_top20": importance,
        "coverage": coverage,
        "sampling": {
            "max_train_trades": int(max_train_trades),
            "max_per_date_strategy": DEFAULT_MAX_PER_DATE_STRATEGY,
            "hold_sample_stride": DEFAULT_HOLD_SAMPLE_STRIDE,
        },
    }
    INTRADAY_EXIT_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(INTRADAY_EXIT_MODEL_PATH))
    INTRADAY_EXIT_META_PATH.write_text(json.dumps(_json_safe(metadata), ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "metadata": metadata,
        "coverage": coverage,
    }


def build_intraday_exit_dataset(
    months: int = 12,
    refresh_candidates: bool = False,
    max_train_trades: int = DEFAULT_MAX_TRAIN_TRADES,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    candidates = _load_evaluated_candidates(months, refresh_candidates=refresh_candidates)
    candidates = _normalise_candidates(candidates)
    swing = candidates[candidates["strategy_type"].isin(SWING_STRATEGY_TYPES)].copy()
    trading_dates = _trading_dates_from_db()
    next3 = _next3_trading_dates(trading_dates)
    required_dates = {day for date_value in swing["date"].astype(str).unique() for day in next3.get(date_value, [])}
    coverage_index = _minute_coverage_index(required_dates)

    swing["future_days"] = swing["date"].astype(str).map(lambda day: next3.get(day, []))
    swing["minute_coverage_days"] = [
        _covered_future_day_count(code, days, coverage_index)
        for code, days in zip(swing["code"], swing["future_days"])
    ]
    full_window = swing[swing["minute_coverage_days"] == 3].copy()
    selected = _select_training_trades(full_window, max_train_trades=max_train_trades)

    frames: list[pd.DataFrame] = []
    skipped = 0
    minute_cache: dict[str, pd.DataFrame] = {}
    for _, row in selected.iterrows():
        try:
            code = str(row["code"]).zfill(6)[-6:]
            if code not in minute_cache:
                minute_cache[code] = _load_code_minute_frame(code)
            window = _slice_trade_minute_window(minute_cache[code], list(row["future_days"]))
            if _complete_window_day_count(window) < 3:
                skipped += 1
                continue
            features = _build_trade_feature_rows(window, float(row["entry_price"]), _candidate_context(row))
            features = _attach_exit_labels(features, float(row["entry_price"]))
            features["selection_date"] = str(row["date"])
            features["code"] = code
            features["strategy_type"] = str(row["strategy_type"])
            sampled = _sample_training_bars(features)
            if not sampled.empty:
                frames.append(sampled)
        except Exception:
            skipped += 1

    dataset = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    coverage = {
        "candidate_rows": int(len(candidates)),
        "swing_candidate_rows": int(len(swing)),
        "full_window_candidate_rows": int(len(full_window)),
        "partial_window_candidate_rows": int(((swing["minute_coverage_days"] > 0) & (swing["minute_coverage_days"] < 3)).sum()),
        "missing_window_candidate_rows": int((swing["minute_coverage_days"] == 0).sum()),
        "selected_train_trades": int(len(selected)),
        "skipped_selected_trades": int(skipped),
        "training_bar_rows": int(len(dataset)),
        "minute_files_loaded_for_training": int(len(minute_cache)),
        "minute_full_day_bars": MIN_FULL_DAY_BARS,
        "minute_files_with_required_days": int(len(coverage_index)),
        "coverage_note": "训练从完整 T+1-T+3 5m 覆盖的历史候选池中按日期/策略排序抽样，避免 8 万+候选全量展开导致内存失控。",
    }
    if not dataset.empty:
        coverage["training_label_counts"] = {
            LABEL_NAMES[int(key)]: int(value)
            for key, value in dataset["label"].value_counts().sort_index().to_dict().items()
        }
    return dataset, coverage


def replay_top_pick_intraday_exit(
    months: int,
    model: Any,
    feature_columns: list[str],
    refresh_backtest: bool = False,
) -> dict[str, Any]:
    backtest = _load_top_pick_backtest(months, refresh_backtest)
    rows = [row for row in backtest.get("rows", []) if str(row.get("strategy_type")) in SWING_STRATEGY_TYPES]
    trading_dates = _trading_dates_from_db()
    next3 = _next3_trading_dates(trading_dates)
    required_dates = {day for row in rows for day in next3.get(str(row.get("date")), [])}
    coverage_index = _minute_coverage_index(required_dates)
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        item = _replay_one_top_pick(row, next3, coverage_index, model, feature_columns)
        out_rows.append(item)

    evaluated = [row for row in out_rows if row.get("model_exit_return_pct") is not None]
    original_evaluated = [row for row in out_rows if row.get("original_t3_return_pct") is not None]
    covered_original = [row for row in evaluated if row.get("original_t3_return_pct") is not None]
    strategy_performance = _strategy_replay_performance(out_rows)
    summary = {
        "top_pick_swing_rows": int(len(rows)),
        "covered_replay_rows": int(len(evaluated)),
        "missing_replay_rows": int(len(rows) - len(evaluated)),
        "coverage_rate_pct": _pct(len(evaluated), len(rows)),
        "original_t3_win_rate": _win_rate([row.get("original_t3_return_pct") for row in original_evaluated]),
        "original_t3_avg_return_pct": _avg([row.get("original_t3_return_pct") for row in original_evaluated]),
        "covered_original_t3_win_rate": _win_rate([row.get("original_t3_return_pct") for row in covered_original]),
        "covered_original_t3_avg_return_pct": _avg([row.get("original_t3_return_pct") for row in covered_original]),
        "model_exit_win_rate": _win_rate([row.get("model_exit_return_pct") for row in evaluated]),
        "model_exit_avg_return_pct": _avg([row.get("model_exit_return_pct") for row in evaluated]),
        "model_exit_avg_capture_rate_pct": _avg([row.get("capture_rate_pct") for row in evaluated]),
        "oracle_float_win_rate": _win_rate([row.get("oracle_max_gain_pct") for row in evaluated]),
        "oracle_avg_max_gain_pct": _avg([row.get("oracle_max_gain_pct") for row in evaluated]),
        "action_counts": dict(Counter(str(row.get("exit_action") or "missing") for row in out_rows)),
    }
    return {
        "summary": summary,
        "strategy_performance": strategy_performance,
        "rows": out_rows,
    }


def _load_top_pick_backtest(months: int, refresh_backtest: bool) -> dict[str, Any]:
    path = BASE_DIR / "data" / "strategy_cache" / f"top_pick_backtest_m{int(months)}.json"
    if not refresh_backtest and path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
                return payload
        except Exception:
            pass
    return top_pick_open_backtest(months=months, refresh=refresh_backtest)


def load_intraday_exit_model() -> tuple[Any, dict[str, Any]]:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}")
    if not INTRADAY_EXIT_MODEL_PATH.exists():
        raise FileNotFoundError(f"缺少盘中卖点模型：{INTRADAY_EXIT_MODEL_PATH}")
    model = XGBClassifier()
    model.load_model(str(INTRADAY_EXIT_MODEL_PATH))
    meta = json.loads(INTRADAY_EXIT_META_PATH.read_text(encoding="utf-8")) if INTRADAY_EXIT_META_PATH.exists() else {}
    return model, meta


def _load_evaluated_candidates(months: int, refresh_candidates: bool = False) -> pd.DataFrame:
    if refresh_candidates:
        prepared = prepare_evaluated_candidates(months, refresh=True)
        return prepared.get("evaluated", pd.DataFrame())
    path = Path(str(EVALUATED_CANDIDATE_TEMPLATE).format(months=int(months)))
    if not path.exists():
        prepared = prepare_evaluated_candidates(months, refresh=False)
        return prepared.get("evaluated", pd.DataFrame())
    try:
        import pyarrow.parquet as pq

        names = set(pq.ParquetFile(path).schema_arrow.names)
        columns = [col for col in _CANDIDATE_COLUMNS if col in names]
        return pd.read_parquet(path, columns=columns)
    except Exception:
        return pd.read_parquet(path)


def _normalise_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        return out
    if "code" not in out.columns:
        out["code"] = out.get("纯代码", "")
    out["code"] = out["code"].fillna("").astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        out["date"] = ""
    if "strategy_type" not in out.columns:
        out["strategy_type"] = ""
    out["strategy_type"] = out["strategy_type"].fillna("").astype(str)
    if "entry_price" not in out.columns:
        price = _first_numeric(out, ["最新价", "close"])
        out["entry_price"] = price
    for col in ["综合评分", "排序评分", "预期溢价", "生产门槛", "theme_momentum_3d", "theme_pct_chg_3"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "market_gate_mode" not in out.columns:
        out["market_gate_mode"] = ""
    out = out[(out["code"].str.len() == 6) & (pd.to_numeric(out["entry_price"], errors="coerce") > 0)].copy()
    return out


def _select_training_trades(full_window: pd.DataFrame, max_train_trades: int) -> pd.DataFrame:
    if full_window.empty:
        return full_window
    sort_cols = ["date", "strategy_type", "排序评分", "预期溢价", "综合评分"]
    sorted_df = full_window.sort_values(sort_cols, ascending=[True, True, False, False, False]).copy()
    ranked = sorted_df.groupby(["date", "strategy_type"], sort=False).head(max(1, DEFAULT_MAX_PER_DATE_STRATEGY)).copy()
    if max_train_trades > 0 and len(ranked) > max_train_trades:
        ranked = ranked.sort_values(["date", "strategy_type", "排序评分"], ascending=[True, True, False]).head(max_train_trades)
    return ranked.reset_index(drop=True)


def _trading_dates_from_db() -> list[str]:
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT DISTINCT date FROM stock_daily ORDER BY date ASC").fetchall()
    return [str(row["date"]) for row in rows if row["date"]]


def _next3_trading_dates(trading_dates: list[str]) -> dict[str, list[str]]:
    return {
        day: trading_dates[index + 1 : index + 4]
        for index, day in enumerate(trading_dates)
        if len(trading_dates[index + 1 : index + 4]) == 3
    }


def _minute_coverage_index(required_dates: set[str]) -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = {}
    root = MIN_KLINE_DIR / "5m"
    for path in sorted(root.glob("*.parquet")):
        code = path.stem[-6:]
        try:
            dt = pd.to_datetime(pd.read_parquet(path, columns=["datetime"])["datetime"], errors="coerce").dropna()
        except Exception:
            continue
        if dt.empty:
            continue
        counts = dt.dt.strftime("%Y-%m-%d").value_counts()
        full_days = {str(day) for day, count in counts.items() if int(count) >= MIN_FULL_DAY_BARS and (not required_dates or str(day) in required_dates)}
        if full_days:
            coverage[code] = full_days
    return coverage


def _covered_future_day_count(code: str, future_days: list[str], coverage_index: dict[str, set[str]]) -> int:
    days = coverage_index.get(str(code).zfill(6), set())
    return int(sum(1 for day in future_days if day in days))


def _load_trade_minute_window(code: str, future_days: list[str]) -> pd.DataFrame:
    return _slice_trade_minute_window(_load_code_minute_frame(code), future_days)


def _load_code_minute_frame(code: str) -> pd.DataFrame:
    path = MIN_KLINE_DIR / "5m" / f"{str(code).zfill(6)}.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty or "datetime" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"])
    out["trade_date"] = out["datetime"].dt.strftime("%Y-%m-%d")
    if out.empty:
        return out
    out["trade_time"] = out["datetime"].dt.strftime("%H:%M:%S")
    out = out[(out["trade_time"] >= "09:35:00") & (out["trade_time"] <= "15:00:00")].copy()
    for col in ["open", "high", "low", "close", "volume", "amount", "money"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["amount"] = out["amount"].where(out["amount"] > 0, out["money"])
    out["amount"] = out["amount"].where(out["amount"] > 0, out["close"] * out["volume"])
    return out.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)


def _slice_trade_minute_window(code_frame: pd.DataFrame, future_days: list[str]) -> pd.DataFrame:
    if code_frame.empty:
        return code_frame
    out = code_frame[code_frame["trade_date"].isin(set(future_days))].copy()
    if out.empty:
        return out
    order_map = {day: index + 1 for index, day in enumerate(future_days)}
    out["trading_day_index"] = out["trade_date"].map(order_map).fillna(0).astype(int)
    return out.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)


def _complete_window_day_count(window: pd.DataFrame) -> int:
    if window.empty:
        return 0
    return int((window.groupby("trade_date")["datetime"].count() >= MIN_FULL_DAY_BARS).sum())


def _candidate_context(row: pd.Series) -> dict[str, float]:
    strategy = str(row.get("strategy_type") or "")
    tier = str(row.get("selection_tier") or "base")
    theme = row.get("theme_momentum_3d")
    if pd.isna(theme):
        theme = row.get("theme_pct_chg_3")
    gate = str(row.get("market_gate_mode") or "")
    return {
        "strategy_code": STRATEGY_CODE.get(strategy, 0.0),
        "is_global_momentum": 1.0 if strategy == "全局动量狙击" else 0.0,
        "is_main_wave": 1.0 if strategy == "右侧主升浪" else 0.0,
        "is_reversal": 1.0 if strategy == "中线超跌反转" else 0.0,
        "selection_tier_code": 1.0 if tier == "dynamic_floor" else 0.0,
        "selection_score": _safe_float(row.get("综合评分")),
        "sort_score": _safe_float(row.get("排序评分")),
        "expected_t3_max_gain_pct": _safe_float(row.get("预期溢价")),
        "theme_momentum_3d": _safe_float(theme),
        "market_gate_code": MARKET_GATE_CODE.get(gate, 0.0),
    }


def _build_trade_feature_rows(window: pd.DataFrame, entry_price: float, context: dict[str, float]) -> pd.DataFrame:
    out = window.copy()
    entry = max(float(entry_price), 0.0001)
    close = pd.to_numeric(out["close"], errors="coerce").ffill().fillna(0.0)
    high = pd.to_numeric(out["high"], errors="coerce").ffill().fillna(close)
    low = pd.to_numeric(out["low"], errors="coerce").ffill().fillna(close)
    open_ = pd.to_numeric(out["open"], errors="coerce").ffill().fillna(close)
    volume = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)

    out["current_gain_pct"] = (close / entry - 1.0) * 100.0
    out["bar_high_gain_pct"] = (high / entry - 1.0) * 100.0
    out["bar_low_gain_pct"] = (low / entry - 1.0) * 100.0
    running_high = high.cummax().replace(0, np.nan)
    running_low = low.cummin().replace(0, np.nan)
    out["running_high_gain_pct"] = (running_high / entry - 1.0) * 100.0
    out["running_low_gain_pct"] = (running_low / entry - 1.0) * 100.0
    out["drawdown_from_high_pct"] = (close / running_high - 1.0) * 100.0
    out["bar_index"] = np.arange(len(out), dtype=float)
    out["bar_index_day"] = out.groupby("trade_date").cumcount().astype(float)
    dt = pd.to_datetime(out["datetime"], errors="coerce")
    out["minutes_from_open"] = (dt.dt.hour * 60 + dt.dt.minute - (9 * 60 + 30)).astype(float)
    out["is_morning"] = (dt.dt.hour < 12).astype(float)
    out["is_afternoon"] = (dt.dt.hour >= 13).astype(float)
    out["is_last_hour"] = ((dt.dt.hour == 14) | ((dt.dt.hour == 15) & (dt.dt.minute == 0))).astype(float)

    for window_size in [1, 3, 6, 12]:
        out[f"ret_{window_size}"] = close.pct_change(window_size).fillna(0.0) * 100.0
    for window_size in [3, 6, 12]:
        avg_volume = volume.rolling(window_size, min_periods=1).mean().shift(1)
        out[f"volume_ratio_{window_size}"] = _safe_div_series(volume, avg_volume).fillna(1.0)
    for window_size in [6, 12]:
        avg_amount = amount.rolling(window_size, min_periods=1).mean().shift(1)
        out[f"amount_ratio_{window_size}"] = _safe_div_series(amount, avg_amount).fillna(1.0)

    cum_amount = amount.groupby(out["trade_date"]).cumsum()
    cum_volume = volume.groupby(out["trade_date"]).cumsum()
    vwap = _safe_div_series(cum_amount, cum_volume)
    out["vwap_dev_pct"] = (_safe_div_series(close, vwap) - 1.0).fillna(0.0) * 100.0
    prev_close = close.shift(1).replace(0, np.nan)
    out["range_pct"] = _safe_div_series(high - low, prev_close).fillna(0.0) * 100.0
    out["body_pct"] = _safe_div_series(close - open_, open_).fillna(0.0) * 100.0
    out["upper_shadow_pct"] = _safe_div_series(high - np.maximum(open_, close), prev_close).fillna(0.0) * 100.0
    out["lower_shadow_pct"] = _safe_div_series(np.minimum(open_, close) - low, prev_close).fillna(0.0) * 100.0
    out["close_location"] = _safe_div_series(close - low, high - low).fillna(0.5)
    returns = close.pct_change().fillna(0.0) * 100.0
    out["rolling_volatility_6"] = returns.rolling(6, min_periods=2).std(ddof=0).fillna(0.0)
    out["rolling_volatility_12"] = returns.rolling(12, min_periods=2).std(ddof=0).fillna(0.0)
    for window_size in [3, 6, 12]:
        ma = close.rolling(window_size, min_periods=1).mean()
        out[f"ma_slope_{window_size}"] = ma.pct_change(3).fillna(0.0) * 100.0
        if window_size in {6, 12}:
            out[f"close_ma_bias_{window_size}"] = (_safe_div_series(close, ma) - 1.0).fillna(0.0) * 100.0

    up = (close.diff() > 0).astype(int)
    down = (close.diff() < 0).astype(int)
    out["consecutive_up"] = _consecutive_count(up)
    out["consecutive_down"] = _consecutive_count(down)
    for key, value in context.items():
        out[key] = float(value)
    return out


def _attach_exit_labels(features: pd.DataFrame, entry_price: float) -> pd.DataFrame:
    out = features.copy()
    entry = max(float(entry_price), 0.0001)
    high_gain = pd.to_numeric(out["high"], errors="coerce").fillna(0.0) / entry * 100.0 - 100.0
    low_gain = pd.to_numeric(out["low"], errors="coerce").fillna(0.0) / entry * 100.0 - 100.0
    close_gain = pd.to_numeric(out["close"], errors="coerce").fillna(0.0) / entry * 100.0 - 100.0
    future_high_gain = high_gain.iloc[::-1].cummax().iloc[::-1]
    future_low_gain = low_gain.iloc[::-1].cummin().iloc[::-1]
    max_gain = float(high_gain.max()) if len(high_gain) else 0.0
    peak_pos = int(high_gain.values.argmax()) if len(high_gain) else 0

    label = pd.Series(LABEL_HOLD, index=out.index, dtype="int64")
    bar_index = pd.Series(np.arange(len(out)), index=out.index)
    near_peak = (bar_index >= max(0, peak_pos - 1)) & (bar_index <= min(len(out) - 1, peak_pos + 2))
    top_zone = (
        (max_gain >= 1.5)
        & (high_gain >= max_gain - 0.55)
        & (close_gain >= MIN_TAKE_PROFIT_PCT)
        & near_peak
    )
    rollover_take = (
        (out["running_high_gain_pct"] >= 2.2)
        & (out["drawdown_from_high_pct"] <= -0.75)
        & (future_high_gain <= out["running_high_gain_pct"] + 0.35)
        & (close_gain >= MIN_TAKE_PROFIT_PCT)
    )
    stop_loss = ((close_gain <= -3.0) & (future_high_gain < 0.8)) | (close_gain <= -5.0)
    last_chance_stop = (future_low_gain <= -4.0) & (close_gain <= -2.0) & (future_high_gain < 1.2)
    label.loc[top_zone | rollover_take] = LABEL_TAKE_PROFIT
    label.loc[stop_loss | last_chance_stop] = LABEL_STOP_LOSS
    out["label"] = label
    out["future_high_gain_pct"] = future_high_gain
    out["future_low_gain_pct"] = future_low_gain
    out["oracle_max_gain_pct"] = max_gain
    return out


def _sample_training_bars(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features
    non_hold = features["label"].astype(int) != LABEL_HOLD
    stride = max(2, DEFAULT_HOLD_SAMPLE_STRIDE)
    hold_sample = (features["bar_index"].astype(int) % stride == 0) | (features["bar_index_day"].astype(int) == 0)
    sampled = features[non_hold | hold_sample].copy()
    keep_cols = ["selection_date", "code", "strategy_type", "datetime", "label", *FEATURE_COLUMNS]
    return sampled[keep_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _replay_one_top_pick(
    row: dict[str, Any],
    next3: dict[str, list[str]],
    coverage_index: dict[str, set[str]],
    model: Any,
    feature_columns: list[str],
) -> dict[str, Any]:
    selection_date = str(row.get("date") or row.get("selection_date") or "")[:10]
    code = str(row.get("code") or "").zfill(6)[-6:]
    future_days = next3.get(selection_date, [])
    coverage_days = _covered_future_day_count(code, future_days, coverage_index)
    base = {
        "date": selection_date,
        "code": code,
        "name": row.get("name"),
        "strategy_type": row.get("strategy_type"),
        "original_t3_return_pct": _optional_float(row.get("t3_settlement_return_pct")),
        "original_t3_max_gain_pct": _optional_float(row.get("t3_max_gain_pct")),
        "minute_coverage_days": coverage_days,
    }
    if coverage_days < 3:
        base.update({"status": "missing_minute_coverage", "exit_action": "missing"})
        return base

    entry_price = _entry_price_from_backtest_row(row)
    window = _load_trade_minute_window(code, future_days)
    if _complete_window_day_count(window) < 3 or entry_price <= 0:
        base.update({"status": "missing_minute_coverage", "exit_action": "missing"})
        return base

    context = _context_from_backtest_row(row)
    features = _build_trade_feature_rows(window, entry_price, context)
    probabilities = _predict_probabilities(model, features, feature_columns)
    exit_index, exit_action, exit_prob = _first_exit_signal(features, probabilities)
    oracle_max_gain = float(((features["high"].max() / entry_price) - 1.0) * 100.0)
    if exit_index is None:
        close_price = float(features.iloc[-1]["close"])
        checked_at = pd.to_datetime(features.iloc[-1]["datetime"]).strftime("%Y-%m-%d %H:%M:%S")
        action = "hold_to_t3_close"
        prob = None
    else:
        action = exit_action or "model_exit"
        prob = exit_prob
        execution_index = min(exit_index + 1, len(features) - 1)
        execution_row = features.iloc[execution_index]
        close_price = float(execution_row["open"] if execution_index > exit_index else execution_row["close"])
        checked_at = pd.to_datetime(execution_row["datetime"]).strftime("%Y-%m-%d %H:%M:%S")

    exit_return = (close_price / entry_price - 1.0) * 100.0
    capture_rate = (exit_return / oracle_max_gain * 100.0) if oracle_max_gain > 0 and exit_return > 0 else None
    base.update(
        {
            "status": "replayed",
            "entry_price": round(entry_price, 4),
            "exit_action": action,
            "exit_price": round(close_price, 4),
            "exit_checked_at": checked_at,
            "exit_probability": round(float(prob), 6) if prob is not None else None,
            "model_exit_return_pct": round(exit_return, 4),
            "oracle_max_gain_pct": round(oracle_max_gain, 4),
            "capture_rate_pct": round(float(capture_rate), 4) if capture_rate is not None and math.isfinite(capture_rate) else None,
        }
    )
    return base


def _entry_price_from_backtest_row(row: dict[str, Any]) -> float:
    for key in ("snapshot_price", "selection_price", "close"):
        value = _optional_float(row.get(key))
        if value is not None and value > 0:
            return float(value)
    return 0.0


def _context_from_backtest_row(row: dict[str, Any]) -> dict[str, float]:
    strategy = str(row.get("strategy_type") or "")
    return {
        "strategy_code": STRATEGY_CODE.get(strategy, 0.0),
        "is_global_momentum": 1.0 if strategy == "全局动量狙击" else 0.0,
        "is_main_wave": 1.0 if strategy == "右侧主升浪" else 0.0,
        "is_reversal": 1.0 if strategy == "中线超跌反转" else 0.0,
        "selection_tier_code": 1.0 if str(row.get("selection_tier") or "") == "dynamic_floor" else 0.0,
        "selection_score": _safe_float(row.get("composite_score")),
        "sort_score": _safe_float(row.get("sort_score")),
        "expected_t3_max_gain_pct": _safe_float(row.get("expected_premium")),
        "theme_momentum_3d": _safe_float(row.get("theme_momentum_3d") or row.get("theme_pct_chg_3")),
        "market_gate_code": MARKET_GATE_CODE.get(str(row.get("market_gate_mode") or ""), 0.0),
    }


def _predict_probabilities(model: Any, features: pd.DataFrame, feature_columns: list[str]) -> np.ndarray:
    aligned = _align_features(features, feature_columns)
    probabilities = model.predict_proba(aligned)
    if probabilities.shape[1] < 3:
        padded = np.zeros((len(probabilities), 3), dtype=float)
        padded[:, : probabilities.shape[1]] = probabilities
        return padded
    return probabilities


def _first_exit_signal(features: pd.DataFrame, probabilities: np.ndarray) -> tuple[Optional[int], Optional[str], Optional[float]]:
    for idx in range(len(features)):
        current_gain = float(features.iloc[idx]["current_gain_pct"])
        take_prob = float(probabilities[idx, LABEL_TAKE_PROFIT])
        stop_prob = float(probabilities[idx, LABEL_STOP_LOSS])
        if stop_prob >= STOP_LOSS_THRESHOLD and current_gain <= MIN_STOP_LOSS_PCT:
            return idx, "model_stop_loss", stop_prob
        if take_prob >= TAKE_PROFIT_THRESHOLD and current_gain >= MIN_TAKE_PROFIT_PCT:
            return idx, "model_take_profit", take_prob
    return None, None, None


def _strategy_replay_performance(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for strategy in ["全局动量狙击", "右侧主升浪", "中线超跌反转"]:
        items = [row for row in rows if row.get("strategy_type") == strategy]
        covered = [row for row in items if row.get("model_exit_return_pct") is not None]
        out.append(
            {
                "strategy_type": strategy,
                "rows": int(len(items)),
                "covered": int(len(covered)),
                "missing": int(len(items) - len(covered)),
                "coverage_rate_pct": _pct(len(covered), len(items)),
                "original_t3_win_rate": _win_rate([row.get("original_t3_return_pct") for row in items]),
                "original_t3_avg_return_pct": _avg([row.get("original_t3_return_pct") for row in items]),
                "covered_original_t3_win_rate": _win_rate([row.get("original_t3_return_pct") for row in covered]),
                "covered_original_t3_avg_return_pct": _avg([row.get("original_t3_return_pct") for row in covered]),
                "model_exit_win_rate": _win_rate([row.get("model_exit_return_pct") for row in covered]),
                "model_exit_avg_return_pct": _avg([row.get("model_exit_return_pct") for row in covered]),
                "oracle_float_win_rate": _win_rate([row.get("oracle_max_gain_pct") for row in covered]),
                "oracle_avg_max_gain_pct": _avg([row.get("oracle_max_gain_pct") for row in covered]),
                "avg_capture_rate_pct": _avg([row.get("capture_rate_pct") for row in covered]),
                "action_counts": dict(Counter(str(row.get("exit_action") or "missing") for row in items)),
            }
        )
    return out


def _breakout_threshold_sweep_summary(months: int, refresh: bool) -> dict[str, Any]:
    try:
        from quant_core.threshold_sweep import choose_sweet_spot, sweep_thresholds

        rows = sweep_thresholds(months=months, start=64.0, end=72.0, step=0.5, refresh=refresh)
        sweet = choose_sweet_spot(rows)
        return {
            "status": "ready",
            "rows": [row.__dict__ for row in rows],
            "recommended_threshold": sweet.threshold if sweet else None,
            "recommendation": (
                "低阈值满足胜率>=85%、平均开盘溢价>=2%、最大连亏<=3且出手明显增加"
                if sweet
                else "未找到满足降阈值安全条件的门槛，生产门槛继续保持 72"
            ),
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _classification_metrics(y_true: np.ndarray, predictions: np.ndarray) -> dict[str, Any]:
    total = max(1, len(y_true))
    metrics: dict[str, Any] = {
        "accuracy": round(float((predictions == y_true).sum() / total), 6),
    }
    for label, name in LABEL_NAMES.items():
        tp = int(((predictions == label) & (y_true == label)).sum())
        fp = int(((predictions == label) & (y_true != label)).sum())
        fn = int(((predictions != label) & (y_true == label)).sum())
        metrics[f"{name}_precision"] = round(tp / max(1, tp + fp), 6)
        metrics[f"{name}_recall"] = round(tp / max(1, tp + fn), 6)
        metrics[f"{name}_support"] = int((y_true == label).sum())
    return metrics


def _top_feature_importance(model: Any, feature_columns: list[str], topn: int = 20) -> dict[str, float]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    pairs = sorted(zip(feature_columns, [float(value) for value in values]), key=lambda item: item[1], reverse=True)
    return {name: round(value, 8) for name, value in pairs[:topn]}


def _align_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in feature_columns:
        if col not in out.columns:
            out[col] = 0.0
    aligned = out[feature_columns].apply(pd.to_numeric, errors="coerce")
    return aligned.replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0).astype("float32")


def _first_numeric(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series(np.nan, index=df.index, dtype="float64")
    for col in columns:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        result = result.where(result.notna(), values)
    return result


def _safe_div_series(left: Any, right: Any) -> pd.Series:
    left_series = left if isinstance(left, pd.Series) else pd.Series(left)
    right_series = right if isinstance(right, pd.Series) else pd.Series(right, index=left_series.index)
    return left_series / right_series.replace(0, np.nan)


def _consecutive_count(flags: pd.Series) -> pd.Series:
    groups = flags.ne(flags.shift()).cumsum()
    counts = flags.groupby(groups).cumcount() + 1
    return counts.where(flags.astype(bool), 0).astype(float)


def _safe_float(value: Any, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _pct(numerator: int, denominator: int) -> float:
    return round(float(numerator) / denominator * 100.0, 4) if denominator else 0.0


def _avg(values: list[Any]) -> float:
    numbers = [float(value) for value in values if _optional_float(value) is not None]
    return round(float(np.mean(numbers)), 4) if numbers else 0.0


def _win_rate(values: list[Any]) -> float:
    numbers = [float(value) for value in values if _optional_float(value) is not None]
    return round(float((np.asarray(numbers) > 0).mean() * 100.0), 4) if numbers else 0.0


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and replay the T+3 5m intraday exit model.")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--retrain", action="store_true")
    parser.add_argument("--max-train-trades", type=int, default=DEFAULT_MAX_TRAIN_TRADES)
    args = parser.parse_args()
    report = run_intraday_exit_backtest(
        months=args.months,
        refresh=args.refresh,
        retrain=args.retrain,
        max_train_trades=args.max_train_trades,
    )
    print(json.dumps(_json_safe({"summary": report.get("summary"), "model": report.get("model", {}).get("metrics")}), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
