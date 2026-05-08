from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
except Exception as exc:  # pragma: no cover - local dependency guard
    raise RuntimeError(f"scikit-learn 不可导入：{exc}") from exc

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - local dependency guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None


BASE_DIR = Path("/Users/eudis/ths")
DATASET_PATH = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
MODEL_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_candidate.json"
META_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_candidate.meta.json"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "train_v5_7_global_sniper_latest.json"

DEFAULT_TRAIN_START = "2025-01-02"
DEFAULT_TEST_START = "2025-11-01"
DEFAULT_TEST_END = "2026-01-28"
DEFAULT_LABEL = "label_rule_win"

DAILY_FEATURE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change_pct",
    "turnover",
    "volume_ratio",
    "amount",
    "entry_change_pct_1450",
    "limit_up_blocked",
    "market_up_rate",
    "market_down_count",
    "market_avg_change",
    "market_amount",
    "is_st",
    "is_mainboard",
    "is_chinext",
    "is_star",
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "amplitude_pct",
    "gap_pct",
    "return_3d",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "ma5_bias",
    "ma10_bias",
    "ma20_bias",
    "ma30_bias",
    "ma60_bias",
    "ma20_ma60_spread",
    "high_position_60d",
    "drawdown_60d",
    "pullback_from_60d_high",
    "low_position_60d",
    "volume_ratio_to_5d",
    "volume_ratio_to_10d",
    "volume_ratio_to_20d",
    "amount_ratio_to_5d",
    "amount_ratio_to_10d",
    "amount_ratio_to_20d",
    "turnover_mean_5d",
    "turnover_mean_20d",
    "volatility_5d",
    "volatility_10d",
    "volatility_20d",
]

BUY_5M_FEATURE_COLUMNS = [
    "buy5m_bar_count",
    "buy5m_entry_price",
    "buy5m_ret_from_open_pct",
    "buy5m_ret_15m_pct",
    "buy5m_ret_30m_pct",
    "buy5m_ret_60m_pct",
    "buy5m_ret_120m_pct",
    "buy5m_high_gain_pct",
    "buy5m_low_gain_pct",
    "buy5m_pullback_from_high_pct",
    "buy5m_close_location",
    "buy5m_vwap_dev_pct",
    "buy5m_last_bar_ret_pct",
    "buy5m_last_bar_range_pct",
    "buy5m_last_bar_body_pct",
    "buy5m_last_bar_upper_pct",
    "buy5m_last_bar_lower_pct",
    "buy5m_last_bar_close_location",
    "buy5m_volume_ratio_3",
    "buy5m_volume_ratio_6",
    "buy5m_volume_ratio_12",
    "buy5m_amount_ratio_6",
    "buy5m_amount_ratio_12",
    "buy5m_rolling_volatility_12",
    "buy5m_ma_bias_6_pct",
    "buy5m_ma_bias_12_pct",
    "buy5m_consecutive_up",
    "buy5m_consecutive_down",
]

MICRO_FEATURE_COLUMNS = ["smart_money_ratio", "tail_accel", "intra_volatility"]
FEATURE_COLUMNS = list(dict.fromkeys([*DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS, *MICRO_FEATURE_COLUMNS]))
EXTRA_REPORT_COLUMNS = ["code", "date", "label_t3_win", "rule_return_pct", "t3_close_return_pct_5m"]


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def read_panel(path: Path, label_col: str) -> pd.DataFrame:
    header = pd.read_csv(path, nrows=0).columns.tolist()
    required = ["date", label_col]
    usecols = list(dict.fromkeys([*required, *EXTRA_REPORT_COLUMNS, *[col for col in FEATURE_COLUMNS if col in header]]))
    missing_features = [col for col in FEATURE_COLUMNS if col not in header]
    panel = pd.read_csv(path, usecols=[col for col in usecols if col in header], low_memory=False)
    for col in missing_features:
        panel[col] = 0.0
    if label_col not in panel.columns:
        raise ValueError(f"训练集缺少标签字段：{label_col}")
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel = panel.dropna(subset=["date", label_col]).copy()
    panel[label_col] = pd.to_numeric(panel[label_col], errors="coerce")
    panel = panel.dropna(subset=[label_col]).copy()
    panel[label_col] = panel[label_col].astype(int)
    return panel


def split_by_time(
    panel: pd.DataFrame,
    label_col: str,
    train_start: str,
    test_start: str,
    test_end: str,
    purge_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    dates = sorted(panel["date"].dropna().astype(str).unique().tolist())
    if test_start not in dates:
        test_candidates = [item for item in dates if item >= test_start]
        if not test_candidates:
            raise ValueError(f"找不到测试起点或之后的交易日：{test_start}")
        effective_test_start = test_candidates[0]
    else:
        effective_test_start = test_start
    test_idx = dates.index(effective_test_start)
    train_end_idx = max(0, test_idx - max(0, int(purge_days)) - 1)
    train_end = dates[train_end_idx]

    train = panel[(panel["date"] >= train_start) & (panel["date"] <= train_end)].copy()
    test = panel[(panel["date"] >= effective_test_start) & (panel["date"] <= test_end)].copy()
    if train.empty or test.empty:
        raise ValueError(f"时序切分后训练集或测试集为空：train={len(train)} test={len(test)}")
    if train[label_col].nunique() < 2 or test[label_col].nunique() < 2:
        raise ValueError("训练集或测试集标签只有单一类别，无法评估分类模型。")
    meta = {
        "train_start": train_start,
        "train_end": train_end,
        "test_start": effective_test_start,
        "test_end": test_end,
        "purge_days": int(purge_days),
        "train_dates": int(train["date"].nunique()),
        "test_dates": int(test["date"].nunique()),
    }
    return train, test, meta


def align_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for col in FEATURE_COLUMNS:
        if col in frame.columns:
            out[col] = pd.to_numeric(frame[col], errors="coerce")
        else:
            out[col] = 0.0
    out = out.replace([np.inf, -np.inf], np.nan)
    medians = out.median(numeric_only=True).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out.fillna(medians).astype("float32", copy=False)


def build_model(y_train: pd.Series, args: argparse.Namespace) -> Any:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    return XGBClassifier(
        objective="binary:logistic",
        max_depth=int(args.max_depth),
        learning_rate=float(args.learning_rate),
        n_estimators=int(args.n_estimators),
        min_child_weight=float(args.min_child_weight),
        subsample=float(args.subsample),
        colsample_bytree=float(args.colsample_bytree),
        reg_lambda=float(args.reg_lambda),
        eval_metric="logloss",
        early_stopping_rounds=int(args.early_stopping_rounds),
        scale_pos_weight=neg / max(1, pos),
        tree_method="hist",
        random_state=int(args.random_state),
        n_jobs=-1,
    )


def evaluate_predictions(y_true: pd.Series, probabilities: np.ndarray) -> dict[str, Any]:
    y = y_true.astype(int).to_numpy()
    pred = (probabilities >= 0.5).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    metrics = {
        "auc": float(roc_auc_score(y, probabilities)),
        "average_precision": float(average_precision_score(y, probabilities)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "accuracy": float(accuracy_score(y, pred)),
        "positive_rate": float(np.mean(y)),
        "avg_pred_prob": float(np.mean(probabilities)),
        "max_pred_prob": float(np.max(probabilities)),
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
    }
    threshold_sweep = []
    for threshold in [0.5, 0.6, 0.7, 0.8, 0.9]:
        picked = probabilities >= threshold
        if picked.any():
            threshold_sweep.append(
                {
                    "threshold": threshold,
                    "picked": int(picked.sum()),
                    "precision": float(np.mean(y[picked] == 1)),
                    "coverage_pct": float(picked.mean() * 100.0),
                }
            )
        else:
            threshold_sweep.append({"threshold": threshold, "picked": 0, "precision": 0.0, "coverage_pct": 0.0})
    metrics["threshold_sweep"] = threshold_sweep
    return metrics


def summarize_daily_top1(test: pd.DataFrame, probabilities: np.ndarray, label_col: str) -> dict[str, Any]:
    scored = test.copy()
    scored["model_probability"] = probabilities
    scored = scored.sort_values(["date", "model_probability"], ascending=[True, False])
    top1 = scored.groupby("date", sort=False).head(1).copy()
    out: dict[str, Any] = {
        "dates": int(top1["date"].nunique()),
        "label_win_rate_pct": round(float(top1[label_col].mean() * 100.0), 4) if not top1.empty else 0.0,
        "avg_probability": round(float(top1["model_probability"].mean()), 6) if not top1.empty else 0.0,
    }
    for col in ("rule_return_pct", "t3_close_return_pct_5m"):
        if col in top1.columns:
            values = pd.to_numeric(top1[col], errors="coerce").dropna()
            out[col] = {
                "rows": int(len(values)),
                "win_rate_pct": round(float((values > 0).mean() * 100.0), 4) if not values.empty else 0.0,
                "mean_pct": round(float(values.mean()), 4) if not values.empty else 0.0,
                "median_pct": round(float(values.median()), 4) if not values.empty else 0.0,
            }
    return out


def feature_importance(model: Any) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return [], {}
    rows = [
        {"rank": idx + 1, "feature": feature, "importance": float(value)}
        for idx, (feature, value) in enumerate(
            sorted(zip(FEATURE_COLUMNS, [float(item) for item in values]), key=lambda item: item[1], reverse=True)
        )
    ]
    micro_ranks = {
        row["feature"]: {"rank": int(row["rank"]), "importance": float(row["importance"])}
        for row in rows
        if row["feature"] in MICRO_FEATURE_COLUMNS
    }
    return rows, micro_ranks


def best_iteration_value(model: Any) -> int:
    value = getattr(model, "best_iteration", None)
    if value is not None:
        return int(value)
    try:
        raw = model.get_booster().attributes().get("best_iteration")
    except Exception:
        raw = None
    return int(raw) if raw is not None else -1


def best_score_value(model: Any) -> float | None:
    value = getattr(model, "best_score", None)
    if value is not None:
        return float(value)
    try:
        raw = model.get_booster().attributes().get("best_score")
    except Exception:
        raw = None
    return float(raw) if raw is not None else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train isolated V5.7 global sniper candidate model.")
    parser.add_argument("--dataset", default=str(DATASET_PATH))
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--label-col", default=DEFAULT_LABEL)
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", default=DEFAULT_TEST_END)
    parser.add_argument("--purge-days", type=int, default=3)
    parser.add_argument("--n-estimators", type=int, default=500)
    parser.add_argument("--early-stopping-rounds", type=int, default=40)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--learning-rate", type=float, default=0.045)
    parser.add_argument("--min-child-weight", type=float, default=8.0)
    parser.add_argument("--subsample", type=float, default=0.85)
    parser.add_argument("--colsample-bytree", type=float, default=0.85)
    parser.add_argument("--reg-lambda", type=float, default=4.0)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    dataset_path = Path(args.dataset)
    model_path = Path(args.model_path)
    meta_path = Path(args.meta_path)
    report_path = Path(args.report_path)

    print("========== V5.7 Global Sniper Candidate Trainer ==========")
    print(f"Dataset      : {dataset_path}")
    print(f"Label        : {args.label_col}")
    print(f"Output Model : {model_path}")

    panel = read_panel(dataset_path, args.label_col)
    train, test, split_meta = split_by_time(
        panel,
        args.label_col,
        args.train_start,
        args.test_start,
        args.test_end,
        args.purge_days,
    )

    X_train = align_features(train)
    y_train = train[args.label_col].astype(int)
    X_test = align_features(test)
    y_test = test[args.label_col].astype(int)

    model = build_model(y_train, args)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    probabilities = model.predict_proba(X_test)[:, 1]
    metrics = evaluate_predictions(y_test, probabilities)
    top1 = summarize_daily_top1(test, probabilities, args.label_col)
    importance_rows, micro_ranks = feature_importance(model)

    model_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready",
        "dataset": str(dataset_path),
        "label_col": args.label_col,
        "model_path": str(model_path),
        "feature_columns": FEATURE_COLUMNS,
        "feature_count": len(FEATURE_COLUMNS),
        "micro_feature_columns": MICRO_FEATURE_COLUMNS,
        "split": split_meta,
        "rows": {
            "panel": int(len(panel)),
            "train": int(len(train)),
            "test": int(len(test)),
        },
        "label_counts": {
            "train": {str(k): int(v) for k, v in y_train.value_counts().sort_index().to_dict().items()},
            "test": {str(k): int(v) for k, v in y_test.value_counts().sort_index().to_dict().items()},
        },
        "model_params": model.get_params(),
        "best_iteration": best_iteration_value(model),
        "best_score": best_score_value(model),
        "metrics": metrics,
        "daily_top1": top1,
        "feature_importance_top30": importance_rows[:30],
        "micro_feature_ranks": micro_ranks,
        "elapsed_seconds": round(float(time.perf_counter() - started), 3),
    }
    meta_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Rows         : train={len(train)} test={len(test)}")
    print(
        "Split        : "
        f"train {split_meta['train_start']} -> {split_meta['train_end']} / "
        f"test {split_meta['test_start']} -> {split_meta['test_end']} / "
        f"purge_days={split_meta['purge_days']}"
    )
    print("Metrics      :")
    print(f"  AUC                : {metrics['auc']:.6f}")
    print(f"  Average Precision  : {metrics['average_precision']:.6f}")
    print(f"  Precision@0.5      : {metrics['precision']:.6f}")
    print(f"  Recall@0.5         : {metrics['recall']:.6f}")
    print(f"  F1@0.5             : {metrics['f1']:.6f}")
    print(f"  Accuracy           : {metrics['accuracy']:.6f}")
    print(f"  Confusion          : TP={metrics['tp']} FP={metrics['fp']} TN={metrics['tn']} FN={metrics['fn']}")
    print("Threshold Precision:")
    for row in metrics["threshold_sweep"]:
        print(
            f"  p>={row['threshold']:.1f}: picked={row['picked']} "
            f"precision={row['precision']:.4f} coverage={row['coverage_pct']:.4f}%"
        )
    print("Top 30 Feature Importance:")
    for row in importance_rows[:30]:
        print(f"  #{row['rank']:02d} {row['feature']}: {row['importance']:.8f}")
    print("Micro Feature Ranks:")
    for feature in MICRO_FEATURE_COLUMNS:
        item = micro_ranks.get(feature, {"rank": None, "importance": 0.0})
        print(f"  {feature}: rank={item['rank']} importance={item['importance']:.8f}")
    print(f"Model Saved  : {model_path}")
    print(f"Meta Saved   : {meta_path}")
    print(f"Elapsed      : {payload['elapsed_seconds']:.3f}s")
    print("=========================================================\n")


if __name__ == "__main__":
    main()
