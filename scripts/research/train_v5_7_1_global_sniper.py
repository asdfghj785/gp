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
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None


BASE_DIR = Path("/Users/eudis/ths")
DATASET_PATH = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
MODEL_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_1_candidate.json"
META_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_1_candidate.meta.json"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "train_v5_7_1_global_sniper_latest.json"

LABEL_COL = "label_rule_win"
N_SPLITS = 5
PURGE_DAYS = 3

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

KEPT_MICRO_FEATURE_COLUMNS = ["tail_accel", "intra_volatility"]
REMOVED_FEATURES_EXACT = {"smart_money_ratio"}
FEATURE_COLUMNS_RAW = list(dict.fromkeys([*DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS, *KEPT_MICRO_FEATURE_COLUMNS]))
FEATURE_COLUMNS = [
    col
    for col in FEATURE_COLUMNS_RAW
    if not col.startswith("market_") and col not in REMOVED_FEATURES_EXACT
]


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


def read_panel(path: Path, label_col: str) -> tuple[pd.DataFrame, list[str]]:
    header = pd.read_csv(path, nrows=0).columns.tolist()
    removed = [col for col in header if col.startswith("market_") or col in REMOVED_FEATURES_EXACT]
    usecols = ["date", label_col, *[col for col in FEATURE_COLUMNS if col in header]]
    panel = pd.read_csv(path, usecols=list(dict.fromkeys(usecols)), low_memory=False)
    for col in FEATURE_COLUMNS:
        if col not in panel.columns:
            panel[col] = 0.0
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel[label_col] = pd.to_numeric(panel[label_col], errors="coerce")
    panel = panel.dropna(subset=["date", label_col]).copy()
    panel[label_col] = panel[label_col].astype(int)
    panel = panel.sort_values(["date"]).reset_index(drop=True)
    return panel, removed


def align_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for col in FEATURE_COLUMNS:
        out[col] = pd.to_numeric(frame.get(col, 0.0), errors="coerce")
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


def evaluate_fold(y_true: pd.Series, probabilities: np.ndarray) -> dict[str, Any]:
    y = y_true.astype(int).to_numpy()
    pred = (probabilities >= 0.5).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return {
        "auc": float(roc_auc_score(y, probabilities)),
        "average_precision": float(average_precision_score(y, probabilities)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "accuracy": float(accuracy_score(y, pred)),
        "positive_rate": float(np.mean(y)),
        "avg_pred_prob": float(np.mean(probabilities)),
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
    }


def best_iteration_value(model: Any) -> int:
    value = getattr(model, "best_iteration", None)
    if value is not None:
        return int(value)
    try:
        raw = model.get_booster().attributes().get("best_iteration")
    except Exception:
        raw = None
    return int(raw) if raw is not None else -1


def run_time_series_cv(panel: pd.DataFrame, args: argparse.Namespace) -> tuple[list[dict[str, Any]], Any]:
    dates = np.asarray(sorted(panel["date"].astype(str).unique().tolist()))
    splitter = TimeSeriesSplit(n_splits=int(args.n_splits), gap=int(args.purge_days))
    folds: list[dict[str, Any]] = []
    last_model = None

    for fold_id, (train_date_idx, test_date_idx) in enumerate(splitter.split(dates), start=1):
        train_dates = set(dates[train_date_idx].tolist())
        test_dates = set(dates[test_date_idx].tolist())
        train = panel[panel["date"].isin(train_dates)].copy()
        test = panel[panel["date"].isin(test_dates)].copy()
        y_train = train[args.label_col].astype(int)
        y_test = test[args.label_col].astype(int)
        if y_train.nunique() < 2 or y_test.nunique() < 2:
            raise RuntimeError(f"Fold {fold_id} 标签只有单一类别，无法评估。")

        model = build_model(y_train, args)
        model.fit(align_features(train), y_train, eval_set=[(align_features(test), y_test)], verbose=False)
        probabilities = model.predict_proba(align_features(test))[:, 1]
        metrics = evaluate_fold(y_test, probabilities)
        fold_report = {
            "fold": int(fold_id),
            "train_start": str(dates[train_date_idx[0]]),
            "train_end": str(dates[train_date_idx[-1]]),
            "test_start": str(dates[test_date_idx[0]]),
            "test_end": str(dates[test_date_idx[-1]]),
            "train_dates": int(len(train_date_idx)),
            "test_dates": int(len(test_date_idx)),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_label_counts": {str(k): int(v) for k, v in y_train.value_counts().sort_index().to_dict().items()},
            "test_label_counts": {str(k): int(v) for k, v in y_test.value_counts().sort_index().to_dict().items()},
            "best_iteration": best_iteration_value(model),
            "metrics": metrics,
        }
        folds.append(fold_report)
        last_model = model
        print(
            f"[Fold {fold_id}] train={fold_report['train_start']}->{fold_report['train_end']} "
            f"test={fold_report['test_start']}->{fold_report['test_end']} "
            f"AUC={metrics['auc']:.6f} precision={metrics['precision']:.6f} "
            f"best_iter={fold_report['best_iteration']}",
            flush=True,
        )

    if last_model is None:
        raise RuntimeError("TimeSeriesSplit 没有产生有效模型。")
    return folds, last_model


def summarize_cv(folds: list[dict[str, Any]]) -> dict[str, float]:
    metric_names = ["auc", "average_precision", "precision", "recall", "accuracy", "positive_rate"]
    return {
        f"mean_{name}": float(np.mean([fold["metrics"][name] for fold in folds]))
        for name in metric_names
    } | {
        f"std_{name}": float(np.std([fold["metrics"][name] for fold in folds]))
        for name in metric_names
    }


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
        if row["feature"] in KEPT_MICRO_FEATURE_COLUMNS
    }
    return rows, micro_ranks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train V5.7.1 pruned global sniper candidate with TimeSeriesSplit CV.")
    parser.add_argument("--dataset", default=str(DATASET_PATH))
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--label-col", default=LABEL_COL)
    parser.add_argument("--n-splits", type=int, default=N_SPLITS)
    parser.add_argument("--purge-days", type=int, default=PURGE_DAYS)
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.01)
    parser.add_argument("--n-estimators", type=int, default=900)
    parser.add_argument("--early-stopping-rounds", type=int, default=60)
    parser.add_argument("--min-child-weight", type=float, default=10.0)
    parser.add_argument("--subsample", type=float, default=0.85)
    parser.add_argument("--colsample-bytree", type=float, default=0.85)
    parser.add_argument("--reg-lambda", type=float, default=6.0)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    dataset_path = Path(args.dataset)
    model_path = Path(args.model_path)
    meta_path = Path(args.meta_path)
    report_path = Path(args.report_path)

    print("========== V5.7.1 Pruned Global Sniper CV Trainer ==========")
    print(f"Dataset        : {dataset_path}")
    print(f"Model Output   : {model_path}")
    print(f"Feature Count  : {len(FEATURE_COLUMNS)}")
    print(f"Pruned Prefix  : market_*")
    print(f"Pruned Feature : smart_money_ratio")

    panel, removed_features = read_panel(dataset_path, args.label_col)
    folds, last_model = run_time_series_cv(panel, args)
    cv_summary = summarize_cv(folds)
    importance_rows, micro_ranks = feature_importance(last_model)

    model_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    last_model.save_model(str(model_path))

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready",
        "dataset": str(dataset_path),
        "label_col": args.label_col,
        "model_path": str(model_path),
        "feature_columns": FEATURE_COLUMNS,
        "feature_count": len(FEATURE_COLUMNS),
        "removed_features": removed_features,
        "cv": {
            "n_splits": int(args.n_splits),
            "purge_days": int(args.purge_days),
            "summary": cv_summary,
            "folds": folds,
        },
        "last_fold_feature_importance_top20": importance_rows[:20],
        "micro_feature_ranks": micro_ranks,
        "model_params": last_model.get_params(),
        "elapsed_seconds": round(float(time.perf_counter() - started), 3),
    }
    meta_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n---------- CV Summary ----------")
    print(f"Mean AUC       : {cv_summary['mean_auc']:.6f} ± {cv_summary['std_auc']:.6f}")
    print(f"Mean Precision : {cv_summary['mean_precision']:.6f} ± {cv_summary['std_precision']:.6f}")
    print(f"Mean AP        : {cv_summary['mean_average_precision']:.6f} ± {cv_summary['std_average_precision']:.6f}")
    print("\nTop 20 Feature Importance (Last Fold):")
    for row in importance_rows[:20]:
        print(f"  #{row['rank']:02d} {row['feature']}: {row['importance']:.8f}")
    print("\nMicro Feature Ranks:")
    for feature in KEPT_MICRO_FEATURE_COLUMNS:
        item = micro_ranks.get(feature, {"rank": None, "importance": 0.0})
        print(f"  {feature}: rank={item['rank']} importance={item['importance']:.8f}")
    print(f"\nModel Saved    : {model_path}")
    print(f"Meta Saved     : {meta_path}")
    print(f"Elapsed        : {payload['elapsed_seconds']:.3f}s")
    print("===========================================================\n")


if __name__ == "__main__":
    main()
