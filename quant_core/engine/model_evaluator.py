from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - dependency guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.engine.daily_model_trainer import (
    META_PATH,
    MODEL_PATH,
    build_panel_dataset,
    discover_daily_data_dir,
    global_time_series_split,
    list_daily_files,
)


DEFAULT_THRESHOLDS = [0.5, 0.6, 0.7, 0.8, 0.9]


def load_daily_model(model_path: str | Path = MODEL_PATH) -> XGBClassifier:
    if XGBClassifier is None:
        raise RuntimeError(
            f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}. 请先执行 pip install xgboost"
        )
    model = XGBClassifier()
    model.load_model(str(model_path))
    return model


def evaluate_thresholds(
    probabilities: np.ndarray,
    y_true: pd.Series | np.ndarray,
    thresholds: list[float],
) -> list[dict[str, float | int]]:
    labels = np.asarray(y_true, dtype=int)
    rows: list[dict[str, float | int]] = []
    for threshold in thresholds:
        signals = probabilities >= threshold
        signal_count = int(signals.sum())
        if signal_count:
            wins = int(labels[signals].sum())
            precision = wins / signal_count
        else:
            wins = 0
            precision = 0.0
        rows.append(
            {
                "threshold": float(threshold),
                "signal_count": signal_count,
                "wins": wins,
                "precision": precision,
            }
        )
    return rows


def print_threshold_table(rows: list[dict[str, float | int]]) -> None:
    print("\n========== Probability Threshold Sweep ==========")
    print("| 阈值 | 触发交易次数 | 命中次数 | Precision |")
    print("|---:|---:|---:|---:|")
    for row in rows:
        precision = float(row["precision"])
        line = (
            f"| {float(row['threshold']):.2f} "
            f"| {int(row['signal_count'])} "
            f"| {int(row['wins'])} "
            f"| {precision:.2%} |"
        )
        if float(row["threshold"]) >= 0.8 and precision >= 0.55:
            line = f"\033[91m\033[1m{line}  HIGH-CONFIDENCE\033[0m"
        print(line)
    print("================================================\n")


def _load_meta(meta_path: str | Path) -> dict:
    path = Path(meta_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate daily XGBoost probability thresholds")
    parser.add_argument("--data-dir", default="", help="Daily kline directory. Auto-detected if omitted.")
    parser.add_argument("--limit", type=int, default=100, help="Number of symbols to evaluate. Use 0 for full market.")
    parser.add_argument("--target-horizon", type=int, default=3)
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    parser.add_argument("--thresholds", default="0.5,0.6,0.7,0.8,0.9")
    args = parser.parse_args()

    model = load_daily_model(args.model_path)
    meta = _load_meta(args.meta_path)
    data_dir = Path(args.data_dir) if args.data_dir else discover_daily_data_dir()
    files = list_daily_files(data_dir, limit=args.limit)
    thresholds = [float(item.strip()) for item in args.thresholds.split(",") if item.strip()]

    panel = build_panel_dataset(files, target_horizon=args.target_horizon)
    _, _, X_test, y_test, feature_cols, split_date = global_time_series_split(
        panel,
        train_ratio=args.train_ratio,
        target_horizon=args.target_horizon,
    )
    expected_cols = list(meta.get("feature_columns") or feature_cols)
    X_test = X_test.reindex(columns=expected_cols, fill_value=0.0)
    probabilities = model.predict_proba(X_test)[:, 1]
    rows = evaluate_thresholds(probabilities, y_test, thresholds)

    print("========== Daily Model Evaluator ==========")
    print(f"Data Dir           : {data_dir}")
    print(f"Selected Symbols   : {len(files)}")
    print(f"Panel Rows         : {len(panel)}")
    print(f"Split Date         : {split_date.date()}")
    print(f"Test Rows          : {len(X_test)}")
    print(f"Test Positive Rate : {float(y_test.mean()):.2%}")
    print(f"Model Path         : {args.model_path}")
    print(f"Meta Path          : {args.meta_path}")
    print("==========================================")
    print_threshold_table(rows)


if __name__ == "__main__":
    main()
