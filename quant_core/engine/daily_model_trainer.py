from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - dependency guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.config import BASE_DIR, DATA_DIR, MODELS_DIR
from quant_core.engine.daily_factor_factory import build_daily_factors, feature_columns


MODEL_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.json"
META_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.meta.json"


@dataclass(frozen=True)
class PanelTrainResult:
    model: Any
    model_path: Path
    meta_path: Path
    feature_columns: list[str]
    split_date: pd.Timestamp
    feature_importance_top15: dict[str, float]
    metrics: dict[str, float | int]


def discover_daily_data_dir(base_dir: str | Path = BASE_DIR) -> Path:
    """Detect the best local daily K-line directory."""
    base = Path(base_dir)
    candidates = [
        DATA_DIR,
        base / "data" / "all_kline",
        base / "data" / "day_kline",
        base / "data" / "daily",
        base / "data" / "stock_data",
        base / "data" / "kline",
    ]
    scored: list[tuple[int, Path]] = []
    for directory in candidates:
        if not directory.exists() or not directory.is_dir():
            continue
        files = list(directory.glob("*_daily.parquet")) + list(directory.glob("*.parquet")) + list(directory.glob("*.csv"))
        scored.append((len(files), directory))
    if not scored:
        raise FileNotFoundError(f"未找到本地日线数据目录: {base / 'data'}")
    return sorted(scored, key=lambda item: item[0], reverse=True)[0][1]


def list_daily_files(data_dir: str | Path, limit: int = 100) -> list[Path]:
    directory = Path(data_dir)
    parquet = sorted(directory.glob("*_daily.parquet"))
    if not parquet:
        parquet = sorted(directory.glob("*.parquet"))
    csv = sorted(directory.glob("*.csv"))
    files = parquet + csv
    if limit > 0:
        files = files[:limit]
    return files


def build_panel_dataset(
    files: Iterable[str | Path],
    *,
    target_horizon: int = 3,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    for path_like in files:
        path = Path(path_like)
        try:
            frame = _read_daily_file(path)
            symbol = _symbol_from_path(path)
            factors = build_daily_factors(frame, symbol=symbol, target_horizon=target_horizon)
            if not factors.empty:
                frames.append(factors)
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")
    if not frames:
        raise RuntimeError(f"没有成功构建任何日线因子表，错误样例: {errors[:5]}")
    panel = pd.concat(frames, ignore_index=True)
    panel["datetime"] = pd.to_datetime(panel["datetime"], errors="coerce")
    panel = panel.replace([np.inf, -np.inf], np.nan)
    panel = panel.dropna(subset=["datetime", "label", "future_max_return"])
    numeric_cols = panel.select_dtypes(include=[np.number]).columns
    panel[numeric_cols] = panel[numeric_cols].ffill().fillna(0.0)
    panel = reduce_panel_memory(panel)
    return panel.sort_values(["datetime", "symbol"]).reset_index(drop=True)


def reduce_panel_memory(panel: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric panel columns for full-market training."""
    out = panel.copy()
    for col in out.select_dtypes(include=["float64"]).columns:
        out[col] = pd.to_numeric(out[col], downcast="float")
    for col in out.select_dtypes(include=["int64"]).columns:
        if col == "label":
            out[col] = out[col].astype("int8")
        else:
            out[col] = pd.to_numeric(out[col], downcast="integer")
    return out


def global_time_series_split(
    panel: pd.DataFrame,
    train_ratio: float = 0.8,
    target_horizon: int = 3,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, list[str], pd.Timestamp]:
    """Split by one absolute chronological cutoff date across all symbols.

    The training tail is purged by target_horizon trading days because labels
    use future highs. This prevents labels immediately before the cutoff from
    peeking into the out-of-sample period.
    """
    if not 0.5 < train_ratio < 0.95:
        raise ValueError("train_ratio must be between 0.5 and 0.95")
    frame = panel.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    frame = frame.dropna(subset=["datetime", "label"]).sort_values(["datetime", "symbol"]).reset_index(drop=True)
    unique_dates = pd.Series(frame["datetime"].dt.normalize().drop_duplicates().sort_values().to_numpy())
    if len(unique_dates) < 10:
        raise ValueError("有效交易日过少，无法做严格时序切分")
    cutoff_idx = int(len(unique_dates) * train_ratio)
    cutoff_idx = max(1, min(cutoff_idx, len(unique_dates) - 1))
    split_date = pd.Timestamp(unique_dates.iloc[cutoff_idx])
    train_end_idx = max(1, cutoff_idx - max(0, int(target_horizon)))
    train_end_date = pd.Timestamp(unique_dates.iloc[train_end_idx])

    train = frame[frame["datetime"] < train_end_date].copy()
    test = frame[frame["datetime"] >= split_date].copy()
    if train.empty or test.empty:
        raise ValueError(f"切分后训练集或测试集为空: split_date={split_date.date()}")

    cols = feature_columns(frame)
    X_train = _clean_features(train[cols])
    y_train = train["label"].astype(int)
    X_test = _clean_features(test[cols])
    y_test = test["label"].astype(int)
    return X_train, y_train, X_test, y_test, cols, split_date


def train_daily_global_model(
    panel: pd.DataFrame,
    *,
    train_ratio: float = 0.8,
    target_horizon: int = 3,
    model_path: str | Path = MODEL_PATH,
    meta_path: str | Path = META_PATH,
) -> PanelTrainResult:
    if XGBClassifier is None:
        raise RuntimeError(
            f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}. 请先执行 pip install xgboost"
        )
    X_train, y_train, X_test, y_test, cols, split_date = global_time_series_split(
        panel,
        train_ratio=train_ratio,
        target_horizon=target_horizon,
    )
    if y_train.nunique() < 2:
        raise ValueError("训练集 label 只有单一类别，无法训练 XGBoost 二分类器")

    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    scale_pos_weight = neg / max(1, pos)

    model = XGBClassifier(
        max_depth=5,
        learning_rate=0.05,
        n_estimators=500,
        subsample=0.8,
        colsample_bytree=0.85,
        min_child_weight=3,
        reg_lambda=2.0,
        objective="binary:logistic",
        eval_metric="logloss",
        early_stopping_rounds=40,
        scale_pos_weight=scale_pos_weight,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    metrics = classification_metrics(y_test.to_numpy(), probabilities, predictions)
    importance = top_feature_importance(model, cols)

    model_path = Path(model_path)
    meta_path = Path(meta_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    meta = {
        "feature_columns": cols,
        "feature_importance_top15": importance,
        "metrics": metrics,
        "split_date": split_date.date().isoformat(),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "purged_target_horizon_days": int(target_horizon),
        "train_positive_rows": pos,
        "train_negative_rows": neg,
        "scale_pos_weight": scale_pos_weight,
        "model_path": str(model_path),
        "created_at": pd.Timestamp.now().isoformat(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return PanelTrainResult(model, model_path, meta_path, cols, split_date, importance, metrics)


def classification_metrics(y_true: np.ndarray, probabilities: np.ndarray, predictions: np.ndarray) -> dict[str, float | int]:
    y_true = y_true.astype(int)
    predictions = predictions.astype(int)
    tp = int(((predictions == 1) & (y_true == 1)).sum())
    fp = int(((predictions == 1) & (y_true == 0)).sum())
    tn = int(((predictions == 0) & (y_true == 0)).sum())
    fn = int(((predictions == 0) & (y_true == 1)).sum())
    precision = tp / max(1, tp + fp)
    recall = tp / max(1, tp + fn)
    accuracy = (tp + tn) / max(1, len(y_true))
    positive_rate = float(y_true.mean()) if len(y_true) else 0.0
    return {
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "accuracy": round(accuracy, 6),
        "positive_rate": round(positive_rate, 6),
        "avg_pred_prob": round(float(np.mean(probabilities)), 6),
        "max_pred_prob": round(float(np.max(probabilities)), 6),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
    }


def top_feature_importance(model: Any, cols: list[str], topn: int = 15) -> dict[str, float]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    pairs = sorted(zip(cols, [float(v) for v in values]), key=lambda item: item[1], reverse=True)
    return {name: round(value, 8) for name, value in pairs[:topn]}


def _read_daily_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _symbol_from_path(path: Path) -> str:
    stem = path.stem.replace("_daily", "")
    return stem[-6:] if len(stem) >= 6 else stem


def _clean_features(features: pd.DataFrame) -> pd.DataFrame:
    out = features.apply(pd.to_numeric, errors="coerce")
    out = out.replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0)
    return out.astype("float32", copy=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train global daily XGBoost panel model")
    parser.add_argument("--data-dir", default="", help="Daily kline directory. Auto-detected if omitted.")
    parser.add_argument("--limit", type=int, default=100, help="Number of symbols to train on. Use 0 for full market.")
    parser.add_argument("--target-horizon", type=int, default=3)
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    args = parser.parse_args()

    start = time.perf_counter()
    data_dir = Path(args.data_dir) if args.data_dir else discover_daily_data_dir()
    files = list_daily_files(data_dir, limit=args.limit)
    if not files:
        raise FileNotFoundError(f"目录内没有日线 csv/parquet 文件: {data_dir}")

    print("========== Daily Global XGBoost Trainer ==========")
    print(f"Detected Data Dir  : {data_dir}")
    print(f"Selected Symbols   : {len(files)}")
    print(f"First File         : {files[0].name}")
    panel = build_panel_dataset(files, target_horizon=args.target_horizon)
    print(f"Panel Shape        : {panel.shape}")
    print(f"Date Range         : {panel['datetime'].min().date()} -> {panel['datetime'].max().date()}")
    print(f"Positive Rate      : {panel['label'].mean():.4f}")

    result = train_daily_global_model(
        panel,
        train_ratio=args.train_ratio,
        target_horizon=args.target_horizon,
        model_path=args.model_path,
        meta_path=args.meta_path,
    )
    elapsed = time.perf_counter() - start

    print(f"Split Date         : {result.split_date.date()}")
    print(f"Feature Count      : {len(result.feature_columns)}")
    print(f"Model Path         : {result.model_path}")
    print(f"Meta Path          : {result.meta_path}")
    print(f"Elapsed Seconds    : {elapsed:.3f}")
    print("Out-of-Sample Metrics:")
    print(f"  Precision        : {result.metrics['precision']:.4f}")
    print(f"  Recall           : {result.metrics['recall']:.4f}")
    print(f"  Accuracy         : {result.metrics['accuracy']:.4f}")
    print(f"  Positive Rate    : {result.metrics['positive_rate']:.4f}")
    print(f"  Confusion        : TP={result.metrics['tp']} FP={result.metrics['fp']} TN={result.metrics['tn']} FN={result.metrics['fn']}")
    print("Top 15 Feature Importance:")
    for name, value in result.feature_importance_top15.items():
        print(f"  {name}: {value:.6f}")
    print("=================================================")


if __name__ == "__main__":
    main()
