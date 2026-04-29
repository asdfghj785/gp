from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - environment guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.config import BASE_DIR, MODELS_DIR
from quant_core.engine.factor_factory import build_features_for_ticker


MODEL_PATH = MODELS_DIR / "xgboost_swing_v1.json"
META_PATH = MODELS_DIR / "xgboost_swing_v1.meta.json"
NON_FEATURE_COLS = {
    "datetime",
    "date",
    "time",
    "symbol",
    "code",
    "name",
    "future_return",
    "label",
}


@dataclass(frozen=True)
class TrainResult:
    model: Any
    feature_columns: list[str]
    feature_importance_top15: dict[str, float]
    metrics: dict[str, float | int]
    model_path: Path
    meta_path: Path


def time_series_split(
    df: pd.DataFrame,
    train_ratio: float = 0.8,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, list[str]]:
    """Chronological train/test split.

    No shuffle is allowed here. The first 80% of rows are train by default,
    the last 20% are held out to avoid future leakage.
    """
    if not 0.5 < train_ratio < 0.95:
        raise ValueError("train_ratio must be between 0.5 and 0.95")
    if "label" not in df.columns:
        raise ValueError("DataFrame must contain label")

    frame = df.copy()
    if "datetime" in frame.columns:
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.sort_values("datetime")
    frame = frame.reset_index(drop=True)

    feature_columns = _feature_columns(frame)
    if not feature_columns:
        raise ValueError("No numeric feature columns found")

    split_idx = int(len(frame) * train_ratio)
    split_idx = max(1, min(split_idx, len(frame) - 1))

    X = frame[feature_columns].replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0)
    y = frame["label"].astype(int)
    return X.iloc[:split_idx], y.iloc[:split_idx], X.iloc[split_idx:], y.iloc[split_idx:], feature_columns


def train_xgboost_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    *,
    model_path: str | Path = MODEL_PATH,
    meta_path: str | Path = META_PATH,
    feature_columns: list[str] | None = None,
) -> TrainResult:
    if XGBClassifier is None:
        raise RuntimeError(
            f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}. 请先执行 pip install xgboost"
        )
    if y_train.nunique() < 2:
        raise ValueError("训练集 label 只有单一类别，无法训练二分类 XGBoost")

    model_path = Path(model_path)
    meta_path = Path(meta_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    pos_count = int((y_train == 1).sum())
    neg_count = int((y_train == 0).sum())
    scale_pos_weight = neg_count / max(1, pos_count)

    model = XGBClassifier(
        max_depth=5,
        learning_rate=0.05,
        n_estimators=300,
        subsample=0.8,
        colsample_bytree=0.85,
        min_child_weight=2,
        reg_lambda=1.5,
        objective="binary:logistic",
        eval_metric="logloss",
        early_stopping_rounds=30,
        scale_pos_weight=scale_pos_weight,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    metrics = _classification_metrics(y_test.to_numpy(), probabilities, predictions)

    columns = feature_columns or list(X_train.columns)
    feature_importance = _top_feature_importance(model, columns, topn=15)

    model.save_model(str(model_path))
    meta = {
        "feature_columns": columns,
        "feature_importance_top15": feature_importance,
        "metrics": metrics,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "positive_train_rows": pos_count,
        "negative_train_rows": neg_count,
        "scale_pos_weight": scale_pos_weight,
        "model_path": str(model_path),
        "created_at": pd.Timestamp.now().isoformat(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return TrainResult(model, columns, feature_importance, metrics, model_path, meta_path)


def load_model(model_path: str | Path = MODEL_PATH):
    if XGBClassifier is None:
        raise RuntimeError(
            f"xgboost 未安装或不可导入：{XGBOOST_IMPORT_ERROR}. 请先执行 pip install xgboost"
        )
    model = XGBClassifier()
    model.load_model(str(model_path))
    return model


def load_metadata(meta_path: str | Path = META_PATH) -> dict[str, Any]:
    path = Path(meta_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_model_and_metadata(
    model_path: str | Path = MODEL_PATH,
    meta_path: str | Path = META_PATH,
) -> tuple[Any, dict[str, Any]]:
    return load_model(model_path), load_metadata(meta_path)


def predict_prob(
    model: Any,
    current_features: pd.DataFrame | pd.Series | dict[str, Any],
    feature_columns: list[str] | None = None,
) -> np.ndarray:
    """Return probability of label=1 for one row or a batch."""
    if isinstance(current_features, pd.Series):
        frame = current_features.to_frame().T
    elif isinstance(current_features, dict):
        frame = pd.DataFrame([current_features])
    else:
        frame = current_features.copy()

    if feature_columns is None:
        feature_columns = _model_feature_names(model) or _feature_columns(frame)
    if not feature_columns:
        raise ValueError("feature_columns is required when model has no embedded feature names")

    aligned = _align_features(frame, feature_columns)
    probabilities = model.predict_proba(aligned)[:, 1]
    return np.asarray(probabilities, dtype=float)


def train_from_factor_frame(
    factor_df: pd.DataFrame,
    *,
    train_ratio: float = 0.8,
    model_path: str | Path = MODEL_PATH,
    meta_path: str | Path = META_PATH,
) -> TrainResult:
    X_train, y_train, X_test, y_test, features = time_series_split(factor_df, train_ratio=train_ratio)
    return train_xgboost_model(
        X_train,
        y_train,
        X_test,
        y_test,
        model_path=model_path,
        meta_path=meta_path,
        feature_columns=features,
    )


def _feature_columns(df: pd.DataFrame) -> list[str]:
    return [
        col
        for col in df.columns
        if col not in NON_FEATURE_COLS and pd.api.types.is_numeric_dtype(df[col])
    ]


def _align_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in feature_columns:
        if col not in out.columns:
            out[col] = 0.0
    out = out[feature_columns]
    out = out.apply(pd.to_numeric, errors="coerce")
    return out.replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0)


def _model_feature_names(model: Any) -> list[str]:
    try:
        names = model.get_booster().feature_names
    except Exception:
        return []
    return list(names or [])


def _top_feature_importance(model: Any, feature_columns: list[str], topn: int = 15) -> dict[str, float]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    pairs = sorted(
        zip(feature_columns, [float(v) for v in values]),
        key=lambda item: item[1],
        reverse=True,
    )
    return {name: round(value, 8) for name, value in pairs[:topn]}


def _classification_metrics(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    predictions: np.ndarray,
) -> dict[str, float | int]:
    y_true = y_true.astype(int)
    predictions = predictions.astype(int)
    tp = int(((predictions == 1) & (y_true == 1)).sum())
    fp = int(((predictions == 1) & (y_true == 0)).sum())
    tn = int(((predictions == 0) & (y_true == 0)).sum())
    fn = int(((predictions == 0) & (y_true == 1)).sum())
    total = max(1, len(y_true))
    precision = tp / max(1, tp + fp)
    recall = tp / max(1, tp + fn)
    accuracy = (tp + tn) / total
    positive_rate = float(y_true.mean()) if total else 0.0
    return {
        "accuracy": round(accuracy, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "positive_rate": round(positive_rate, 6),
        "avg_pred_prob": round(float(np.mean(probabilities)), 6),
        "max_pred_prob": round(float(np.max(probabilities)), 6),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
    }


def _default_test_path() -> Path:
    candidates = [
        BASE_DIR / "data" / "min_kline" / "5m" / "sh600000.parquet",
        BASE_DIR / "data" / "min_kline" / "5m" / "600000.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("未找到 sh600000 或 600000 的 5m Parquet 测试文件")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train XGBoost swing classifier from Alpha Factor Factory")
    parser.add_argument("--data", default="", help="5m Parquet path. Default: sh600000 sample")
    parser.add_argument("--target-horizon", type=int, default=48, help="Future bars used for label generation")
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    args = parser.parse_args()

    data_path = Path(args.data) if args.data else _default_test_path()
    start = time.perf_counter()
    factor_df = build_features_for_ticker(data_path, target_horizon=args.target_horizon)
    result = train_from_factor_frame(
        factor_df,
        train_ratio=args.train_ratio,
        model_path=args.model_path,
        meta_path=args.meta_path,
    )
    elapsed = time.perf_counter() - start

    print("========== XGBoost Swing Trainer ==========")
    print(f"Data Path          : {data_path}")
    print(f"Target Horizon     : {args.target_horizon} bars")
    print(f"Rows               : {len(factor_df)}")
    print(f"Features           : {len(result.feature_columns)}")
    print(f"Model Path         : {result.model_path}")
    print(f"Meta Path          : {result.meta_path}")
    print(f"Elapsed Seconds    : {elapsed:.3f}")
    print("Metrics            :")
    for key, value in result.metrics.items():
        print(f"  {key}: {value}")
    print("Top 15 Features    :")
    for name, value in result.feature_importance_top15.items():
        print(f"  {name}: {value:.6f}")
    latest_prob = float(predict_prob(result.model, factor_df.tail(1), result.feature_columns)[0])
    print(f"Latest Probability : {latest_prob:.4f}")
    print("==========================================")


if __name__ == "__main__":
    main()
