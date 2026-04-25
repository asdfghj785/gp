from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error

from quant_core.config import BASE_DIR, BREAKOUT_HIGH_TARGET_PCT, MODEL_PATH, PREMIUM_MODEL_PATH
from quant_core.predictor import FEATURE_COLS, PROFIT_TARGET_PCT


DATASET_PATH = BASE_DIR / "data" / "ml_dataset" / "smart_overnight_data.parquet"
FEE_BUFFER_PCT = PROFIT_TARGET_PCT


def train() -> dict[str, object]:
    df = pd.read_parquet(DATASET_PATH).sort_values("date").reset_index(drop=True)
    df = df.dropna(subset=FEATURE_COLS + ["next_day_premium"]).copy()
    if "next_day_high_premium" not in df.columns:
        df["next_day_high_premium"] = df["next_day_premium"]
    df = df[df["next_day_premium"].between(-15, 15)].copy()
    split_index = int(len(df) * 0.8)
    train_df = df.iloc[:split_index].copy()
    test_df = df.iloc[split_index:].copy()

    x_train = train_df[FEATURE_COLS]
    x_test = test_df[FEATURE_COLS]
    train_guillotine = pd.to_numeric(train_df.get("近3日断头铡刀标记", 0), errors="coerce").fillna(0) >= 0.5
    test_guillotine = pd.to_numeric(test_df.get("近3日断头铡刀标记", 0), errors="coerce").fillna(0) >= 0.5
    y_train_actual = train_df["next_day_premium"].clip(-10, 8)
    y_train_reg = y_train_actual.where(~train_guillotine, y_train_actual.clip(upper=-1.0))
    y_test_reg = test_df["next_day_premium"].clip(-8, 8)

    regressor = xgb.XGBRegressor(
        n_estimators=420,
        learning_rate=0.028,
        max_depth=4,
        subsample=0.82,
        colsample_bytree=0.82,
        min_child_weight=10,
        reg_lambda=2.2,
        reg_alpha=0.15,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=4,
    )
    regressor.fit(x_train, y_train_reg)

    _backup(MODEL_PATH)
    _backup(PREMIUM_MODEL_PATH)
    regressor.save_model(str(PREMIUM_MODEL_PATH))

    pred_reg = regressor.predict(x_test)
    test_eval = test_df[["next_day_premium", "next_day_high_premium"]].copy()
    test_eval["predicted_premium"] = pred_reg
    top_decile = test_eval.sort_values("predicted_premium", ascending=False).head(max(1, len(test_eval) // 10))
    top_percent = test_eval.sort_values("predicted_premium", ascending=False).head(max(1, len(test_eval) // 100))
    positive_rule = (test_eval["next_day_premium"] > FEE_BUFFER_PCT) | (test_eval["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    top_decile_positive = (top_decile["next_day_premium"] > FEE_BUFFER_PCT) | (top_decile["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    top_percent_positive = (top_percent["next_day_premium"] > FEE_BUFFER_PCT) | (top_percent["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    metrics = {
        "dataset_rows": int(len(df)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "features": FEATURE_COLS,
        "model_type": "XGBRegressor",
        "fee_buffer_pct": FEE_BUFFER_PCT,
        "breakout_high_target_pct": BREAKOUT_HIGH_TARGET_PCT,
        "target_rule": "predict next_day_premium; if recent 3-day guillotine flag is true, training target is capped at -1.0%",
        "penalized_train_rows": int(train_guillotine.sum()),
        "penalized_test_rows": int(test_guillotine.sum()),
        "test_positive_rate_pct": round(float(positive_rule.mean() * 100), 4),
        "mae_premium": round(float(mean_absolute_error(y_test_reg, pred_reg)), 4),
        "top_decile_rows": int(len(top_decile)),
        "top_decile_avg_open_premium": round(float(top_decile["next_day_premium"].mean()), 4),
        "top_decile_positive_rate_pct": round(float(top_decile_positive.mean() * 100), 4),
        "top_1pct_rows": int(len(top_percent)),
        "top_1pct_avg_open_premium": round(float(top_percent["next_day_premium"].mean()), 4),
        "top_1pct_positive_rate_pct": round(float(top_percent_positive.mean() * 100), 4),
        "model_path": "unused_regression_only",
        "premium_model_path": str(PREMIUM_MODEL_PATH),
    }
    return metrics


def _backup(path: Path) -> None:
    if not path.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(path, path.with_suffix(path.suffix + f".bak_{stamp}"))


if __name__ == "__main__":
    result = train()
    for key, value in result.items():
        print(f"{key}: {value}")
