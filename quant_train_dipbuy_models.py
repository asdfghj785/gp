from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error

from quant_core.config import BASE_DIR, BREAKOUT_HIGH_TARGET_PCT, DIPBUY_PREMIUM_MODEL_PATH, PROFIT_TARGET_PCT
from quant_core.predictor import DIPBUY_FEATURE_COLS


DATASET_PATH = BASE_DIR / "data" / "ml_dataset" / "smart_overnight_data.parquet"
FEE_BUFFER_PCT = PROFIT_TARGET_PCT
DIPBUY_FILTERS = {
    "min_5d_high_gain": 15.0,
    "min_intraday_flush": -9.5,
    "max_intraday_flush": -4.0,
    "bias10_low": -3.0,
    "bias10_high": 3.0,
    "max_amount_shrink_pct": 0.0,
}


def train() -> dict[str, object]:
    df = pd.read_parquet(DATASET_PATH).sort_values("date").reset_index(drop=True)
    for col in [*DIPBUY_FEATURE_COLS, "next_day_premium", "next_day_high_premium"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)

    if "next_day_high_premium" not in df.columns:
        df["next_day_high_premium"] = df["next_day_premium"]
    df = df[df["next_day_premium"].between(-15, 15)].copy()
    dipbuy_mask = (
        (df["近5日最高涨幅"] > DIPBUY_FILTERS["min_5d_high_gain"])
        & (df["今日急跌度"] > DIPBUY_FILTERS["min_intraday_flush"])
        & (df["今日急跌度"] < DIPBUY_FILTERS["max_intraday_flush"])
        & (df["10日均线乖离率"].between(DIPBUY_FILTERS["bias10_low"], DIPBUY_FILTERS["bias10_high"]))
        & (df["今日缩量比例"] < DIPBUY_FILTERS["max_amount_shrink_pct"])
    )
    df = df[dipbuy_mask].copy()
    if len(df) < 500:
        raise RuntimeError(f"首阴低吸样本过少: {len(df)}，请先检查训练集特征或放宽物理过滤。")

    split_index = int(len(df) * 0.8)
    train_df = df.iloc[:split_index].copy()
    test_df = df.iloc[split_index:].copy()

    x_train = train_df[DIPBUY_FEATURE_COLS]
    x_test = test_df[DIPBUY_FEATURE_COLS]
    train_guillotine = pd.to_numeric(train_df.get("近3日断头铡刀标记", 0), errors="coerce").fillna(0) >= 0.5
    test_guillotine = pd.to_numeric(test_df.get("近3日断头铡刀标记", 0), errors="coerce").fillna(0) >= 0.5
    y_train_actual = train_df["next_day_premium"].clip(-10, 8)
    y_train_reg = y_train_actual
    y_test_reg = test_df["next_day_premium"].clip(-8, 8)

    regressor = xgb.XGBRegressor(
        n_estimators=360,
        learning_rate=0.025,
        max_depth=3,
        subsample=0.80,
        colsample_bytree=0.82,
        min_child_weight=8,
        reg_lambda=2.6,
        reg_alpha=0.20,
        objective="reg:squarederror",
        random_state=43,
        n_jobs=4,
    )
    regressor.fit(x_train, y_train_reg)

    _backup(DIPBUY_PREMIUM_MODEL_PATH)
    regressor.save_model(str(DIPBUY_PREMIUM_MODEL_PATH))

    pred_reg = regressor.predict(x_test)
    test_eval = test_df[["next_day_premium", "next_day_high_premium"]].copy()
    test_eval["predicted_premium"] = pred_reg
    top_decile = test_eval.sort_values("predicted_premium", ascending=False).head(max(1, len(test_eval) // 10))
    top_percent = test_eval.sort_values("predicted_premium", ascending=False).head(max(1, len(test_eval) // 100))
    positive_rule = (test_eval["next_day_premium"] > FEE_BUFFER_PCT) | (test_eval["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    top_decile_positive = (top_decile["next_day_premium"] > FEE_BUFFER_PCT) | (top_decile["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    top_percent_positive = (top_percent["next_day_premium"] > FEE_BUFFER_PCT) | (top_percent["next_day_high_premium"] > BREAKOUT_HIGH_TARGET_PCT)
    return {
        "dataset_rows": int(len(df)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "features": DIPBUY_FEATURE_COLS,
        "model_type": "XGBRegressor",
        "strategy_type": "首阴低吸",
        "physical_filters": DIPBUY_FILTERS,
        "fee_buffer_pct": FEE_BUFFER_PCT,
        "breakout_high_target_pct": BREAKOUT_HIGH_TARGET_PCT,
        "target_rule": "predict true next_day_premium; dip-buy keeps guillotine samples unmodified because sharp drops are part of the setup",
        "guillotine_train_rows_observed": int(train_guillotine.sum()),
        "guillotine_test_rows_observed": int(test_guillotine.sum()),
        "test_positive_rate_pct": round(float(positive_rule.mean() * 100), 4),
        "mae_premium": round(float(mean_absolute_error(y_test_reg, pred_reg)), 4),
        "top_decile_rows": int(len(top_decile)),
        "top_decile_avg_open_premium": round(float(top_decile["next_day_premium"].mean()), 4),
        "top_decile_positive_rate_pct": round(float(top_decile_positive.mean() * 100), 4),
        "top_1pct_rows": int(len(top_percent)),
        "top_1pct_avg_open_premium": round(float(top_percent["next_day_premium"].mean()), 4),
        "top_1pct_positive_rate_pct": round(float(top_percent_positive.mean() * 100), 4),
        "premium_model_path": str(DIPBUY_PREMIUM_MODEL_PATH),
    }


def _backup(path: Path) -> None:
    if not path.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(path, path.with_suffix(path.suffix + f".bak_{stamp}"))


if __name__ == "__main__":
    result = train()
    for key, value in result.items():
        print(f"{key}: {value}")
