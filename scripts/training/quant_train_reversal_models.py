from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, r2_score

from build_reversal_dataset import OUTPUT_PATH, REVERSAL_FEATURE_COLS
from quant_core.config import BASE_DIR, REVERSAL_MODEL_PATH


MODEL_PATH = REVERSAL_MODEL_PATH
DATASET_PATH = OUTPUT_PATH
DRAWDOWN_PENALTY_PCT = -8.0
PENALIZED_TARGET_PCT = -2.0


def train() -> dict[str, Any]:
    df = pd.read_parquet(DATASET_PATH).sort_values(["date", "code"]).reset_index(drop=True)
    required_cols = [*REVERSAL_FEATURE_COLS, "t3_max_gain_pct", "t3_max_drawdown_pct"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)

    df = df[df["t3_max_gain_pct"].between(-20, 80)].copy()
    df = df[df["t3_max_drawdown_pct"].between(-30, 20)].copy()
    if len(df) < 300:
        raise RuntimeError(f"中线反转训练样本过少: {len(df)}，不建议训练模型。")

    split_index = int(len(df) * 0.8)
    train_df = df.iloc[:split_index].copy()
    test_df = df.iloc[split_index:].copy()

    y_train_actual = train_df["t3_max_gain_pct"].clip(-5, 35)
    y_train = y_train_actual.copy()
    train_risky = train_df["t3_max_drawdown_pct"] <= DRAWDOWN_PENALTY_PCT
    y_train.loc[train_risky] = PENALIZED_TARGET_PCT
    y_test_actual = test_df["t3_max_gain_pct"].clip(-5, 35)

    x_train = train_df[REVERSAL_FEATURE_COLS]
    x_test = test_df[REVERSAL_FEATURE_COLS]

    regressor = xgb.XGBRegressor(
        n_estimators=220,
        learning_rate=0.04,
        max_depth=2,
        subsample=0.82,
        colsample_bytree=0.85,
        min_child_weight=20,
        reg_lambda=5.0,
        reg_alpha=0.5,
        objective="reg:squarederror",
        random_state=47,
        n_jobs=4,
    )
    regressor.fit(x_train, y_train)

    _backup(MODEL_PATH)
    regressor.save_model(str(MODEL_PATH))

    pred = regressor.predict(x_test)
    test_eval = test_df[["code", "name", "date", "t3_max_gain_pct", "t3_max_drawdown_pct", "next_open_premium"]].copy()
    test_eval["predicted_t3_max_gain_pct"] = pred
    top_10 = test_eval.sort_values("predicted_t3_max_gain_pct", ascending=False).head(max(1, int(len(test_eval) * 0.10)))
    top_5 = test_eval.sort_values("predicted_t3_max_gain_pct", ascending=False).head(max(1, int(len(test_eval) * 0.05)))
    top_1 = test_eval.sort_values("predicted_t3_max_gain_pct", ascending=False).head(max(1, int(len(test_eval) * 0.01)))

    result = {
        "dataset_path": str(DATASET_PATH),
        "model_path": str(MODEL_PATH),
        "model_type": "XGBRegressor",
        "target": "t3_max_gain_pct",
        "penalty_rule": f"if t3_max_drawdown_pct <= {DRAWDOWN_PENALTY_PCT:.1f} then train target = {PENALIZED_TARGET_PCT:.1f}",
        "dataset_rows": int(len(df)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "penalized_train_rows": int(train_risky.sum()),
        "feature_count": len(REVERSAL_FEATURE_COLS),
        "features": REVERSAL_FEATURE_COLS,
        "test_mae": round(float(mean_absolute_error(y_test_actual, pred)), 4),
        "test_r2": round(float(r2_score(y_test_actual, pred)), 4),
        "test_avg_t3_max_gain_pct": round(float(test_eval["t3_max_gain_pct"].mean()), 4),
        "test_positive_rate_pct": round(float((test_eval["t3_max_gain_pct"] > 0).mean() * 100), 4),
        "top_10pct_rows": int(len(top_10)),
        "top_10pct_actual_avg_t3_max_gain_pct": round(float(top_10["t3_max_gain_pct"].mean()), 4),
        "top_10pct_win_rate_pct": round(float((top_10["t3_max_gain_pct"] > 0).mean() * 100), 4),
        "top_10pct_avg_drawdown_pct": round(float(top_10["t3_max_drawdown_pct"].mean()), 4),
        "top_5pct_actual_avg_t3_max_gain_pct": round(float(top_5["t3_max_gain_pct"].mean()), 4),
        "top_5pct_win_rate_pct": round(float((top_5["t3_max_gain_pct"] > 0).mean() * 100), 4),
        "top_1pct_actual_avg_t3_max_gain_pct": round(float(top_1["t3_max_gain_pct"].mean()), 4),
        "top_1pct_win_rate_pct": round(float((top_1["t3_max_gain_pct"] > 0).mean() * 100), 4),
        "top_10pct_examples": top_10.head(10).to_dict(orient="records"),
    }
    return result


def format_training_report(result: dict[str, Any]) -> str:
    lines = [
        "# 中线超跌反转 T+3 回归模型训练报告",
        "",
        f"- 数据集：{result['dataset_path']}",
        f"- 模型：{result['model_path']}",
        f"- 样本数：{result['dataset_rows']}（训练 {result['train_rows']} / 验证 {result['test_rows']}）",
        f"- 特征数：{result['feature_count']}",
        f"- 避险惩罚：{result['penalty_rule']}，训练集中触发 {result['penalized_train_rows']} 条",
        f"- 验证集 MAE：{result['test_mae']:.4f}",
        f"- 验证集 R2：{result['test_r2']:.4f}",
        f"- 验证集整体 T+3 平均最大涨幅：{result['test_avg_t3_max_gain_pct']:.2f}%",
        f"- 验证集整体胜率（T+3最大涨幅>0）：{result['test_positive_rate_pct']:.2f}%",
        "",
        "## 预测得分 Top 分层",
        "",
        "| 分层 | 实际T+3平均最大涨幅 | 胜率(T+3最大涨幅>0) |",
        "|---|---:|---:|",
        f"| Top 10% | {result['top_10pct_actual_avg_t3_max_gain_pct']:.2f}% | {result['top_10pct_win_rate_pct']:.2f}% |",
        f"| Top 5% | {result['top_5pct_actual_avg_t3_max_gain_pct']:.2f}% | {result['top_5pct_win_rate_pct']:.2f}% |",
        f"| Top 1% | {result['top_1pct_actual_avg_t3_max_gain_pct']:.2f}% | {result['top_1pct_win_rate_pct']:.2f}% |",
    ]
    examples = result.get("top_10pct_examples") or []
    if examples:
        lines.extend(["", "## Top 10% 样例", "", "| 日期 | 代码 | 名称 | 实际T+3最大涨幅 | T+3最大回撤 | 预测分 |", "|---|---:|---|---:|---:|---:|"])
        for item in examples:
            lines.append(
                f"| {item.get('date')} | {item.get('code')} | {item.get('name')} | "
                f"{float(item.get('t3_max_gain_pct') or 0):.2f}% | "
                f"{float(item.get('t3_max_drawdown_pct') or 0):.2f}% | "
                f"{float(item.get('predicted_t3_max_gain_pct') or 0):.2f}% |"
            )
    return "\n".join(lines)


def _backup(path: Path) -> None:
    if not path.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(path, path.with_suffix(path.suffix + f".bak_{stamp}"))


if __name__ == "__main__":
    training_result = train()
    print(format_training_report(training_result))
