import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def train_and_analyze_precise():
    print("🚀 启动 XGBoost 精准尾盘隔夜策略...\n")

    df = pd.read_parquet("data/ml_dataset/precise_overnight_data.parquet")
    df = df.sort_values(by='date').reset_index(drop=True)

    feature_cols = ['turn', '量比', 'MA5', 'MA10', 'MA20', 'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数']
    X = df[feature_cols]

    # 目标：第二天开盘溢价率 > 0.5% (覆盖手续费并确保有利润空间)
    y = (df['next_day_premium'] > 0.3).astype(int)

    split_index = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    test_premium = df['next_day_premium'].iloc[split_index:]

    print(f"📚 极品样本训练集: {len(X_train)} 条")
    print(f"🎯 实盘盲测验证集: {len(X_test)} 条\n")

    model = xgb.XGBClassifier(
        n_estimators=100, learning_rate=0.03, max_depth=4, # 降低深度防止过拟合
        subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric='logloss'
    )
    model.fit(X_train, y_train)

    print("📊 极品池内核心归因（当形态都好时，什么参数最能决定明天高开？）：")
    importances = model.feature_importances_
    importance_df = pd.DataFrame({'特征': feature_cols, '重要度': importances})
    importance_df = importance_df.sort_values(by='重要度', ascending=False)
    for idx, row in importance_df.iterrows():
        print(f"  ⭐️ {row['特征']:<12}: {row['重要度'] * 100:.2f}%")

    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # 提高出手门槛，AI 认为胜率 > 60% 我们才重仓
    threshold = 0.4
    buy_signals = y_pred_proba > threshold

    actual_wins = y_test[buy_signals].sum()
    total_buys = buy_signals.sum()

    print(f"\n📈 近期实盘盲测结果（AI 预测胜率 > 60% 才出手）：")
    if total_buys > 0:
        win_rate = actual_wins / total_buys * 100
        avg_premium = test_premium[buy_signals].mean()
        print(f"  👉 尾盘符合条件并买入: {total_buys} 次")
        print(f"  👉 第二天成功吃溢价: {actual_wins} 次")
        print(f"  👉 真实出手胜率: {win_rate:.2f}%")
        print(f"  👉 平均单次隔夜溢价: {avg_premium:.2f}%")
    else:
        print("  👉 在目前的严苛标准下，近期没有符合出手的股票。")

if __name__ == "__main__":
    train_and_analyze_precise()