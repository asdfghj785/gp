import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def train_and_analyze_overnight():
    print("🚀 启动 XGBoost 尾盘隔夜溢价分析模型...\n")

    df = pd.read_parquet("data/ml_dataset/overnight_strategy_data.parquet")
    df = df.sort_values(by='date').reset_index(drop=True)

    # 定义特征和目标
    feature_cols = ['turn', '量比', 'MA5', 'MA10', 'MA20', 'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数']
    X = df[feature_cols]

    # 目标：第二天开盘只要比今天收盘价高（溢价率 > 0.2%，扣除手续费），就算成功
    y = (df['next_day_premium'] > 0.2).astype(int)

    # 按时间划分 70% 训练，30% 回测
    split_index = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    test_premium = df['next_day_premium'].iloc[split_index:]

    print(f"📚 训练集大小: {len(X_train)} 条")
    print(f"🎯 测试集大小: {len(X_test)} 条\n")

    model = xgb.XGBClassifier(
        n_estimators=150, learning_rate=0.05, max_depth=5,
        subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric='logloss'
    )
    model.fit(X_train, y_train)

    # ================= 分析一：归因分析 =================
    print("📊 尾盘隔夜策略核心归因（影响明天高开的最重要参数）：")
    importances = model.feature_importances_
    importance_df = pd.DataFrame({'特征': feature_cols, '重要度': importances})
    importance_df = importance_df.sort_values(by='重要度', ascending=False)
    for idx, row in importance_df.iterrows():
        print(f"  ⭐️ {row['特征']:<12}: {row['重要度'] * 100:.2f}%")

    # ================= 分析二：模拟胜率 =================
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # 策略：AI 认为胜率大于 65% 的我们才在尾盘潜伏
    threshold = 0.65
    buy_signals = y_pred_proba > threshold

    actual_wins = y_test[buy_signals].sum()
    total_buys = buy_signals.sum()

    print(f"\n📈 近期实盘盲测结果（AI 预测胜率 > 65% 才出手）：")
    if total_buys > 0:
        win_rate = actual_wins / total_buys * 100
        avg_premium = test_premium[buy_signals].mean()
        print(f"  👉 尾盘触发买入: {total_buys} 次")
        print(f"  👉 第二天成功吃溢价: {actual_wins} 次")
        print(f"  👉 真实胜率: {win_rate:.2f}%")
        print(f"  👉 平均单次隔夜溢价: {avg_premium:.2f}%")
    else:
        print("  👉 在目前的严苛标准下，近期没有符合出手的股票。")

if __name__ == "__main__":
    train_and_analyze_overnight()