import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score
import matplotlib.pyplot as plt

# 解决 Mac 中文字体显示问题
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def train_and_simulate():
    print("🚀 启动 XGBoost 超短线竞价溢价预测模型...\n")

    # 1. 加载我们清洗好的黄金数据集
    df = pd.read_parquet("data/ml_dataset/xgboost_training_data.parquet")
    df = df.sort_values(by='交易日期').reset_index(drop=True)

    # 2. 选取特征 (X) 和定义目标 (Y)
    # 将时间字符串转换为浮点数特征 (例如 093000 -> 93000)，越早封板数值越小
    if '首次封板时间' in df.columns:
        df['首次封板时间_num'] = df['首次封板时间'].str.replace(':', '').astype(float)

    # 挑选出所有的纯数字特征
    feature_cols = [
        '连板数', '封板资金', '炸板次数', '流通市值',
        'turn', '量比', 'MA5', 'MA10', 'MA20',
        'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数',
        '首次封板时间_num'
    ]

    # 确保特征列都在数据中
    feature_cols = [col for col in feature_cols if col in df.columns]

    X = df[feature_cols]
    # 目标 Y：第二天溢价率大于 0 视为成功 (1)，否则为失败 (0)
    y = (df['next_day_premium'] > 0).astype(int)

    # 3. 严格按时间划分训练集和测试集（模拟实盘）
    # 假设前 70% 的日子用于训练找规律，后 30% 的日子（约近一到两个月）用于实盘模拟验证
    split_index = int(len(df) * 0.7)

    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    test_dates = df['交易日期'].iloc[split_index:]
    test_codes = df['代码'].iloc[split_index:]
    test_names = df['名称'].iloc[split_index:]
    test_premium = df['next_day_premium'].iloc[split_index:]

    print(f"📚 训练集大小: {len(X_train)} 条 (让 AI 寻找规律)")
    print(f"🎯 盲测集大小: {len(X_test)} 条 (模拟最近一到两个月的实盘操作)\n")

    # 4. 初始化并训练 XGBoost 模型
    # 这些参数是为了防止 AI 死记硬背（过拟合）而设置的
    model = xgb.XGBClassifier(
        n_estimators=200,      # 树的数量
        learning_rate=0.05,    # 学习率
        max_depth=5,           # 树的深度
        subsample=0.8,         # 每次训练随机抽取80%的数据
        colsample_bytree=0.8,  # 每次训练随机抽取80%的特征
        random_state=42,
        eval_metric='logloss'
    )

    model.fit(X_train, y_train)

    # ================= 分析一：归因分析（到底什么数据最有用？） =================
    print("📊 AI 深度归因分析：【特征重要性排名】")
    importances = model.feature_importances_
    feature_importance_df = pd.DataFrame({'特征': feature_cols, '重要度': importances})
    feature_importance_df = feature_importance_df.sort_values(by='重要度', ascending=False)

    for idx, row in feature_importance_df.iterrows():
        print(f"  ⭐️ {row['特征']:<12}: {row['重要度'] * 100:.2f}%")

    # ================= 分析二：模拟实盘胜率回测 =================
    # 让 AI 对最近一两个月的数据进行预测，输出它判断能赚钱的概率
    y_pred_proba = model.predict_proba(X_test)[:, 1] # 获取预测为 1（能赚钱）的概率

    # 我们制定一个严格的交易策略：只有当 AI 认为胜率超过 70% 时，我们才买入！
    threshold = 0.70
    buy_signals = y_pred_proba > threshold

    actual_wins = y_test[buy_signals].sum()
    total_buys = buy_signals.sum()

    print(f"\n📈 最近一到两个月实盘模拟结果（AI 置信度 > 70% 才出手）：")
    if total_buys > 0:
        win_rate = actual_wins / total_buys * 100
        print(f"  👉 总共触发买入信号: {total_buys} 次")
        print(f"  👉 成功吃肉 (第二天开盘红盘): {actual_wins} 次")
        print(f"  👉 极致胜率: {win_rate:.2f}%")

        # 看看 AI 都挑了些什么股票
        print("\n🏆 AI 强烈推荐买入的股票案例（部分）:")
        results_df = pd.DataFrame({
            '日期': test_dates[buy_signals],
            '代码': test_codes[buy_signals],
            '名称': test_names[buy_signals],
            'AI预测胜率': y_pred_proba[buy_signals],
            '次日实际溢价': test_premium[buy_signals]
        })
        print(results_df.head(5).to_string(index=False))
    else:
        print("  👉 在目前的严苛标准下，近期没有符合出手的股票。你可以尝试调低 threshold 参数。")

if __name__ == "__main__":
    train_and_simulate()