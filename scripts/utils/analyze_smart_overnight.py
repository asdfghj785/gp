import pandas as pd
import numpy as np
import xgboost as xgb
import os
import matplotlib.pyplot as plt
import traceback
import warnings
warnings.filterwarnings('ignore')

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def train_and_analyze_ultimate_pure():
    print("🚀 启动 XGBoost 尾盘单挑 (实战防封板版) 回测模型...\n")

    file_path = "data/ml_dataset/smart_overnight_data.parquet"
    if not os.path.exists(file_path):
        print("❌ 未找到特征大表，请先运行 build_smart_overnight_dataset.py")
        return

    df = pd.read_parquet(file_path)

    # --- 核心修复：剔除当日已涨停、无法买入的股票 ---
    # 涨停板通常在 9.5% 以上就开始难以成交，我们设定 9.5% 为红线
    original_len = len(df)
    df = df[df['真实涨幅点数'] < 9.5].copy()
    print(f"📡 已拦截封板标的：剔除当日涨幅 >= 9.5% 的数据 {original_len - len(df)} 条")

    # 彻底清理可能存在的空值和无穷大（防止之前的数据污染影响回测）
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    df = df.sort_values(by='date').reset_index(drop=True)

    # 确保必要字段存在
    required_cols = ['close', 'next_open', 'next_date', '真实涨幅点数']
    if not all(col in df.columns for col in required_cols):
        print("❌ 数据表中缺少核心字段，请检查 build 脚本。")
        return

    # ================= 核心同步：与实盘严格保持一致的 7 个特征 =================
    feature_cols = [
        'turn', '量比', '真实涨幅点数',
        '实体比例', '上影线比例', '下影线比例', '日内振幅'
    ]
    # ========================================================================

    X = df[feature_cols]
    # 目标：次日开盘溢价大于 0.2% 视为获利（扣除手续费后的肉）
    y = (df['next_day_premium'] > 0.2).astype(int)

    # 训练集与测试集分割
    split_index = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    model = xgb.XGBClassifier(
        n_estimators=150, learning_rate=0.05, max_depth=5,
        subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric='logloss'
    )
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # 组装测试结果
    test_results = pd.DataFrame({
        '原始日期': df['date'].iloc[split_index:],
        '下一交易日': df['next_date'].iloc[split_index:],
        '股票代码': df['symbol'].iloc[split_index:],
        '当日涨幅': df['真实涨幅点数'].iloc[split_index:],
        '买入价': df['close'].iloc[split_index:],
        '卖出价': df['next_open'].iloc[split_index:],
        'AI预测胜率': y_pred_proba,
        '次日开盘溢价': df['next_day_premium'].iloc[split_index:],
        '是否获利': y_test
    })

    # 每天只取 AI 评分最高、且在实盘中确实能买入的那一只
    trade_log = test_results.sort_values(by=['原始日期', 'AI预测胜率'], ascending=[True, False]).groupby('原始日期').head(1)

    # 格式化显示逻辑
    trade_log['日期化'] = pd.to_datetime(trade_log['原始日期']).dt.strftime('%Y-%m-%d')
    trade_log['下一交易日化'] = pd.to_datetime(trade_log['下一交易日']).dt.strftime('%Y-%m-%d')

    trade_log['买入指令 (14:45)'] = trade_log['日期化'] + " 买入: " + trade_log['买入价'].apply(lambda x: f"{x:.2f}")
    trade_log['卖出指令 (09:25)'] = trade_log['下一交易日化'] + " 卖出: " + trade_log['卖出价'].apply(lambda x: f"{x:.2f}")

    trade_log['交易结果'] = trade_log['是否获利'].map({1: '✅ 吃肉', 0: '❌ 亏损'})
    trade_log['胜率显示'] = (trade_log['AI预测胜率'] * 100).round(2).astype(str) + '%'
    trade_log['溢价显示'] = trade_log['次日开盘溢价'].round(2).astype(str) + '%'

    print("📜 【真实战报】每日单挑交易明细 (已剔除涨停不可买入标的)：")
    print("=" * 125)
    pd.set_option('display.max_rows', 50) # 只显示最后50个交易日，防止刷屏

    display_cols = ['股票代码', '当日涨幅', '买入指令 (14:45)', '卖出指令 (09:25)', '胜率显示', '溢价显示', '交易结果']
    print(trade_log[display_cols].to_string(index=False))
    print("=" * 125)

    total_trades = len(trade_log)
    win_trades = trade_log['是否获利'].sum()
    win_rate = (win_trades / total_trades) * 100
    avg_premium = test_results.loc[trade_log.index, '次日开盘溢价'].mean()

    print(f"\n📊 【终极结论】剔除涨停板后的真实表现：")
    print(f"  👉 有效测试天数: {total_trades} 天")
    print(f"  👉 预测准确次数: {win_trades} 次")
    print(f"  👉 真实狙击胜率: {win_rate:.2f}%")
    print(f"  👉 真实平均溢价: {avg_premium:.2f}%")

    # 计算复利
    trade_log['daily_ret'] = test_results.loc[trade_log.index, '次日开盘溢价'] / 100
    trade_log['cumulative_profit'] = (1 + trade_log['daily_ret']).cumprod()
    final_return = (trade_log['cumulative_profit'].iloc[-1] - 1) * 100
    print(f"  👉 真实模拟总收益 (复利): {final_return:.2f}%")

if __name__ == "__main__":
    train_and_analyze_ultimate_pure()