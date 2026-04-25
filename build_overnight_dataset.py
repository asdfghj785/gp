import pandas as pd
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

def build_tail_end_dataset():
    print("开始构建【尾盘潜伏博隔夜】专用特征大表...")

    kline_dir = "data/all_kline"
    if not os.path.exists(kline_dir):
        print("找不到基础日线数据，请确保已下载。")
        return

    all_files = [f for f in os.listdir(kline_dir) if f.endswith('_daily.parquet')]

    merged_data = []

    for file in tqdm(all_files, desc="全市场数据筛查与提取"):
        code = file.split('_')[0]

        # 1. 严格过滤：剔除创业板(300)和科创板(688)
        if code.startswith('30') or code.startswith('68'):
            continue

        file_path = os.path.join(kline_dir, file)
        df_k = pd.read_parquet(file_path)

        if df_k.empty or len(df_k) < 20: # 剔除新股或数据极少的股票
            continue

        # 2. 目标计算：提取“明天”的开盘价，计算隔夜溢价率
        df_k['next_open'] = df_k['open'].shift(-1)
        df_k['next_day_premium'] = (df_k['next_open'] - df_k['close']) / df_k['close'] * 100

        # 3. 核心过滤：剔除当天已经涨停的股票 (涨幅 >= 9.5%)，因为尾盘买不进
        # 同时剔除第二天停牌或没数据的行
        valid_rows = df_k[(df_k['真实涨幅点数'] < 9.5) & (df_k['next_day_premium'].notna())].copy()

        if valid_rows.empty:
            continue

        # 提取喂给 AI 的特征
        features = valid_rows[['symbol', 'date', 'turn', 'MA5', 'MA10', 'MA20', '量比', 'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数', 'next_day_premium']]
        merged_data.append(features)

    if not merged_data:
        print("未能提取到有效数据。")
        return

    # 生成最终大表
    final_dataset = pd.concat(merged_data, ignore_index=True)

    os.makedirs("data/ml_dataset", exist_ok=True)
    output_path = "data/ml_dataset/overnight_strategy_data.parquet"
    final_dataset.to_parquet(output_path, engine='pyarrow')

    print(f"\n✅ 隔夜策略数据构建完毕！")
    print(f"共提取 {len(final_dataset)} 条【主板非涨停】样本。数据已存至: {output_path}")

if __name__ == "__main__":
    build_tail_end_dataset()