import pandas as pd
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

def build_precise_dataset():
    print("开始构建【高胜率尾盘潜伏】精准特征大表...")

    kline_dir = "data/all_kline"
    if not os.path.exists(kline_dir):
        print("找不到基础数据，请确保已下载。")
        return

    all_files = [f for f in os.listdir(kline_dir) if f.endswith('_daily.parquet')]
    merged_data = []

    for file in tqdm(all_files, desc="全市场精准漏斗筛查"):
        code = file.split('_')[0]

        # 剔除创业板和科创板
        if code.startswith('30') or code.startswith('68'):
            continue

        file_path = os.path.join(kline_dir, file)
        df_k = pd.read_parquet(file_path)

        if df_k.empty or len(df_k) < 30:
            continue

        df_k['next_open'] = df_k['open'].shift(-1)
        df_k['next_day_premium'] = (df_k['next_open'] - df_k['close']) / df_k['close'] * 100

        # ================= 核心精准过滤条件 =================
        # 剔除包含空值的行，确保均线计算有效
        df_valid = df_k.dropna(subset=['MA5', 'MA10', 'MA20', '量比', 'MACD_DIF', 'next_day_premium']).copy()

        condition = (
            (df_valid['真实涨幅点数'] >= 3.0) & (df_valid['真实涨幅点数'] <= 7.5) &  # 涨幅 3%~7.5%
            (df_valid['MA5'] > df_valid['MA10']) & (df_valid['MA10'] > df_valid['MA20']) & # 均线多头
            (df_valid['量比'] > 1.2) &        # 量比放大
            (df_valid['turn'] > 5.0) &        # 换手活跃
            (df_valid['MACD_DIF'] > df_valid['MACD_DEA']) # MACD 金叉或多头
        )

        precise_rows = df_valid[condition]
        # ==================================================

        if precise_rows.empty:
            continue

        features = precise_rows[['symbol', 'date', 'turn', 'MA5', 'MA10', 'MA20', '量比', 'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数', 'next_day_premium']]
        merged_data.append(features)

    if not merged_data:
        print("条件太苛刻，未能提取到有效数据。")
        return

    final_dataset = pd.concat(merged_data, ignore_index=True)

    os.makedirs("data/ml_dataset", exist_ok=True)
    output_path = "data/ml_dataset/precise_overnight_data.parquet"
    final_dataset.to_parquet(output_path, engine='pyarrow')

    print(f"\n✅ 精准漏斗筛选完毕！")
    print(f"从海量数据中，为你提炼出 {len(final_dataset)} 条符合【尾盘抢筹铁律】的极品样本。")

if __name__ == "__main__":
    build_precise_dataset()