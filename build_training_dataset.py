import pandas as pd
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

def merge_features_and_target():
    print("开始构建 AI 训练专用的特征大表...")

    # 1. 读取涨停板核心数据库
    limit_up_file = "data/core_db/half_year_limit_up.parquet"
    if not os.path.exists(limit_up_file):
        print("未找到涨停板数据，请确保之前的 batch_limit_up.py 已运行成功。")
        return

    df_zt = pd.read_parquet(limit_up_file)
    # 统一股票代码格式为纯数字（例如：000001）
    df_zt['代码'] = df_zt['代码'].astype(str).str.replace(r'[^0-9]', '', regex=True).str.zfill(6)

    merged_data = []
    grouped = df_zt.groupby('代码')

    # 2. 遍历每一只有过涨停的股票，去日线库里提取特征
    for code, group in tqdm(grouped, desc="数据融合与溢价计算进度"):
        kline_file = f"data/all_kline/{code}_daily.parquet"

        if not os.path.exists(kline_file):
            continue

        df_k = pd.read_parquet(kline_file)

        # 统一日期格式进行匹配
        df_k['date'] = df_k['date'].astype(str)
        group_df = group.copy()
        group_df['交易日期'] = group_df['交易日期'].astype(str).str.replace('-', '')

        # ================= 核心目标计算：第二天的竞价溢价 =================
        # shift(-1) 提取“下一交易日”的开盘价
        df_k['next_open'] = df_k['open'].shift(-1)

        # 溢价率公式 = (第二天开盘价 - 今天收盘价) / 今天收盘价 * 100
        df_k['next_day_premium'] = (df_k['next_open'] - df_k['close']) / df_k['close'] * 100
        # ==================================================================

        # 提取需要喂给 AI 的特征列
        k_features = df_k[['date', 'turn', 'MA5', 'MA10', 'MA20', '量比', 'MACD_DIF', 'MACD_DEA', 'MACD_hist', '真实涨幅点数', 'next_day_premium']]

        # 3. 将当天的技术指标合并到涨停数据中 (按日期对齐)
        merged = pd.merge(group_df, k_features, left_on='交易日期', right_on='date', how='inner')
        merged_data.append(merged)

    if not merged_data:
        print("没有匹配到任何数据，请检查基础数据。")
        return

    # 4. 生成最终大表
    final_dataset = pd.concat(merged_data, ignore_index=True)
    if 'date' in final_dataset.columns:
        final_dataset.drop(columns=['date'], inplace=True)

    # 清理掉第二天没有开盘价（比如停牌）的无效数据
    final_dataset.dropna(subset=['next_day_premium'], inplace=True)

    os.makedirs("data/ml_dataset", exist_ok=True)
    output_path = "data/ml_dataset/xgboost_training_data.parquet"
    final_dataset.to_parquet(output_path, engine='pyarrow')

    print(f"\n✅ 数据大缝合完毕！")
    print(f"共生成 {len(final_dataset)} 条极其珍贵的超短线样本。")
    print(f"包含了【封板资金/连板高度】+【换手/量比/MACD】+【次日溢价率】！")
    print(f"数据已存至: {output_path}")

if __name__ == "__main__":
    merge_features_and_target()