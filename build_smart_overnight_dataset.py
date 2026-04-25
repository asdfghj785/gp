import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

def build_smart_dataset():
    print("开始构建【隔夜策略】特征大表：单日K线 + 滚动趋势 + 量价异动 + 市场宽度...")

    kline_dir = "data/all_kline"
    if not os.path.exists(kline_dir):
        print("找不到基础日线数据，请确保已下载。")
        return

    all_files = [f for f in os.listdir(kline_dir) if f.endswith('_daily.parquet')]
    merged_data = []

    for file in tqdm(all_files, desc="提取个股时序与量价特征"):
        code = file.split('_')[0]

        # 仅保留主板，训练口径与生产候选池保持一致。
        if code.startswith(('30', '68', '4', '8', '92')):
            continue

        file_path = os.path.join(kline_dir, file)
        df_k = pd.read_parquet(file_path)

        if df_k.empty or len(df_k) < 70:
            continue
        df_k = df_k.sort_values('date').copy()
        for col in ['open', 'high', 'low', 'close', 'volume', 'turn', '量比', '真实涨幅点数']:
            if col not in df_k.columns:
                df_k[col] = 0
            df_k[col] = pd.to_numeric(df_k[col], errors='coerce')

        prev_close = df_k['close'].shift(1)

        df_k['实体比例'] = (df_k['close'] - df_k['open']) / prev_close * 100
        df_k['上影线比例'] = (df_k['high'] - df_k[['open', 'close']].max(axis=1)) / prev_close * 100
        df_k['下影线比例'] = (df_k[['open', 'close']].min(axis=1) - df_k['low']) / prev_close * 100
        df_k['日内振幅'] = (df_k['high'] - df_k['low']) / prev_close * 100

        # 让模型具备“记忆力”：趋势、均线位置、近期活跃度。
        df_k['5日累计涨幅'] = (df_k['close'] / df_k['close'].shift(5) - 1) * 100
        df_k['3日累计涨幅'] = (df_k['close'] / df_k['close'].shift(3) - 1) * 100
        df_k['5日均线乖离率'] = (df_k['close'] / df_k['close'].rolling(5, min_periods=3).mean() - 1) * 100
        df_k['20日均线乖离率'] = (df_k['close'] / df_k['close'].rolling(20, min_periods=10).mean() - 1) * 100
        df_k['3日平均换手率'] = df_k['turn'].rolling(3, min_periods=2).mean()

        # 量化主力行为：爆量、红盘资金参与、地量与缩量下跌。
        avg_vol10 = df_k['volume'].shift(1).rolling(10, min_periods=5).mean()
        avg_vol5 = df_k['volume'].shift(1).rolling(5, min_periods=3).mean()
        min_vol5 = df_k['volume'].rolling(5, min_periods=3).min()
        high60 = df_k['close'].rolling(60, min_periods=20).max()
        df_k['5日量能堆积'] = df_k['volume'] / avg_vol5
        df_k['10日量比'] = df_k['volume'] / avg_vol10
        df_k['3日红盘比例'] = (df_k['close'] > df_k['open']).astype(float).rolling(3, min_periods=2).mean() * 100
        df_k['5日地量标记'] = ((df_k['volume'] > 0) & (df_k['volume'] <= min_vol5)).astype(float)
        df_k['缩量下跌标记'] = ((df_k['close'] < prev_close) & (df_k['volume'] < avg_vol5)).astype(float)
        df_k['60日高位比例'] = df_k['close'] / high60 * 100
        df_k['高位爆量标记'] = ((df_k['60日高位比例'] >= 97) & ((df_k['量比'] > 3) | (df_k['5日量能堆积'] > 3))).astype(float)

        # 日线级“尾盘诱多”代理特征：没有分钟线时，用虚拉、缩量大涨和极端下影线给模型线索。
        df_k['振幅换手比'] = (df_k['日内振幅'] / df_k['turn'].replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
        df_k['缩量大涨标记'] = ((df_k['真实涨幅点数'] > 3) & (df_k['5日量能堆积'] < 1)).astype(float)
        df_k['极端下影线标记'] = ((df_k['下影线比例'] > df_k['实体比例'].abs() * 2) & (df_k['真实涨幅点数'] > 3)).astype(float)
        df_k['近3日断头铡刀标记'] = (df_k['真实涨幅点数'].shift(1).rolling(3, min_periods=1).min() <= -7).astype(float)
        # 当前历史库没有分钟线，无法真实识别14:30尾盘偷袭；保留字段供后续接入分钟数据。
        df_k['尾盘诱多标记'] = 0.0

        # ================= 核心：精准锁定明天的时间和价格 =================
        df_k['next_date'] = df_k['date'].shift(-1)  # 获取下一行真实的交易日期
        df_k['next_open'] = df_k['open'].shift(-1)
        df_k['next_high'] = df_k['high'].shift(-1)
        df_k['next_day_premium'] = (df_k['next_open'] - df_k['close']) / df_k['close'] * 100
        df_k['next_day_high_premium'] = (df_k['next_high'] - df_k['close']) / df_k['close'] * 100

        # 过滤：剔除当天涨停的（买不到）和数据残缺的。
        required = [
            'turn', '量比', '真实涨幅点数', '实体比例', '上影线比例', '下影线比例', '日内振幅',
            '5日累计涨幅', '3日累计涨幅', '5日均线乖离率', '20日均线乖离率', '3日平均换手率',
            '5日量能堆积', '10日量比', '3日红盘比例', '5日地量标记', '缩量下跌标记',
            '振幅换手比', '缩量大涨标记', '极端下影线标记', '近3日断头铡刀标记',
            '60日高位比例', '高位爆量标记', '尾盘诱多标记', 'next_day_premium', 'next_day_high_premium'
        ]
        valid_rows = df_k[(df_k['真实涨幅点数'] < 9.5) & (df_k['next_day_premium'].notna())].dropna(subset=required)

        if valid_rows.empty:
            continue

        # 把我们需要的所有字段打包（包含了价格和 next_date）
        features = valid_rows[[
            'symbol', 'date', 'next_date', 'close', 'next_open', 'next_high', 'turn', '量比', '真实涨幅点数',
            '实体比例', '上影线比例', '下影线比例', '日内振幅',
            '5日累计涨幅', '3日累计涨幅', '5日均线乖离率', '20日均线乖离率', '3日平均换手率',
            '5日量能堆积', '10日量比', '3日红盘比例', '5日地量标记', '缩量下跌标记',
            '振幅换手比', '缩量大涨标记', '极端下影线标记', '近3日断头铡刀标记',
            '60日高位比例', '高位爆量标记', '尾盘诱多标记', 'next_day_premium', 'next_day_high_premium'
        ]]
        merged_data.append(features)

    if not merged_data:
        print("提取失败，没有符合条件的数据。")
        return

    final_dataset = pd.concat(merged_data, ignore_index=True)
    final_dataset = _attach_market_context(final_dataset)

    os.makedirs("data/ml_dataset", exist_ok=True)
    output_path = "data/ml_dataset/smart_overnight_data.parquet"
    final_dataset.to_parquet(output_path, engine='pyarrow')

    print(f"\n特征提取完毕，共提取 {len(final_dataset)} 条数据，特征大表已更新。")


def _attach_market_context(df: pd.DataFrame) -> pd.DataFrame:
    market = (
        df.groupby('date')
        .agg(
            market_up_rate=('真实涨幅点数', lambda values: float((values > 0).mean() * 100)),
            market_down_count=('真实涨幅点数', lambda values: int((values < 0).sum())),
            market_avg_change=('真实涨幅点数', 'mean'),
        )
        .reset_index()
    )
    return df.merge(market, on='date', how='left')

if __name__ == "__main__":
    build_smart_dataset()
