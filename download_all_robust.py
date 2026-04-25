import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from tqdm import tqdm

# ================= 强制清除代理，防止网络中断 =================
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

def calculate_indicators(df):
    """本地计算技术指标：均线、量比、MACD"""
    # 确保数据按日期正序排列
    df = df.sort_values(by='date').reset_index(drop=True)

    # 均线
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()

    # 量比
    df['vol_ma5'] = df['volume'].shift(1).rolling(window=5).mean()
    df['量比'] = df['volume'] / df['vol_ma5']
    df.drop(columns=['vol_ma5'], inplace=True)

    # MACD
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_DIF'] = ema12 - ema26
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

    # 真实涨幅
    df['真实涨幅点数'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100

    return df

def robust_fetch_all():
    print("开始获取全市场股票列表...")
    try:
        stock_list_df = ak.stock_info_a_code_name()
        # 保留沪深主板(00, 60开头)、创业板(30开头)、科创板(68开头)
        valid_stocks = stock_list_df[stock_list_df['code'].str.startswith(('00', '30', '60', '68'))]
        stock_codes = valid_stocks['code'].tolist()
        print(f"共获取到 {len(stock_codes)} 只目标股票。")
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return

    # 设置半年时间范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    save_dir = "data/all_kline"
    os.makedirs(save_dir, exist_ok=True)

    print("\n🚀 开始全市场数据拉取与指标计算，请保持网络通畅...")
    print("提示：如果中途停止，再次运行会自动跳过已下载的股票（断点续传）\n")

    success_count = 0
    fail_count = 0

    # 进度条
    pbar = tqdm(stock_codes, desc="全量数据下载进度")

    for code in pbar:
        file_path = f"{save_dir}/{code}_daily.parquet"

        # 【断点续传核心】如果文件存在且大小正常，直接跳过
        if os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
            success_count += 1
            continue

        # 增加重试机制：最多重试3次
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 获取日线数据
                df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")

                if df is None or df.empty:
                    break # 数据为空（可能是停牌或新股），直接跳出重试

                df.rename(columns={
                    '日期': 'date', '开盘': 'open', '收盘': 'close',
                    '最高': 'high', '最低': 'low', '成交量': 'volume',
                    '成交额': 'amount', '振幅': 'amplitude', '涨跌幅': 'pct_chg',
                    '涨跌额': 'change', '换手率': 'turnover'
                }, inplace=True)

                # 计算指标并加入代码列
                df = calculate_indicators(df)
                df.insert(0, 'symbol', code)

                # 保存并记录成功
                df.to_parquet(file_path, engine='pyarrow')
                success_count += 1

                # 动态更新进度条后缀信息
                pbar.set_postfix({'成功': success_count, '失败或无数据': fail_count})

                # 休眠防封杀
                time.sleep(0.3)
                break # 成功则跳出重试循环

            except BaseException as e: # 捕获网络异常
                if attempt < max_retries - 1:
                    time.sleep(1) # 遇到错误停顿1秒再试
                else:
                    fail_count += 1
                    pbar.set_postfix({'成功': success_count, '失败或无数据': fail_count})

    print(f"\n✅ 全市场基础数据库构建完毕！")
    print(f"成功: {success_count} 只股票 | 跳过/失败/无数据: {fail_count} 只股票")

if __name__ == "__main__":
    robust_fetch_all()