import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from tqdm import tqdm

# 清除代理环境变量
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

def calculate_indicators(df):
    """在本地计算技术指标：均线、量比、MACD"""
    # 确保数据按日期正序排列（从早到晚）
    df = df.sort_values(by='date').reset_index(drop=True)

    # 1. 计算均线 (日线、周线、月线逻辑可以在后续聚合时做，这里先算日级别的5/10/20)
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()

    # 2. 计算日线级别量比
    # 量比公式：当日成交量 / 前5个交易日平均成交量
    # shift(1) 是为了防止把当天的成交量也算进前5日的平均中
    df['vol_ma5'] = df['volume'].shift(1).rolling(window=5).mean()
    df['量比'] = df['volume'] / df['vol_ma5']
    df.drop(columns=['vol_ma5'], inplace=True) # 算完清理中间变量

    # 3. 计算 MACD (标准参数 12, 26, 9)
    # EMA12 和 EMA26
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    # DIF (差离值)
    df['MACD_DIF'] = ema12 - ema26
    # DEA (信号线)
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
    # MACD柱状图 (通常是 DIF - DEA 的 2倍)
    df['MACD_hist'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

    # 4. 计算每天涨幅的趋势点数 (你提到的需求：也就是每日真实的涨跌百分比)
    # 虽然接口里有 pct_chg，但我们可以自己算一个更精确的
    df['真实涨幅点数'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100

    # 截取掉前面因为计算均线产生 NaN 空值的行 (前26天MACD算不准)
    # 但为了保留这半年的完整数据，我们选择不删除，用 0 或前值填充，或者保留 NaN
    # df.fillna(0, inplace=True)

    return df

def fetch_all_stocks_half_year():
    print("正在获取A股所有股票代码列表...")
    try:
        # 获取所有 A 股股票列表
        stock_list_df = ak.stock_info_a_code_name()
        # 过滤掉北交所(8, 4开头)和部分退市股，主要保留沪深主板和创业板科创板
        valid_stocks = stock_list_df[stock_list_df['code'].str.startswith(('00', '30', '60', '68'))]
        stock_codes = valid_stocks['code'].tolist()
        print(f"共筛选出 {len(stock_codes)} 只沪深主板/创业板/科创板股票。")
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return

    # 计算时间范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    # 创建存储目录
    save_dir = "data/all_kline"
    os.makedirs(save_dir, exist_ok=True)

    print("开始批量下载并计算指标 (预计耗时 1~2 小时，可挂在后台运行)...")

    # 使用 tqdm 创建进度条
    for code in tqdm(stock_codes, desc="下载进度"):
        file_path = f"{save_dir}/{code}_daily.parquet"

        # 增量更新逻辑：如果文件已经存在，就跳过（方便断点续传）
        if os.path.exists(file_path):
            continue

        try:
            # 下载基础日线数据
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")

            if df is None or df.empty:
                continue

            # 统一命名规范
            df.rename(columns={
                '日期': 'date', '开盘': 'open', '收盘': 'close',
                '最高': 'high', '最低': 'low', '成交量': 'volume',
                '成交额': 'amount', '振幅': 'amplitude', '涨跌幅': 'pct_chg',
                '涨跌额': 'change', '换手率': 'turnover'
            }, inplace=True)

            # 核心：将基础数据送入我们写的函数中，计算所有技术指标
            df = calculate_indicators(df)

            # 加入股票代码列，方便以后多表合并时识别
            df.insert(0, 'symbol', code)

            # 保存为 Parquet
            df.to_parquet(file_path, engine='pyarrow')

            # 友好休眠，避免被东方财富服务器识别为恶意攻击而封杀 IP
            time.sleep(0.3)

        except Exception as e:
            # 静默处理单个股票的错误，防止整个循环崩溃
            continue

    print(f"\n全部下载及指标计算完成！数据保存在 {save_dir} 目录下。")

if __name__ == "__main__":
    fetch_all_stocks_half_year()