import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore') # 忽略运算过程中的格式警告

def calculate_indicators(df):
    """在本地利用 Mac mini 算力计算技术指标"""
    df = df.sort_values(by='date').reset_index(drop=True)

    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()

    df['vol_ma5'] = df['volume'].shift(1).rolling(window=5).mean()
    df['量比'] = df['volume'] / df['vol_ma5']
    df.drop(columns=['vol_ma5'], inplace=True)

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_DIF'] = ema12 - ema26
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

    df['真实涨幅点数'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100

    return df

def robust_fetch_all_baostock():
    print("正在连接 Baostock 量化数据中心...")
    bs.login()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print("开始获取全市场股票列表...")

    # ================= 核心修复部分 =================
    # 自动往前推算，寻找最近的一个 A 股交易日来获取股票列表
    stock_df = pd.DataFrame()
    for i in range(15):
        check_date = (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=check_date)
        stock_list = []
        while (rs.error_code == '0') & rs.next():
            stock_list.append(rs.get_row_data())

        # 如果获取到了几千只股票，说明找到了真实的交易日，跳出循环
        if len(stock_list) > 1000:
            stock_df = pd.DataFrame(stock_list, columns=rs.fields)
            print(f"✅ 成功找到最近交易日 {check_date}，获取到基础列表！")
            break

    if stock_df.empty:
        print("❌ 获取股票列表失败，请检查网络。")
        bs.logout()
        return
    # ================================================

    # 仅保留沪深主板、创业板、科创板
    valid_stocks = stock_df[stock_df['code'].str.contains(r'^(sh\.60|sh\.68|sz\.00|sz\.30)')]
    stock_codes = valid_stocks['code'].tolist()
    print(f"共筛选出 {len(stock_codes)} 只目标股票。")

    save_dir = "data/all_kline"
    os.makedirs(save_dir, exist_ok=True)

    print("\n🚀 开始全量数据拉取与指标计算，请稍候...\n")

    success_count = 0
    fail_count = 0
    pbar = tqdm(stock_codes, desc="全量下载进度")

    for code in pbar:
        clean_code = code.split('.')[1]
        file_path = f"{save_dir}/{clean_code}_daily.parquet"

        # 断点续传
        if os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
            success_count += 1
            continue

        try:
            rs = bs.query_history_k_data_plus(code,
                "date,code,open,high,low,close,volume,amount,turn,pctChg",
                start_date=start_str, end_date=end_str,
                frequency="d", adjustflag="2")

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                fail_count += 1
                continue

            df = pd.DataFrame(data_list, columns=rs.fields)
            df = calculate_indicators(df)

            df['date'] = df['date'].str.replace('-', '')
            df.insert(0, 'symbol', clean_code)
            df.drop(columns=['code'], inplace=True)

            df.to_parquet(file_path, engine='pyarrow')
            success_count += 1

            pbar.set_postfix({'成功': success_count, '无数据或停牌': fail_count})

        except Exception as e:
            fail_count += 1
            pbar.set_postfix({'成功': success_count, '无数据或停牌': fail_count})

    print(f"\n✅ 全市场基础数据库构建完毕！")
    print(f"成功获取: {success_count} 只股票 | 跳过或无数据: {fail_count} 只股票")
    bs.logout()

if __name__ == "__main__":
    robust_fetch_all_baostock()