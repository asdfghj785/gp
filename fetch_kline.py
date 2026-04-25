import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta


# 同样加上清除代理的环境变量，防止网络问题
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""


def fetch_and_save_kline(symbol="000001", months=6):
    print(f"开始获取 {symbol} 过去 {months} 个月的日线数据...")

    # 计算半年前的日期
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months*30)

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    try:
        # 调用 AkShare 接口获取 A股日线数据（qfq：前复权）
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")

        if df.empty:
            print("未获取到数据，请检查股票代码或网络。")
            return

        # 整理数据列名，方便后续处理
        df.rename(columns={
            '日期': 'date', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volume',
            '成交额': 'amount', '振幅': 'amplitude', '涨跌幅': 'pct_chg',
            '涨跌额': 'change', '换手率': 'turnover'
        }, inplace=True)

        # 确保目录存在
        os.makedirs("data/kline", exist_ok=True)

        # 极其关键：保存为 Parquet 格式，大幅节省 Mac 的硬盘空间
        file_path = f"data/kline/{symbol}_daily.parquet"
        df.to_parquet(file_path, engine='pyarrow')

        print(f"成功！数据已保存至 {file_path}")
        print(df.head(3)) # 打印前三行看看效果

    except Exception as e:
        print(f"获取失败: {e}")

# 测试运行：获取“平安银行(000001)”的数据
if __name__ == "__main__":
    fetch_and_save_kline(symbol="000001", months=6)