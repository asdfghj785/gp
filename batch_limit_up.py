import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta
import time

# 清除代理环境变量
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

def fetch_half_year_limit_up():
    print("开始构建半年期超短线核心数据库...")

    # 1. 获取 A 股交易日历
    # 获取最近 180 天的日期，并过滤出交易日
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)

    # 使用 akshare 获取交易日历
    tool_trade_date_hist_df = ak.tool_trade_date_hist_sina()
    trade_dates = tool_trade_date_hist_df["trade_date"].astype(str).tolist()

    # 转换日期格式进行过滤 (筛选出在 start_date 和 end_date 之间的交易日)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    valid_dates = [d.replace("-", "") for d in trade_dates if start_str <= d <= end_str]

    print(f"过去半年共有 {len(valid_dates)} 个交易日。开始批量下载...")

    all_data = []

    # 2. 循环获取每天的涨停数据
    for date in valid_dates:
        print(f"正在抓取 {date} 的数据...")
        try:
            df = ak.stock_zt_pool_em(date=date)
            if not df.empty:
                # 插入日期列，方便后续按时间回测
                df['交易日期'] = date
                all_data.append(df)

            # 加上 0.5 秒的延迟，防止请求过快被东方财富封锁 IP
            time.sleep(0.5)

        except Exception as e:
            print(f"  > {date} 获取失败跳过: {e}")
            continue

    # 3. 合并所有数据并保存
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # 提取核心列
        desired_columns = [
            '交易日期', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值',
            '换手率', '连板数', '首次封板时间', '最后封板时间',
            '封板资金', '炸板次数', '所属行业', '涨停统计'
        ]
        columns_to_keep = [col for col in desired_columns if col in final_df.columns]
        final_df = final_df[columns_to_keep]

        os.makedirs("data/core_db", exist_ok=True)
        file_path = "data/core_db/half_year_limit_up.parquet"
        final_df.to_parquet(file_path, engine='pyarrow')

        print(f"\n大功告成！半年涨停数据合并完毕，总共 {len(final_df)} 条记录。")
        print(f"数据已压缩保存至: {file_path}")
    else:
        print("未能获取到任何数据。")

if __name__ == "__main__":
    fetch_half_year_limit_up()