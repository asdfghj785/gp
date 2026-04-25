import akshare as ak
import pandas as pd
import os

# 同样加上清除代理的环境变量，防止网络问题
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

def fetch_limit_up_pool(target_date="20260317"):
    print(f"正在获取 {target_date} 的涨停板池数据...")

    try:
        df = ak.stock_zt_pool_em(date=target_date)

        if df.empty:
            print(f"{target_date} 没有获取到数据，可能是周末或节假日。")
            return

        # 打印出接口实际返回的所有列名，方便我们排查
        print(f"\n接口实际返回的所有字段如下：\n{df.columns.tolist()}\n")

        # 我们期望获取的列（把常用的都写上）
        desired_columns = [
            '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值',
            '换手率', '连板数', '首次封板时间', '最后封板时间',
            '封板资金', '炸板次数', '所属行业', '涨停统计'
        ]

        # 核心修复：只保留实际存在于 DataFrame 中的列，避免因为缺列报错
        columns_to_keep = [col for col in desired_columns if col in df.columns]

        df = df[columns_to_keep]

        os.makedirs("data/limit_up", exist_ok=True)
        file_path = f"data/limit_up/zt_{target_date}.parquet"
        df.to_parquet(file_path, engine='pyarrow')

        print(f"成功！{target_date} 共有 {len(df)} 只股票涨停。数据已保存至 {file_path}")
        print(df.head(3))

    except Exception as e:
        print(f"获取数据时出错: {e}")

if __name__ == "__main__":
    # 2026年3月17日是交易日，用这个测试没问题
    fetch_limit_up_pool(target_date="20260317")