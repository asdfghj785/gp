import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')

# 锁定你的底层数据仓库
DATA_DIR = "/Users/eudis/ths/data/all_kline"

# 我们随机抽查两只风向标股票：平安银行(000001) 和 贵州茅台(600519)
# 你也可以换成你今天雷达扫出来的任意一只股票代码
test_codes = ['000001', '600519']

def probe_data():
    print("🔍 正在启动底层数据探针...\n")

    for code in test_codes:
        file_path = os.path.join(DATA_DIR, f"{code}_daily.parquet")

        if os.path.exists(file_path):
            try:
                # 读取 Parquet 文件
                df = pd.read_parquet(file_path)
                print(f"========== 标的: {code} ==========")
                print(f"总数据量: {len(df)} 根 K 线")

                # 只打印最后 3 天的数据，看看今天的数据在不在里面
                # 选取几个核心字段展示，防止终端显示太长换行
                display_cols = ['date', 'close', 'change_pct', 'turnover', 'volume_ratio']

                # 如果老数据的列名和这些不完全一样，可能会报 KeyError
                # 我们做一个容错，提取存在于表里的列
                valid_cols = [col for col in display_cols if col in df.columns]

                print(df[valid_cols].tail(3).to_string(index=False))
                print("====================================\n")
            except Exception as e:
                print(f"❌ 读取 {code} 文件失败: {e}\n")
        else:
            print(f"⚠️ 警告: 找不到 {code} 的 Parquet 文件！路径: {file_path}\n")

if __name__ == "__main__":
    probe_data()