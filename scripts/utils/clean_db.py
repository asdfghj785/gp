import pandas as pd
import glob
import os

DATA_DIR = "/Users/eudis/ths/data/all_kline"

def purge_bad_data():
    files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    print(f"🧹 开始清理 {len(files)} 个历史文件...")

    for f in files:
        try:
            df = pd.read_parquet(f)
            # 1. 记录原始行数
            original_len = len(df)

            # 2. 强力过滤：剔除价格为0、代码为 NaN 或价格异常大的行
            df = df[df['close'] > 0]
            df = df.dropna(subset=['close'])

            # 3. 如果有行被删除了，写回文件
            if len(df) < original_len:
                df.to_parquet(f)
                print(f"✅ {os.path.basename(f)}: 已清理 {original_len - len(df)} 条脏数据")
        except:
            continue

if __name__ == "__main__":
    purge_bad_data()