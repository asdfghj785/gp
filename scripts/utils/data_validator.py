import pandas as pd
import numpy as np
import os

def validate_dataset():
    file_path = "data/ml_dataset/xgboost_training_data.parquet"

    if not os.path.exists(file_path):
        print(f"❌ 找不到文件: {file_path}")
        return

    print("==================================================")
    print("📊 AI 训练数据集深度体检报告")
    print("==================================================\n")

    df = pd.read_parquet(file_path)

    # ---------------------------------------------------------
    # 1. 基础规模与完整性检测
    # ---------------------------------------------------------
    print("【第一项】数据规模与完整度检测")
    print(f"总样本数: {len(df)} 条涨停记录")
    print(f"总特征数: {len(df.columns)} 个字段")

    # 检查缺失值
    missing_data = df.isnull().sum()
    missing_cols = missing_data[missing_data > 0]
    if missing_cols.empty:
        print("✅ 缺失值检测: 完美！没有任何缺失值。")
    else:
        print("⚠️ 发现缺失值:")
        for col, count in missing_cols.items():
            print(f"   - [{col}]: 缺失 {count} 条 ({count/len(df)*100:.2f}%)")

    # 检查日期跨度
    min_date = df['交易日期'].min()
    max_date = df['交易日期'].max()
    print(f"✅ 时间跨度: {min_date} 至 {max_date}")
    print("-" * 50)

    # ---------------------------------------------------------
    # 2. 逻辑正确性检测 (金融常识验证)
    # ---------------------------------------------------------
    print("【第二项】金融逻辑正确性检测")

    # 检测1：量比和换手率是否合法（必须 > 0）
    if '量比' in df.columns and 'turn' in df.columns:
        invalid_vol = len(df[(df['量比'] <= 0) | (df['turn'] <= 0)])
        if invalid_vol == 0:
            print("✅ 交易活跃度: 量比和换手率全部大于 0，逻辑正确。")
        else:
            print(f"❌ 异常警告: 发现 {invalid_vol} 条量比或换手率 <= 0 的脏数据！")

    # 检测2：因为是“涨停板数据库”，当天的“真实涨幅点数”应该大部分在 9% 以上
    if '真实涨幅点数' in df.columns:
        # A股主板涨停约 10%，创业板 20%，ST股 5%。偶尔有炸板或特殊情况。
        # 如果当天涨幅小于 4%，大概率数据匹配错位了。
        low_pct_count = len(df[df['真实涨幅点数'] < 4.0])
        if low_pct_count / len(df) < 0.05: # 允许 5% 的特殊情况(如地天板、ST股)
            print("✅ 涨停板逻辑: 当天涨跌幅数据符合涨停池特征。")
        else:
            print(f"⚠️ 注意: 有 {low_pct_count} 条数据的当天涨幅低于 4%，请检查是否有炸板或ST股干扰。")
    print("-" * 50)

    # ---------------------------------------------------------
    # 3. 真实性与极值检测 (防止 AI 模型被离群点带偏)
    # ---------------------------------------------------------
    print("【第三项】特征极值与离群点检测")

    # 检测核心目标：第二天竞价溢价率
    if 'next_day_premium' in df.columns:
        premium = df['next_day_premium']
        print(f"🎯 核心目标 [次日竞价溢价率] 统计:")
        print(f"   - 平均溢价: {premium.mean():.2f}%")
        print(f"   - 中 位 数: {premium.median():.2f}%")
        print(f"   - 最高溢价: {premium.max():.2f}%")
        print(f"   - 最低溢价: {premium.min():.2f}%")

        # 极值警告：次日开盘溢价一般在 -10% 到 +20% 之间（极端地天/天地板例外）
        # 如果出现 100% 以上的溢价，说明碰到了新股上市或者除权除息导致的价格断层
        extreme_premium = len(df[(df['next_day_premium'] > 30) | (df['next_day_premium'] < -20)])
        if extreme_premium > 0:
            print(f"⚠️ 极端异常值警告: 发现 {extreme_premium} 条次日溢价率极其离谱(>30% 或 <-20%)的数据。")
            print("   (建议：在喂给 XGBoost 之前，通过代码将这些由于复权或新股导致的极端值剔除)")
        else:
            print("✅ 溢价率分布正常，没有破坏性极值。")

    print("==================================================\n")
    print("💡 结论与建议：")
    if extreme_premium > 0 or not missing_cols.empty:
        print("数据总体可用，但在训练前，建议清洗掉包含缺失值的行，并剔除掉溢价率超过常规范围的极值点。")
    else:
        print("数据质量极佳！各项指标符合 A 股逻辑，可以直接进入 AI 模型训练阶段。")

if __name__ == "__main__":
    validate_dataset()