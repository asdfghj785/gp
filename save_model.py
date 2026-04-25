import pandas as pd
import xgboost as xgb
import os

def export_model():
    print("正在剔除冗余指标，训练纯粹的【日内情绪博弈】模型...")
    df = pd.read_parquet("data/ml_dataset/smart_overnight_data.parquet")

    # 核心修改：剔除 MA5 和 MACD_DIF，只保留最纯粹的 7 个日内游资指标
    feature_cols = ['turn', '量比', '真实涨幅点数', '实体比例', '上影线比例', '下影线比例', '日内振幅']
    X = df[feature_cols]
    y = (df['next_day_premium'] > 0.2).astype(int)

    model = xgb.XGBClassifier(
        n_estimators=150, learning_rate=0.05, max_depth=5,
        subsample=0.8, colsample_bytree=0.8, random_state=42
    )
    model.fit(X, y)

    model.save_model("overnight_xgboost.json")
    print("✅ 纯净版模型已成功固化为: overnight_xgboost.json")

if __name__ == "__main__":
    export_model()