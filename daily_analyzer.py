import requests
import pandas as pd
import xgboost as xgb
import os
import numpy as np
import time
from datetime import datetime
import json
import re
import warnings

warnings.filterwarnings('ignore')

# ================= 配置区 =================
PUSH_TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
BASE_DIR = "/Users/eudis/ths"  # 从你的系统配置中锁定绝对路径
MODEL_PATH = os.path.join(BASE_DIR, "overnight_xgboost.json")
OUTPUT_JSON = os.path.join(BASE_DIR, "latest_top_50.json") # 网页端读取的缓存文件
# =========================================

def send_wechat_msg(title, content):
    """微信推送函数"""
    if not PUSH_TOKEN:
        print("未配置 PUSHPLUS_TOKEN，跳过 PushPlus 推送。")
        return
    url = "http://www.pushplus.plus/send"
    data = {"token": PUSH_TOKEN, "title": title, "content": content, "template": "txt"}
    try:
        res = requests.post(url, json=data, timeout=10)
        if res.json()['code'] == 200:
            print(f"✅ 推送成功：{title}")
        else:
            print(f"❌ 推送失败：{res.text}")
    except Exception as e:
        print(f"❌ 网络推送异常: {e}")

def get_realtime_data_sina():
    """新浪(Sina)底层全市场快照"""
    all_data = []
    page = 1
    print("⏳ 启动 Ashare 极简模式：向新浪索要最新数据...")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://vip.stock.finance.sina.com.cn/'
    }

    while True:
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=page"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            text = res.text
            if not text or text == "null" or text == "[]": break

            valid_json_text = re.sub(r'([{,])([a-zA-Z0-9_]+):', r'\1"\2":', text)
            data_list = json.loads(valid_json_text)

            if not data_list: break
            all_data.extend(data_list)

            if len(data_list) < 100: break
            page += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"⚠️ 第 {page} 页拉取异常: {e}")
            break

    df = pd.DataFrame(all_data)
    if df.empty: return pd.DataFrame()

    rename_map = {
        "code": "代码", "name": "名称", "trade": "最新价",
        "changepercent": "涨跌幅", "turnoverratio": "换手率",
        "high": "最高", "low": "最低", "open": "今开", "settlement": "昨收"
    }
    df = df.rename(columns=rename_map)
    df['量比'] = 0.0

    return df

def get_realtime_recommendation():
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"⏰ 狙击手启动：{now_str}")

    if not os.path.exists(MODEL_PATH):
        send_wechat_msg("❌ 异常", "找不到模型文件 overnight_xgboost.json")
        return

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)

    df = get_realtime_data_sina()
    if df.empty:
        send_wechat_msg("❌ 中断", "新浪服务器连接失败。")
        return

    # 1. 强制转为数字，空值填 0
    cols = ['最新价', '涨跌幅', '量比', '换手率', '最高', '最低', '今开', '昨收']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # 2. 智能实战过滤盾
    df['纯代码'] = df['代码'].astype(str).str.extract(r'(\d{6})')[0]
    df = df[~df['纯代码'].str.startswith(('30', '68', '4', '8'), na=False)].copy()
    df = df[~df['名称'].str.contains('ST|退', case=False, na=False)].copy()
    df = df[(df['最新价'] > 0) & (df['昨收'] > 0)].copy()

    pre_len = len(df)
    df = df[df['涨跌幅'] < 9.5].copy()
    print(f"📡 已拦截 {pre_len - len(df)} 只封板无法买入的标的。")

    if df.empty:
        send_wechat_msg("💡 空仓", "全市场无符合条件的非涨停主板标的。")
        return

    # 3. 特征提炼
    df['实体比例'] = ((df['最新价'] - df['今开']) / df['昨收'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    df['上影线比例'] = ((df['最高'] - df[['今开', '最新价']].max(axis=1)) / df['昨收'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    df['下影线比例'] = ((df[['今开', '最新价']].min(axis=1) - df['最低']) / df['昨收'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    df['日内振幅'] = ((df['最高'] - df['最低']) / df['昨收'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    df['真实涨幅点数'] = df['涨跌幅']
    df['turn'] = df['换手率']

    feature_cols = ['turn', '量比', '真实涨幅点数', '实体比例', '上影线比例', '下影线比例', '日内振幅']
    X_live = df[feature_cols].values

    try:
        df['AI胜率'] = model.predict_proba(X_live)[:, 1] * 100
    except Exception as e:
        send_wechat_msg("❌ 特征报错", f"模型预测异常: {e}")
        return

    # 4. 排序并抓取头狼
    df = df.sort_values(by='AI胜率', ascending=False)
    winner = df.iloc[0]
    win_rate = float(winner['AI胜率'])

    # ================= 🚀 核心新增：快照定格逻辑 =================
    # 把此时此刻计算出的 Top 50 榜单，直接覆盖写入 JSON 文件
    top_50 = df.head(50)
    result_json = []
    for _, row in top_50.iterrows():
        result_json.append({
            "code": str(row['纯代码']),
            "name": str(row['名称']),
            "price": float(row['最新价']),
            "change": float(row['涨跌幅']),
            "volume_ratio": float(row['量比']),
            "turnover": float(row['换手率']),
            "win_rate": float(row['AI胜率']),
            "tech_features": {
                "body_ratio": float(row['实体比例']),
                "upper_shadow": float(row['上影线比例']),
                "lower_shadow": float(row['下影线比例']),
                "amplitude": float(row['日内振幅'])
            }
        })

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"📸 实盘快照已成功定格！已覆盖写入 {OUTPUT_JSON}")
    except Exception as e:
        print(f"⚠️ JSON 写入失败: {e}")
    # =========================================================

    # 5. 推送逻辑
    content = f"""【AI 综合评估结果 (Sina主板版)】
代码: {winner['纯代码']} | 名称: {winner['名称']}
价格: {winner['最新价']:.2f} | 涨幅: {winner['涨跌幅']:.2f}%
换手: {winner['换手率']:.2f}%
-----------------------
💎 AI 预测胜率: {win_rate:.2f}%
-----------------------"""

    if win_rate >= 60.0:
        title = f"🎯 尾盘狙击: {winner['名称']} ({win_rate:.1f}%)"
        footer = "💡 纪律: 14:54-14:56买入，次日09:25无条件卖出！"
        send_wechat_msg(title, content + "\n" + footer)
    else:
        title = f"⚠️ 建议空仓 ({win_rate:.1f}%)"
        footer = f"❌ 原因: 最高评分标的 {winner['名称']} 未达60%安全线。"
        send_wechat_msg(title, content + "\n" + footer)

if __name__ == "__main__":
    get_realtime_recommendation()
