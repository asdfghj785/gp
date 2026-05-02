import os
import sys
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import json
import re
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.data_pipeline.trading_calendar import latest_trading_day_on_or_before

# ================= 配置区 =================
# 锁定你之前的 Parquet 历史数据目录
DATA_DIR = "/Users/eudis/ths/data/all_kline"
PUSH_TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
# ==========================================

def notify(msg):
    """同步结果推送"""
    if not PUSH_TOKEN:
        print("未配置 PUSHPLUS_TOKEN，跳过 PushPlus 推送。")
        return
    url = "http://www.pushplus.plus/send"
    data = {"token": PUSH_TOKEN, "title": "📊 K线数据同步报告", "content": msg}
    try:
        requests.post(url, json=data, timeout=10)
    except:
        pass

def fetch_all_stock_data_sina():
    """使用新浪节点获取全市场收盘快照（穿透海外IP封锁）"""
    all_data = []
    page = 1
    print("⏳ 启动新浪底层节点，开始下载全市场收盘数据...")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://vip.stock.finance.sina.com.cn/'
    }

    while True:
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=page"
        try:
            res = requests.get(url, headers=headers, timeout=15)
            text = res.text
            if not text or text == "null" or text == "[]": break

            # 正则修复 JS 格式数据
            valid_json_text = re.sub(r'([{,])([a-zA-Z0-9_]+):', r'\1"\2":', text)
            data_list = json.loads(valid_json_text)

            if not data_list: break
            all_data.extend(data_list)
            print(f"   ✅ 已获取第 {page} 页...")

            if len(data_list) < 100: break
            page += 1
            time.sleep(0.1) # 极简停顿

        except Exception as e:
            print(f"⚠️ 第 {page} 页拉取异常: {e}")
            break

    df = pd.DataFrame(all_data)
    if df.empty:
        raise Exception("新浪节点返回空数据，可能是网络断开。")

    # --- 字段清洗与格式对齐 ---
    rename_map = {
        "symbol": "raw_code", "name": "name", "trade": "close",
        "changepercent": "change_pct", "turnoverratio": "turnover",
        "high": "high", "low": "low", "open": "open", "settlement": "pre_close",
        "amount": "amount"
    }
    df = df.rename(columns=rename_map)

    # 提取纯 6 位数字代码
    df['code'] = df['raw_code'].astype(str).str.extract(r'(\d{6})')[0]

    # 强制数值转换
    cols = ['close', 'pre_close', 'amount', 'open', 'high', 'low', 'change_pct', 'turnover']
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 补充缺失字段
    df['volume_ratio'] = 0.0

    # 核心过滤：只保留有真实成交的主板和创业板数据，剔除死票
    df = df[(df['close'] > 0) & (df['amount'] > 0) & (df['code'].notna())]

    # 打上当天的日期戳
    df['date'] = datetime.now().strftime('%Y-%m-%d')
    return df

def main():
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"🚀 开始将 {today_str} 的数据追加到本地数据库...")

    os.makedirs(DATA_DIR, exist_ok=True)

    try:
        today = datetime.now().date()
        latest_trade_day = latest_trading_day_on_or_before(today)
        if latest_trade_day != today:
            latest_text = latest_trade_day.isoformat() if latest_trade_day else "未知"
            skip_msg = f"⏸️ 非交易日跳过: 今日 {today_str} 最新交易日为 {latest_text}，不写入日线 Parquet。"
            print(skip_msg)
            return

        # 调用新浪引擎抓取数据
        df_today = fetch_all_stock_data_sina()

        success_count = 0
        new_stock_count = 0

        # 批量入库
        for _, row in df_today.iterrows():
            code = str(row['code'])
            file_path = os.path.join(DATA_DIR, f"{code}_daily.parquet")

            # 转为单行的 DataFrame
            df_new_row = pd.DataFrame([row])
            # 剔除辅助列
            if 'raw_code' in df_new_row.columns:
                df_new_row = df_new_row.drop(columns=['raw_code'])

            if os.path.exists(file_path):
                try:
                    df_history = pd.read_parquet(file_path)
                    df_combined = pd.concat([df_history, df_new_row], ignore_index=True)
                    # 按照日期去重，保留最新的一条，防止一天内多次运行导致重复记录
                    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
                    df_combined.to_parquet(file_path, index=False)
                    success_count += 1
                except:
                    pass
            else:
                df_new_row.to_parquet(file_path, index=False)
                new_stock_count += 1

        status_msg = f"✅ 盘后数据同步完成！\n成功追加历史文件: {success_count} 只\n新生成股票文件: {new_stock_count} 只\n日期: {today_str}"
        print(status_msg)
        notify(status_msg)

    except Exception as e:
        error_str = str(e)
        if "空数据" in error_str:
            holiday_msg = f"⏸️ 休市提醒: 今日 ({today_str}) 未获取到有效数据。请安心享受假期！"
            print(holiday_msg)
        else:
            error_msg = f"❌ Parquet 数据同步严重异常: {error_str}"
            print(error_msg)
            notify(error_msg)

if __name__ == "__main__":
    main()
