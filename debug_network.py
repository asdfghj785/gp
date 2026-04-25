import pandas as pd
import requests
import time
import os

# 彻底清理环境变量
os.environ.clear()

def fetch_with_retry(symbol="000001", max_retries=3):
    # 东财 API 地址
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

    # 深度伪装请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://quote.eastmoney.com/center/gridlist.html',
        'Host': 'push2his.eastmoney.com',
        'Accept': '*/*',
        'Connection': 'keep-alive'
    }

    # API 参数 (这是 akshare 内部逻辑的直接模拟)
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "7eea3edfd05dd2ecb92b031d1d628c47",
        "klt": "101", # 日K
        "fqt": "1",   # 前复权
        "secid": f"0.{symbol}" if symbol.startswith('0') else f"1.{symbol}",
        "beg": "20251001",
        "end": "20260404",
    }

    for i in range(max_retries):
        try:
            print(f"📡 尝试第 {i+1} 次发起请求...")
            response = requests.get(url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data and data['data'] and data['data']['klines']:
                    print("✅ 连接成功！服务器已响应。")
                    return data['data']['klines']
                else:
                    print("⚠️ 服务器响应了，但今日无数据（休市中）。")
                    return None

        except Exception as e:
            print(f"❌ 第 {i+1} 次失败: {repr(e)}")
            time.sleep(2) # 等待 2 秒再试

    return None

if __name__ == "__main__":
    result = fetch_with_retry()
    if result:
        print(f"成功获取数据样例: {result[0]}")
    else:
        print("\n🆘 结论：服务器目前拒绝非浏览器连接，建议等到明天开盘前再试。")