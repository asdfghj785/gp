import requests
import os
from datetime import datetime

# ================= 配置区 =================
PUSH_TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
# =========================================

def send_heartbeat():
    if not PUSH_TOKEN:
        print("未配置 PUSHPLUS_TOKEN，跳过心跳推送。")
        return
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    url = "http://www.pushplus.plus/send"

    title = f"🍏 系统状态：正常在线"
    content = f"""监控报告
时间: {now_str}
设备: Mac mini 量化工作站
状态: 守护进程运行中，网络连接正常。
指令预告: 今日 14:40 将准时执行狙击任务。
"""

    data = {
        "token": PUSH_TOKEN,
        "title": title,
        "content": content,
        "template": "txt"
    }

    try:
        res = requests.post(url, json=data, timeout=10)
        if res.json()['code'] == 200:
            print(f"心跳发送成功: {now_str}")
        else:
            print(f"心跳发送失败: {res.text}")
    except Exception as e:
        print(f"心跳网络异常: {e}")

if __name__ == "__main__":
    send_heartbeat()
