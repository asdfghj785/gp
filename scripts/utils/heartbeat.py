import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.execution.pushplus_tasks import send_pushplus

def send_heartbeat():
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    title = f"🍏 系统状态：正常在线"
    content = f"""监控报告
时间: {now_str}
设备: Mac mini 量化工作站
状态: 守护进程运行中，网络连接正常。
指令预告: 今日 14:40 将准时执行狙击任务。
"""

    result = send_pushplus(title, content)
    print({"task": "heartbeat", "time": now_str, "pushplus": result})

if __name__ == "__main__":
    send_heartbeat()
