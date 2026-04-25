import requests
import os

TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("请先设置环境变量 PUSHPLUS_TOKEN 后再运行推送测试。")

url = "http://www.pushplus.plus/send"
data = {
    "token": TOKEN,
    "title": "量化系统通信测试",
    "content": "恭喜！如果你在微信里看到了这条消息，说明你的量化印钞机已经成功连上了微信！",
    "template": "txt"
}

print("正在向微信发送测试指令...")
try:
    response = requests.post(url, json=data)
    print(f"服务器返回结果: {response.text}")
except Exception as e:
    print(f"网络发送失败: {e}")
