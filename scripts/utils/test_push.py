import sys
from pathlib import Path

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.execution.pushplus_tasks import send_pushplus


print("正在向全部启用 PushPlus token 发送测试指令...")
result = send_pushplus(
    "量化系统通信测试",
    "PushPlus 多 token 上线测试：如果你看到这条消息，说明统一推送链路已接通。",
)
print(
    {
        "status": result.get("status"),
        "token_count": result.get("token_count"),
        "sent_count": result.get("sent_count"),
        "failed_count": result.get("failed_count"),
    }
)
