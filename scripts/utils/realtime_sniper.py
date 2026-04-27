from __future__ import annotations

from datetime import datetime

import requests

from quant_core.config import PUSHPLUS_TOKEN
from quant_core.engine.predictor import scan_market


def send_wechat_msg(title: str, content: str) -> None:
    url = "http://www.pushplus.plus/send"
    payload = {"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "txt"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"推送完成: {title}")
    except Exception as exc:
        print(f"推送失败: {exc}")


def get_realtime_recommendation() -> None:
    print(f"尾盘策略启动: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    try:
        result = scan_market(limit=10, persist_snapshot=True, cache_prediction=True, async_persist=False)
    except Exception as exc:
        send_wechat_msg("量化策略异常", f"实时预测失败: {exc}")
        return

    gate = result.get("market_gate") or {}
    intraday = result.get("intraday_snapshot") or {}
    rows = result.get("rows") or []
    if gate.get("blocked"):
        reasons = "；".join(gate.get("reasons") or ["大盘风控触发"])
        send_wechat_msg(
            "尾盘策略空仓",
            f"模式: {gate.get('mode')}\n成交额: {gate.get('market_amount_yi', 0):.0f} 亿\n原因: {reasons}\n\n系统已按大盘风控放弃推送个股。",
        )
        return

    if not rows:
        send_wechat_msg(
            "尾盘策略空仓",
            f"模式: {gate.get('mode')}\n尾盘快照: {intraday.get('status')}\n原因: 没有达到回归预期溢价、综合评分门槛且预期溢价为正的候选股。",
        )
        return

    winner = rows[0]
    content = f"""【AI 尾盘策略】
代码: {winner['code']} | 名称: {winner['name']}
【命中策略】: {winner.get('strategy_type', '未知')}
价格: {winner['price']:.2f} | 涨幅: {winner['change']:.2f}%
换手: {winner['turnover']:.2f}% | 量比: {winner['volume_ratio']:.2f}
收益信号: {winner['win_rate']:.2f}
预期溢价: {winner['expected_premium']:.2f}%
综合评分: {winner['composite_score']:.2f}

市场模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')} | 拦截: {intraday.get('trapped_count', 0)}

策略: 尾盘突破/首阴低吸双轨模型按预期溢价排序；目标为次日开盘溢价覆盖1.0%成本缓冲；晴天60%、阴天75%、尾盘拉升超过阈值直接剔除。"""
    send_wechat_msg(f"尾盘候选: {winner['name']} ({winner['composite_score']:.1f})", content)


if __name__ == "__main__":
    get_realtime_recommendation()
