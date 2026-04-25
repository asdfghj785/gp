from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Any

import requests

from quant_core.config import PUSHPLUS_TOKEN
from quant_core.daily_pick import save_pushed_top_pick
from quant_core.predictor import scan_market
from quant_core.storage import database_overview, get_daily_pick, latest_prediction_snapshot


PUSHPLUS_URL = "http://www.pushplus.plus/send"


def send_pushplus(title: str, content: str) -> dict[str, Any]:
    if not PUSHPLUS_TOKEN:
        raise RuntimeError("缺少 PUSHPLUS_TOKEN，无法发送 PushPlus")
    response = requests.post(
        PUSHPLUS_URL,
        json={"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "txt"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 200:
        raise RuntimeError(f"PushPlus 返回失败: {payload}")
    return payload


def heartbeat() -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    overview = database_overview()
    cached = latest_prediction_snapshot()
    cached_rows = len(cached.get("rows", [])) if cached else 0
    latest_report = overview.get("latest_report") or {}
    latest_report_summary = latest_report.get("summary") or {}

    content = f"""项目代码运行正常
时间: {now}
工作目录: /Users/eudis/ths
数据库股票数: {overview.get('stock_count')}
数据库K线数: {overview.get('rows_count')}
数据日期范围: {overview.get('min_date')} 至 {overview.get('max_date')}
预测缓存条数: {cached_rows}
最新数据验证: {latest_report.get('status', '无')} / 错误 {latest_report_summary.get('error_count', '-')} / 警告 {latest_report_summary.get('warning_count', '-')}
今日14:50将推送实时最高预期溢价股票。
"""
    result = send_pushplus("量化项目心跳：运行正常", content)
    print({"status": "sent", "task": "heartbeat", "time": now, "pushplus": result})
    return result


def top_pick() -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    locked = get_daily_pick(date.today().isoformat())
    if locked:
        content = f"""14:50 推送标的已锁定
时间: {now}
代码: {locked['code']}
名称: {locked['name']}
锁定价: {locked['selection_price']:.2f}
收益信号: {locked['win_rate']:.2f}
状态: {locked['status']}
目标开盘日: {locked['target_date']}

今日记录已存在，系统不会重新扫描或覆盖修改。
"""
        result = send_pushplus(f"14:50尾盘候选已锁定: {locked['name']}", content)
        print({"status": "sent_locked", "task": "top_pick", "time": now, "pick": locked, "pushplus": result})
        return {"pushplus": result, "daily_pick": {"status": "exists", "pick": locked}}

    scan = scan_market(limit=10, persist_snapshot=False, cache_prediction=False, async_persist=False)
    rows = scan.get("rows", [])
    gate = scan.get("market_gate") or {}
    intraday = scan.get("intraday_snapshot") or {}
    if gate.get("blocked"):
        reasons = "；".join(gate.get("reasons") or ["大盘风控触发"])
        content = f"""14:50 实时策略结果
时间: {now}
状态: 空仓
模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')}
原因: {reasons}
模型状态: {scan.get('model_status')}
"""
        result = send_pushplus("14:50尾盘策略：大盘风控空仓", content)
        print({"status": "sent", "task": "top_pick", "time": now, "market_gate": gate, "pushplus": result})
        return result
    if not rows:
        content = f"""14:50 实时策略结果
时间: {now}
状态: 空仓
模式: {gate.get('mode')}
14:30快照: {intraday.get('status')} | 拦截: {intraday.get('trapped_count', 0)}
原因: 没有达到回归预期溢价、综合评分门槛且预期溢价为正的候选股。
模型状态: {scan.get('model_status')}
"""
        result = send_pushplus("14:50尾盘策略：无强信号空仓", content)
        print({"status": "sent", "task": "top_pick", "time": now, "pushplus": result})
        return result
    winner = rows[0]
    content = f"""14:50 实时最高预期溢价股票
时间: {now}
代码: {winner['code']}
名称: {winner['name']}
现价: {winner['price']:.2f}
涨跌幅: {winner['change']:.2f}%
换手率: {winner['turnover']:.2f}%
收益信号: {winner['win_rate']:.2f}
预期溢价: {winner['expected_premium']:.2f}%
综合评分: {winner['composite_score']:.2f}
市场模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')} | 匹配: {intraday.get('matched_count', 0)} | 拦截: {intraday.get('trapped_count', 0)}
模型状态: {scan.get('model_status')}

技术特征:
实体比例: {winner['tech_features']['body_ratio']:.2f}%
上影线: {winner['tech_features']['upper_shadow']:.2f}%
下影线: {winner['tech_features']['lower_shadow']:.2f}%
日内振幅: {winner['tech_features']['amplitude']:.2f}%
尾盘拉升: {winner['trend_features'].get('late_pull_pct', 0):.2f}%
振幅换手比: {winner['trend_features'].get('amplitude_turnover_ratio', 0):.2f}

过滤规则: 已排除创业板、北交所、科创板、ST/退市；回归模型按预期溢价排序；雷暴或下跌缩量空仓；14:30后尾盘拉升超过阈值、近3日断头铡刀直接剔除；目标为次日开盘溢价覆盖1.0%成本缓冲。
"""
    result = send_pushplus(f"14:50尾盘候选: {winner['name']} {winner['composite_score']:.1f}", content)
    saved = save_pushed_top_pick(winner, scan, force=False)
    print({"status": "sent", "task": "top_pick", "time": now, "winner": winner, "pushplus": result, "daily_pick": saved})
    return {"pushplus": result, "daily_pick": saved}


def main() -> None:
    parser = argparse.ArgumentParser(description="PushPlus 定时通知任务")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("heartbeat", help="发送项目运行正常心跳")
    sub.add_parser("top-pick", help="推送实时最高预期溢价股票")
    args = parser.parse_args()

    if args.command == "heartbeat":
        heartbeat()
    elif args.command == "top-pick":
        top_pick()


if __name__ == "__main__":
    main()
