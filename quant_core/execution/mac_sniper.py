from __future__ import annotations

import argparse
import math
import os
import re
import subprocess
import time
from typing import Any, Optional


# 使用前必须在 Mac 的【系统设置 -> 隐私与安全性 -> 辅助功能】中，
# 为当前运行本项目的终端或 IDE（Terminal/iTerm/Cursor/VSCode/PyCharm 等）打勾授权。
ACCESSIBILITY_WARNING = (
    "\033[91m[MacSniper][ACCESSIBILITY REQUIRED] AppleScript 键盘注入失败。\n"
    "请打开 Mac【系统设置 -> 隐私与安全性 -> 辅助功能】，"
    "为当前终端或 IDE（Terminal/iTerm/Cursor/VSCode/PyCharm）打勾授权后重试。\033[0m"
)

def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


CODE_FIELD_INDEX = _env_int("QUANT_MAC_SNIPER_CODE_FIELD_INDEX", 1, minimum=1)
QUANTITY_FIELD_INDEX = _env_int("QUANT_MAC_SNIPER_QUANTITY_FIELD_INDEX", 3, minimum=1)
PRICE_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_PRICE_UI_ELEMENT_INDEX", 38, minimum=1)
CODE_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_CODE_UI_ELEMENT_INDEX", 42, minimum=1)
QUANTITY_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_QUANTITY_UI_ELEMENT_INDEX", 43, minimum=1)
RESET_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_RESET_UI_ELEMENT_INDEX", 37, minimum=1)
SUBMIT_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_SUBMIT_UI_ELEMENT_INDEX", 47, minimum=1)
BUY_UI_ELEMENT_INDEX = _env_int("QUANT_MAC_SNIPER_BUY_UI_ELEMENT_INDEX", 49, minimum=1)
CONFIRM_ENTER_COUNT = _env_int("QUANT_MAC_SNIPER_CONFIRM_ENTER_COUNT", 1)
TRADE_PANEL_BUTTON_INDEX = _env_int("QUANT_MAC_SNIPER_TRADE_PANEL_BUTTON_INDEX", 7, minimum=1)
BUY_BUTTON_NAME = os.getenv("QUANT_MAC_SNIPER_BUY_BUTTON_NAME", "买入")
SELL_BUTTON_NAME = os.getenv("QUANT_MAC_SNIPER_SELL_BUTTON_NAME", "卖出")
RESET_BUTTON_NAME = os.getenv("QUANT_MAC_SNIPER_RESET_BUTTON_NAME", "重填")
SUBMIT_BUTTON_NAME = os.getenv("QUANT_MAC_SNIPER_SUBMIT_BUTTON_NAME", "确定买入")
SELL_SUBMIT_BUTTON_NAME = os.getenv("QUANT_MAC_SNIPER_SELL_SUBMIT_BUTTON_NAME", "确定卖出")
SELL_PANEL_KEY_CODE = _env_int("QUANT_MAC_SNIPER_SELL_PANEL_KEY_CODE", 1, minimum=0)


def aim_and_fire(
    stock_code: str,
    app_name: str = "同花顺",
    shares: Optional[int] = None,
    limit_price: Optional[float] = None,
    action_type: str = "buy",
    dry_run: bool = False,
) -> dict[str, Any]:
    clean_code = normalize_stock_code(stock_code)
    clean_app = str(app_name or "同花顺").strip()
    if not clean_app:
        raise ValueError("app_name 不能为空")
    clean_action_type = normalize_action_type(action_type)
    clean_shares = normalize_shares(shares, clean_action_type) if shares is not None else None
    clean_limit_price = normalize_limit_price(limit_price) if limit_price is not None else None

    try:
        before_snapshot = None
        before_snapshot_error = ""
        preexisting_broker_alert = None
        if clean_shares is not None:
            try:
                preexisting_broker_alert = read_and_dismiss_broker_alert(clean_app)
            except Exception as exc:
                preexisting_broker_alert = {"status": "read_failed", "error": str(exc)}
            try:
                before_snapshot = read_trade_panel_snapshot(clean_app, action_type=clean_action_type)
            except Exception as exc:
                before_snapshot_error = str(exc)
                if clean_action_type == "sell":
                    try:
                        before_snapshot = read_trade_panel_snapshot(clean_app, action_type="buy")
                        before_snapshot_error = f"{before_snapshot_error}; fallback_buy_snapshot_ok"
                    except Exception as fallback_exc:
                        before_snapshot_error = f"{before_snapshot_error}; fallback_buy_snapshot_failed={fallback_exc}"

        script_body = build_script(clean_app, clean_code, clean_shares, clean_limit_price, action_type=clean_action_type)
        if not isinstance(script_body, str) or not script_body.strip():
            raise RuntimeError("AppleScript 生成失败：脚本为空")
    except Exception as exc:
        result = {
            "status": "failed",
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "error": str(exc),
            "hint": "Mac Sniper 脚本生成失败，未触发同花顺下单。",
        }
        print(result)
        return result
    if dry_run:
        result = {
            "status": "dry_run",
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "code_field_index": CODE_FIELD_INDEX if clean_shares else None,
            "quantity_field_index": QUANTITY_FIELD_INDEX if clean_shares else None,
            "price_ui_element_index": PRICE_UI_ELEMENT_INDEX if clean_shares else None,
            "code_ui_element_index": CODE_UI_ELEMENT_INDEX if clean_shares else None,
            "quantity_ui_element_index": QUANTITY_UI_ELEMENT_INDEX if clean_shares else None,
            "trade_panel_button_index": TRADE_PANEL_BUTTON_INDEX if clean_shares else None,
            "script": script_body.strip(),
        }
        print(result)
        return result

    try:
        completed = subprocess.run(
            ["osascript", "-e", script_body],
            check=True,
            capture_output=True,
            text=True,
            timeout=9 if clean_shares else 5,
        )
        broker_verification = None
        after_snapshot = None
        after_snapshot_error = ""
        broker_alert = None
        status = "fired"
        if clean_shares is not None:
            time.sleep(0.5)
            try:
                broker_alert = read_and_dismiss_broker_alert(clean_app)
            except Exception as exc:
                broker_alert = {"status": "read_failed", "error": str(exc)}
            time.sleep(0.5)
            try:
                after_snapshot = read_trade_panel_snapshot(clean_app, action_type=clean_action_type)
                broker_verification = verify_order_filled(
                    before_snapshot,
                    after_snapshot,
                    clean_code,
                    clean_shares,
                    clean_action_type,
                )
                if broker_verification.get("confirmed"):
                    status = "broker_confirmed"
                elif _broker_alert_present(broker_alert):
                    status = "broker_alert"
                    broker_verification["reason"] = "broker_alert"
                    broker_verification["broker_alert_message"] = broker_alert.get("message")
                else:
                    status = "submitted_unverified"
            except Exception as exc:
                after_snapshot_error = str(exc)
                status = "broker_alert" if _broker_alert_present(broker_alert) else "submitted_unverified"
                broker_verification = {
                    "confirmed": False,
                    "code": clean_code,
                    "action_type": clean_action_type,
                    "requested_shares": clean_shares,
                    "before_quantity": position_quantity(before_snapshot, clean_code),
                    "after_quantity": None,
                    "reason": "broker_alert" if _broker_alert_present(broker_alert) else "after_snapshot_failed",
                }
                if _broker_alert_present(broker_alert):
                    broker_verification["broker_alert_message"] = broker_alert.get("message")

        result = {
            "status": status,
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "code_field_index": CODE_FIELD_INDEX if clean_shares else None,
            "quantity_field_index": QUANTITY_FIELD_INDEX if clean_shares else None,
            "price_ui_element_index": PRICE_UI_ELEMENT_INDEX if clean_shares else None,
            "code_ui_element_index": CODE_UI_ELEMENT_INDEX if clean_shares else None,
            "quantity_ui_element_index": QUANTITY_UI_ELEMENT_INDEX if clean_shares else None,
            "trade_panel_button_index": TRADE_PANEL_BUTTON_INDEX if clean_shares else None,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
        if clean_shares is not None:
            result["preexisting_broker_alert"] = preexisting_broker_alert
            result["broker_alert"] = broker_alert
            result["before_snapshot"] = before_snapshot
            result["after_snapshot"] = after_snapshot
            result["before_snapshot_error"] = before_snapshot_error
            result["after_snapshot_error"] = after_snapshot_error
            result["broker_verification"] = broker_verification
        print(result)
        return result
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        access_denied = "不允许发送按键" in stderr or "not allowed" in stderr.lower() or "assistive" in stderr.lower()
        if access_denied:
            print(ACCESSIBILITY_WARNING)
        result = {
            "status": "failed",
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "returncode": exc.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "hint": (
                "当前运行后端的 osascript 没有辅助功能权限。"
                if access_denied
                else "同花顺交易面板定位失败；请确认已打开交易/买入面板，或先用诊断输出校准控件。"
            ),
        }
        print(result)
        return result
    except subprocess.TimeoutExpired as exc:
        result = {
            "status": "timeout",
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "timeout": exc.timeout,
            "hint": "osascript 执行超时，检查目标 App 是否卡死或未安装。",
        }
        print(result)
        return result
    except FileNotFoundError:
        result = {
            "status": "failed",
            "code": clean_code,
            "app_name": clean_app,
            "action_type": clean_action_type,
            "shares": clean_shares,
            "limit_price": clean_limit_price,
            "profile": "mac_ax_trade_form" if clean_shares else "quote_jump",
            "error": "当前系统找不到 osascript，仅支持 macOS 本地桌面环境。",
        }
        print(result)
        return result


def build_script(
    app_name: str,
    stock_code: str,
    shares: Optional[int] = None,
    limit_price: Optional[str] = None,
    action_type: str = "buy",
) -> str:
    clean_action_type = normalize_action_type(action_type)
    if shares is None:
        if clean_action_type == "sell":
            raise ValueError("全自动卖出必须提供卖出股数 shares")
        return f"""
tell application {_applescript_string(app_name)}
    activate
end tell
delay 0.5
tell application "System Events"
    keystroke {_applescript_string(stock_code)}
    delay 0.2
    key code 36
end tell
"""

    confirm_enters = _enter_sequence(CONFIRM_ENTER_COUNT)
    if clean_action_type == "buy" and not limit_price:
        raise ValueError("全自动买入必须提供委托价 limit_price")
    action_button_name = BUY_BUTTON_NAME if clean_action_type == "buy" else SELL_BUTTON_NAME
    submit_button_name = SUBMIT_BUTTON_NAME if clean_action_type == "buy" else SELL_SUBMIT_BUTTON_NAME
    action_panel_name = "买入" if clean_action_type == "buy" else "卖出"
    required_max_index = max(
        CODE_UI_ELEMENT_INDEX,
        QUANTITY_UI_ELEMENT_INDEX,
        SUBMIT_UI_ELEMENT_INDEX,
        PRICE_UI_ELEMENT_INDEX if limit_price else 0,
    )
    if clean_action_type == "sell":
        action_select_block = f"""
        if exists button {_applescript_string(action_button_name)} of window targetWindowIndex then
            click button {_applescript_string(action_button_name)} of window targetWindowIndex
        else
            -- 需在实盘前手动修改 key code，确保同花顺焦点正确切换到【卖出】面板后再填入股数并确认。
            key code {SELL_PANEL_KEY_CODE}
        end if
"""
    else:
        action_select_block = f"""
        if exists button {_applescript_string(action_button_name)} of window targetWindowIndex then click button {_applescript_string(action_button_name)} of window targetWindowIndex
"""
    price_input_block = ""
    if limit_price:
        price_input_block = f"""
        set priceField to UI element {PRICE_UI_ELEMENT_INDEX} of window targetWindowIndex
        if not ((role of priceField as text) is "AXTextField") then error "价格输入框定位失败，UI element {PRICE_UI_ELEMENT_INDEX} 不是文本框"
        set value of priceField to {_applescript_string(limit_price)}
        delay 0.15
        set actualPrice to value of priceField as text
        if actualPrice does not contain {_applescript_string(limit_price)} then
            set pricePosition to position of priceField
            set priceSize to size of priceField
            set priceX to (item 1 of pricePosition) + ((item 1 of priceSize) div 2)
            set priceY to (item 2 of pricePosition) + ((item 2 of priceSize) div 2)
            click at {{priceX, priceY}}
            delay 0.1
            keystroke "a" using {{command down}}
            delay 0.05
            key code 51
            delay 0.05
            keystroke {_applescript_string(limit_price)}
            delay 0.2
            set actualPrice to value of priceField as text
        end if
        if actualPrice does not contain {_applescript_string(limit_price)} then error "价格框写入校验失败: " & actualPrice
"""
    return f"""
tell application {_applescript_string(app_name)}
    activate
end tell
delay 0.8
tell application "System Events"
    key code 53 -- 关闭可能残留的键盘精灵/浮层
    delay 0.2
    key code 53 -- 再次关闭顶部搜索/AI 浮层，避免抢走后续价格输入焦点
    delay 0.1
    tell process {_applescript_string(app_name)}
        set frontmost to true
        if (count of windows) = 0 then error "未找到同花顺窗口"

        repeat with candidateIndex from 1 to count of windows
            try
                if exists button "确认" of window candidateIndex then
                    click button "确认" of window candidateIndex
                    delay 0.2
                end if
            end try
            try
                if exists button "确定" of window candidateIndex then
                    click button "确定" of window candidateIndex
                    delay 0.2
                end if
            end try
        end repeat

        set mainWindowIndex to 0
        set maxChildren to -1
        repeat with candidateIndex from 1 to count of windows
            try
                set childCount to count of UI elements of window candidateIndex
                if childCount > maxChildren then
                    set maxChildren to childCount
                    set mainWindowIndex to candidateIndex
                end if
            end try
        end repeat

        set targetWindowIndex to 0
        repeat with candidateIndex from 1 to count of windows
            try
                if exists button {_applescript_string(submit_button_name)} of window candidateIndex then
                    set targetWindowIndex to candidateIndex
                    exit repeat
                end if
            end try
        end repeat

        if targetWindowIndex = 0 then
            repeat with candidateIndex from 1 to count of windows
                try
                    if exists button {_applescript_string(action_button_name)} of window candidateIndex then
                        set targetWindowIndex to candidateIndex
                        exit repeat
                    end if
                end try
            end repeat
        end if

        if targetWindowIndex = 0 then
            if mainWindowIndex > 0 then
                if (count of UI elements of window mainWindowIndex) >= {TRADE_PANEL_BUTTON_INDEX} then
                    click UI element {TRADE_PANEL_BUTTON_INDEX} of window mainWindowIndex -- Mac 同花顺当前布局：第 7 个顶层按钮进入交易面板
                    delay 0.8
                end if
            end if
        end if

        if targetWindowIndex = 0 then
            repeat with candidateIndex from 1 to count of windows
                try
                    if exists button {_applescript_string(submit_button_name)} of window candidateIndex then
                        set targetWindowIndex to candidateIndex
                        exit repeat
                    end if
                end try
                try
                    if exists button {_applescript_string(action_button_name)} of window candidateIndex then
                        set targetWindowIndex to candidateIndex
                        exit repeat
                    end if
                end try
            end repeat
        end if

        if targetWindowIndex = 0 then error "未找到可操作的同花顺交易主窗口"

        if (count of UI elements of window targetWindowIndex) < {required_max_index} then error "交易表单 UI 元素数量不足，无法自动填单"

{action_select_block}
        delay 0.25
        if not (exists button {_applescript_string(submit_button_name)} of window targetWindowIndex) then error "当前同花顺主窗口不是交易{action_panel_name}面板，未找到“{submit_button_name}”按钮"

        if (role of UI element {RESET_UI_ELEMENT_INDEX} of window targetWindowIndex as text) is "AXButton" then click UI element {RESET_UI_ELEMENT_INDEX} of window targetWindowIndex
        delay 0.2

        set codeField to UI element {CODE_UI_ELEMENT_INDEX} of window targetWindowIndex
        if not ((role of codeField as text) is "AXTextField") then error "代码输入框定位失败，UI element {CODE_UI_ELEMENT_INDEX} 不是文本框"
        set value of codeField to {_applescript_string(stock_code)}
        delay 0.4
        set actualCode to value of codeField as text
        if actualCode does not contain {_applescript_string(stock_code)} then error "代码框写入校验失败: " & actualCode

{price_input_block}

        set quantityField to UI element {QUANTITY_UI_ELEMENT_INDEX} of window targetWindowIndex
        if not ((role of quantityField as text) is "AXTextField") then error "数量输入框定位失败，UI element {QUANTITY_UI_ELEMENT_INDEX} 不是文本框"
        set value of quantityField to {_applescript_string(str(shares))}
        delay 0.2
        set actualShares to value of quantityField as text
        if actualShares does not contain {_applescript_string(str(shares))} then error "数量框写入校验失败: " & actualShares

        click UI element {SUBMIT_UI_ELEMENT_INDEX} of window targetWindowIndex
    end tell
    delay 0.2
{confirm_enters}
end tell
"""


def read_trade_panel_snapshot(app_name: str = "同花顺", action_type: str = "buy") -> dict[str, Any]:
    clean_app = str(app_name or "同花顺").strip()
    if not clean_app:
        raise ValueError("app_name 不能为空")
    clean_action_type = normalize_action_type(action_type)
    completed = subprocess.run(
        ["osascript", "-e", build_snapshot_script(clean_app, action_type=clean_action_type)],
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
    )
    return parse_trade_panel_snapshot(completed.stdout)


def read_and_dismiss_broker_alert(app_name: str = "同花顺") -> dict[str, Any]:
    clean_app = str(app_name or "同花顺").strip()
    if not clean_app:
        raise ValueError("app_name 不能为空")
    completed = subprocess.run(
        ["osascript", "-e", build_broker_alert_script(clean_app)],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    return parse_broker_alert(completed.stdout)


def build_broker_alert_script(app_name: str) -> str:
    return f"""
tell application {_applescript_string(app_name)}
    activate
end tell
delay 0.2
tell application "System Events"
    tell process {_applescript_string(app_name)}
        set frontmost to true
        set out to ""
        set targetWindowIndex to 0
        set dismissedAlert to false
        repeat with windowIndex from 1 to count of windows
            try
                if exists sheet 1 of window windowIndex then
                    set targetWindowIndex to windowIndex
                    exit repeat
                end if
            end try
        end repeat
        if targetWindowIndex = 0 then
            return "status" & tab & "absent"
        end if

        set sheetElem to sheet 1 of window targetWindowIndex
        set out to out & "alert" & tab & (targetWindowIndex as text) & tab & "1" & linefeed
        repeat with childIndex from 1 to count of UI elements of sheetElem
            try
                set childElem to UI element childIndex of sheetElem
                set childRole to role of childElem as text
                set childName to ""
                set childValue to ""
                try
                    set childName to name of childElem as text
                end try
                try
                    set childValue to value of childElem as text
                end try
                if childRole is "AXStaticText" then
                    set out to out & "text" & tab & childName & tab & childValue & linefeed
                else if childRole is "AXButton" then
                    set out to out & "button" & tab & childName & tab & childValue & linefeed
                end if
            end try
        end repeat

        try
            click button "确认" of sheetElem
            set dismissedAlert to true
            delay 0.2
        on error
            repeat with childIndex from 1 to count of UI elements of sheetElem
                try
                    set childElem to UI element childIndex of sheetElem
                    if (role of childElem as text) is "AXButton" then
                        if (name of childElem as text) is "确认" then
                            click childElem
                            set dismissedAlert to true
                            delay 0.2
                            exit repeat
                        end if
                    end if
                end try
            end repeat
        end try

        set out to out & "dismissed" & tab & (dismissedAlert as text)
        return out
    end tell
end tell
"""


def build_snapshot_script(app_name: str, action_type: str = "buy") -> str:
    clean_action_type = normalize_action_type(action_type)
    submit_button_name = SUBMIT_BUTTON_NAME if clean_action_type == "buy" else SELL_SUBMIT_BUTTON_NAME
    return f"""
tell application {_applescript_string(app_name)}
    activate
end tell
delay 0.5
tell application "System Events"
    tell process {_applescript_string(app_name)}
        set frontmost to true
        set targetWindowIndex to 0
        repeat with candidateIndex from 1 to count of windows
            try
                if exists button {_applescript_string(submit_button_name)} of window candidateIndex then
                    set targetWindowIndex to candidateIndex
                    exit repeat
                end if
            end try
        end repeat

        if targetWindowIndex = 0 then
            set mainWindowIndex to 0
            set maxChildren to -1
            repeat with candidateIndex from 1 to count of windows
                try
                    set childCount to count of UI elements of window candidateIndex
                    if childCount > maxChildren then
                        set maxChildren to childCount
                        set mainWindowIndex to candidateIndex
                    end if
                end try
            end repeat

            if mainWindowIndex > 0 then
                if (count of UI elements of window mainWindowIndex) >= {TRADE_PANEL_BUTTON_INDEX} then
                    click UI element {TRADE_PANEL_BUTTON_INDEX} of window mainWindowIndex -- 当前 Mac 同花顺布局：第 7 个顶层按钮进入交易面板
                    delay 0.9
                end if
            end if

            repeat with candidateIndex from 1 to count of windows
                try
                    if exists button {_applescript_string(submit_button_name)} of window candidateIndex then
                        set targetWindowIndex to candidateIndex
                        exit repeat
                    end if
                end try
            end repeat
        end if

        if targetWindowIndex = 0 then error "未找到同花顺交易窗口"

        set out to "target_window" & tab & targetWindowIndex & linefeed

        try
            set orderCode to value of UI element {CODE_UI_ELEMENT_INDEX} of window targetWindowIndex as text
        on error
            set orderCode to ""
        end try
        try
            set orderName to name of UI element 27 of window targetWindowIndex as text
        on error
            set orderName to ""
        end try
        try
            set orderPrice to value of UI element {PRICE_UI_ELEMENT_INDEX} of window targetWindowIndex as text
        on error
            set orderPrice to ""
        end try
        try
            set orderQuantity to value of UI element {QUANTITY_UI_ELEMENT_INDEX} of window targetWindowIndex as text
        on error
            set orderQuantity to ""
        end try
        try
            set currentPrice to name of UI element 53 of window targetWindowIndex as text
        on error
            set currentPrice to ""
        end try
        set out to out & "order" & tab & orderCode & tab & orderName & tab & orderPrice & tab & orderQuantity & tab & currentPrice & linefeed

        try
            set accountTable to UI element 1 of UI element 14 of window targetWindowIndex
            repeat with rowIndex from 1 to count of UI elements of accountTable
                try
                    set rowElem to UI element rowIndex of accountTable
                    if (role of rowElem as text) is "AXRow" and (count of UI elements of rowElem) >= 2 then
                        set labelCell to UI element 1 of rowElem
                        set valueCell to UI element 2 of rowElem
                        set labelText to name of labelCell as text
                        set valueText to name of valueCell as text
                        set out to out & "account" & tab & labelText & tab & valueText & linefeed
                    end if
                end try
            end repeat
        end try

        try
            set positionTableFound to false
            set positionTable to missing value
            repeat with topIndex from 1 to count of UI elements of window targetWindowIndex
                try
                    set topElem to UI element topIndex of window targetWindowIndex
                    if (role of topElem as text) is "AXScrollArea" then
                        repeat with childIndex from 1 to count of UI elements of topElem
                            try
                                set childElem to UI element childIndex of topElem
                                if (role of childElem as text) is "AXTable" then
                                    repeat with groupIndex from 1 to count of UI elements of childElem
                                        try
                                            set groupElem to UI element groupIndex of childElem
                                            if (role of groupElem as text) is "AXGroup" then
                                                set headerText to ""
                                                repeat with headerIndex from 1 to count of UI elements of groupElem
                                                    try
                                                        set headerText to headerText & (name of UI element headerIndex of groupElem as text) & "|"
                                                    end try
                                                end repeat
                                                if headerText contains "证券代码" and headerText contains "证券名称" then
                                                    set positionTable to childElem
                                                    set positionTableFound to true
                                                    exit repeat
                                                end if
                                            end if
                                        end try
                                    end repeat
                                end if
                            end try
                            if positionTableFound then exit repeat
                        end repeat
                    end if
                end try
                if positionTableFound then exit repeat
            end repeat

            if positionTableFound then
                repeat with rowIndex from 1 to count of UI elements of positionTable
                    try
                        set rowElem to UI element rowIndex of positionTable
                        if (role of rowElem as text) is "AXRow" and (count of UI elements of rowElem) >= 13 then
                            set rowLine to "position"
                            repeat with cellIndex from 1 to 13
                                set cellText to ""
                                try
                                    set cellText to name of UI element cellIndex of rowElem as text
                                end try
                                set rowLine to rowLine & tab & cellText
                            end repeat
                            set out to out & rowLine & linefeed
                        end if
                    end try
                end repeat
            end if
        end try

        return out
    end tell
end tell
"""


def build_full_snapshot_script(app_name: str) -> str:
    return f"""
tell application {_applescript_string(app_name)}
    activate
end tell
delay 0.5
tell application "System Events"
    tell process {_applescript_string(app_name)}
        set frontmost to true
        set targetWindowIndex to 0
        repeat with candidateIndex from 1 to count of windows
            try
                if exists button {_applescript_string(SUBMIT_BUTTON_NAME)} of window candidateIndex then
                    set targetWindowIndex to candidateIndex
                    exit repeat
                end if
            end try
        end repeat

        if targetWindowIndex = 0 then
            set mainWindowIndex to 0
            set maxChildren to -1
            repeat with candidateIndex from 1 to count of windows
                try
                    set childCount to count of UI elements of window candidateIndex
                    if childCount > maxChildren then
                        set maxChildren to childCount
                        set mainWindowIndex to candidateIndex
                    end if
                end try
            end repeat

            if mainWindowIndex > 0 then
                if (count of UI elements of window mainWindowIndex) >= {TRADE_PANEL_BUTTON_INDEX} then
                    click UI element {TRADE_PANEL_BUTTON_INDEX} of window mainWindowIndex
                    delay 0.9
                end if
            end if

            repeat with candidateIndex from 1 to count of windows
                try
                    if exists button {_applescript_string(SUBMIT_BUTTON_NAME)} of window candidateIndex then
                        set targetWindowIndex to candidateIndex
                        exit repeat
                    end if
                end try
            end repeat
        end if

        if targetWindowIndex = 0 then error "未找到同花顺交易买入窗口"

        set out to "target_window" & tab & targetWindowIndex & linefeed
        set elems to entire contents of window targetWindowIndex
        set idx to 0
        repeat with elem in elems
            set idx to idx + 1
            try
                set r to role of elem as text
                set n to ""
                set v to ""
                set xPos to ""
                set yPos to ""
                try
                    set n to name of elem as text
                end try
                try
                    set v to value of elem as text
                end try
                if n is not "" or v is not "" then
                    try
                        set posn to position of elem
                        set xPos to item 1 of posn as text
                        set yPos to item 2 of posn as text
                    end try
                    set out to out & idx & tab & r & tab & n & tab & v & tab & xPos & tab & yPos & linefeed
                end if
            end try
        end repeat
        return out
    end tell
end tell
"""


def parse_trade_panel_snapshot(raw: str) -> dict[str, Any]:
    structured = _parse_structured_snapshot(raw)
    if structured:
        return structured

    elements: list[dict[str, Any]] = []
    target_window = None
    for line in str(raw or "").splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0] == "target_window":
            target_window = _safe_int(parts[1])
            continue
        if len(parts) < 6:
            continue
        idx, role, name, value, x_pos, y_pos = parts[:6]
        elements.append(
            {
                "index": _safe_int(idx),
                "role": role,
                "name": _clean_ax_text(name),
                "value": _clean_ax_text(value),
                "x": _safe_float(x_pos),
                "y": _safe_float(y_pos),
            }
        )

    account = _parse_account(elements)
    order_form = _parse_order_form(elements)
    positions = _parse_positions(elements)
    return {
        "status": "synced",
        "target_window": target_window,
        "account": account,
        "order_form": order_form,
        "positions": positions,
        "position_count": len(positions),
        "raw_element_count": len(elements),
    }


def parse_broker_alert(raw: str) -> dict[str, Any]:
    texts: list[str] = []
    buttons: list[str] = []
    alert_window = None
    alert_sheet = None
    dismissed = False
    present = False
    for line in str(raw or "").splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0] == "status" and parts[1] == "absent":
            return {"status": "absent", "present": False, "dismissed": False, "message": ""}
        if len(parts) >= 3 and parts[0] == "alert":
            present = True
            alert_window = _safe_int(parts[1])
            alert_sheet = _safe_int(parts[2])
            continue
        if len(parts) >= 3 and parts[0] == "text":
            text = _clean_ax_text(parts[1]) or _clean_ax_text(parts[2])
            if text and text not in texts:
                texts.append(text)
            continue
        if len(parts) >= 3 and parts[0] == "button":
            text = _clean_ax_text(parts[1]) or _clean_ax_text(parts[2])
            if text and text not in buttons:
                buttons.append(text)
            continue
        if len(parts) >= 2 and parts[0] == "dismissed":
            dismissed = str(parts[1]).strip().lower() == "true"

    if not present:
        return {"status": "absent", "present": False, "dismissed": False, "message": ""}
    title = texts[0] if texts else ""
    message_parts = [text for text in texts if text != title]
    message = "\n".join(message_parts or texts)
    return {
        "status": "present",
        "present": True,
        "title": title,
        "message": message,
        "texts": texts,
        "buttons": buttons,
        "dismissed": dismissed,
        "window": alert_window,
        "sheet": alert_sheet,
        "raw": raw,
    }


def _broker_alert_present(alert: Optional[dict[str, Any]]) -> bool:
    return bool(isinstance(alert, dict) and alert.get("present"))


def _parse_structured_snapshot(raw: str) -> Optional[dict[str, Any]]:
    label_map = {
        "总资产": "total_asset",
        "总市值": "market_value",
        "总盈亏": "total_pnl",
        "当日盈亏": "today_pnl",
        "资金余额": "cash_balance",
        "可取金额": "withdrawable_cash",
        "可用金额": "available_cash",
    }
    account: dict[str, Any] = {}
    order_form: dict[str, Any] = {}
    positions: list[dict[str, Any]] = []
    target_window = None
    saw_structured_line = False

    for line in str(raw or "").splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0] == "target_window":
            target_window = _safe_int(parts[1])
            saw_structured_line = True
            continue
        if not parts:
            continue
        if parts[0] == "account" and len(parts) >= 3:
            saw_structured_line = True
            key = label_map.get(_clean_ax_text(parts[1]))
            if key:
                value = _parse_number(parts[2])
                if value is not None:
                    account[key] = value
            continue
        if parts[0] == "order" and len(parts) >= 6:
            saw_structured_line = True
            code = "".join(re.findall(r"\d", parts[1]))[-6:]
            order_form = {
                "code": code if len(code) == 6 else "",
                "name": _clean_ax_text(parts[2]),
                "limit_price": _parse_number(parts[3]),
                "quantity": _parse_number(parts[4]),
                "current_price": _parse_number(parts[5]) or _parse_number(parts[3]),
            }
            continue
        if parts[0] == "position" and len(parts) >= 14:
            saw_structured_line = True
            code = "".join(re.findall(r"\d", parts[1]))[-6:]
            if len(code) != 6:
                continue
            quantity = _parse_number(parts[7])
            if not quantity or quantity <= 0:
                continue
            positions.append(
                {
                    "code": code,
                    "name": _clean_ax_text(parts[2]),
                    "market_price": _parse_number(parts[3]),
                    "pnl": _parse_number(parts[4]),
                    "today_pnl": _parse_number(parts[5]),
                    "pnl_pct": _parse_number(parts[6]),
                    "actual_quantity": quantity,
                    "stock_balance": _parse_number(parts[8]),
                    "available_quantity": _parse_number(parts[9]),
                    "frozen_quantity": _parse_number(parts[10]),
                    "cost_price": _parse_number(parts[11]),
                    "market_value": _parse_number(parts[12]),
                    "position_pct": _parse_number(parts[13]),
                }
            )

    if not saw_structured_line:
        return None
    return {
        "status": "synced",
        "target_window": target_window,
        "account": account,
        "order_form": order_form,
        "positions": positions,
        "position_count": len(positions),
        "raw_element_count": len(str(raw or "").splitlines()),
    }


def verify_order_filled(
    before_snapshot: Optional[dict[str, Any]],
    after_snapshot: Optional[dict[str, Any]],
    code: str,
    shares: int,
    action_type: str = "buy",
) -> dict[str, Any]:
    clean_action_type = normalize_action_type(action_type)
    before_qty = position_quantity(before_snapshot, code)
    after_qty = position_quantity(after_snapshot, code)
    requested = int(shares or 0)
    if clean_action_type == "sell" and before_snapshot is None:
        return {
            "confirmed": False,
            "code": normalize_stock_code(code),
            "action_type": clean_action_type,
            "requested_shares": requested,
            "before_quantity": before_qty,
            "after_quantity": after_qty,
            "filled_delta": None,
            "reason": "before_snapshot_missing",
        }
    if clean_action_type == "sell":
        confirmed = after_qty is not None and after_qty <= max(0, before_qty - requested)
        reason = "position_quantity_decreased" if confirmed else "position_quantity_not_decreased"
        filled_delta = None if after_qty is None else before_qty - after_qty
    else:
        confirmed = after_qty is not None and after_qty >= before_qty + requested
        reason = "position_quantity_increased" if confirmed else "position_quantity_not_increased"
        filled_delta = None if after_qty is None else after_qty - before_qty
    return {
        "confirmed": bool(confirmed),
        "code": normalize_stock_code(code),
        "action_type": clean_action_type,
        "requested_shares": requested,
        "before_quantity": before_qty,
        "after_quantity": after_qty,
        "filled_delta": filled_delta,
        "reason": reason,
    }


def position_quantity(snapshot: Optional[dict[str, Any]], code: str) -> int:
    if not snapshot:
        return 0
    clean_code = normalize_stock_code(code)
    total = 0
    for position in snapshot.get("positions") or []:
        if normalize_stock_code(position.get("code")) == clean_code:
            total += int(round(_safe_float(position.get("actual_quantity"))))
    return total


def _parse_account(elements: list[dict[str, Any]]) -> dict[str, Any]:
    label_map = {
        "总资产": "total_asset",
        "总市值": "market_value",
        "总盈亏": "total_pnl",
        "当日盈亏": "today_pnl",
        "资金余额": "cash_balance",
        "可取金额": "withdrawable_cash",
        "可用金额": "available_cash",
    }
    account: dict[str, Any] = {}
    for index, element in enumerate(elements):
        text = _element_text(element)
        key = label_map.get(text)
        if not key:
            continue
        for candidate in elements[index + 1 : index + 6]:
            value = _numeric_text(candidate)
            if value is not None:
                account[key] = value
                break
    return account


def _parse_order_form(elements: list[dict[str, Any]]) -> dict[str, Any]:
    code_field = _nearest_element(elements, "AXTextField", 1268, 374)
    price_field = _nearest_element(elements, "AXTextField", 1287, 420)
    quantity_field = _nearest_element(elements, "AXTextField", 1287, 480)
    name_text = _nearest_element(elements, "AXStaticText", 1268, 397)
    current_price_text = _nearest_element(elements, "AXStaticText", 1426, 296)
    order_form = {
        "code": normalize_stock_code(code_field.get("value")) if code_field and code_field.get("value") else "",
        "name": _element_text(name_text) if name_text else "",
        "limit_price": _numeric_text(price_field) if price_field else None,
        "quantity": _numeric_text(quantity_field) if quantity_field else None,
        "current_price": _numeric_text(current_price_text) if current_price_text else None,
    }
    if not order_form["current_price"]:
        order_form["current_price"] = order_form["limit_price"]
    return order_form


def _parse_positions(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    header_names = {
        "证券代码",
        "证券名称",
        "市价",
        "盈亏",
        "当日盈亏",
        "浮动盈亏比(%)",
        "实际数量",
        "成本价",
        "市值",
        "仓位占比(%)",
    }
    headers = {
        _element_text(element): element
        for element in elements
        if element.get("role") == "AXButton" and _element_text(element) in header_names and element.get("x")
    }
    code_header = headers.get("证券代码")
    if not code_header:
        return []
    header_y = float(code_header.get("y") or 0)
    rows = []
    for element in elements:
        text = _element_text(element)
        if element.get("role") != "AXStaticText" or not re.fullmatch(r"\d{6}", text):
            continue
        if float(element.get("y") or 0) <= header_y:
            continue
        y_pos = float(element.get("y") or 0)
        row_elements = [
            item
            for item in elements
            if item.get("role") == "AXStaticText" and abs(float(item.get("y") or 0) - y_pos) <= 3
        ]
        position = {
            "code": text,
            "name": _value_near_header(row_elements, headers, "证券名称") or "",
            "market_price": _float_near_header(row_elements, headers, "市价"),
            "pnl": _float_near_header(row_elements, headers, "盈亏"),
            "today_pnl": _float_near_header(row_elements, headers, "当日盈亏"),
            "pnl_pct": _float_near_header(row_elements, headers, "浮动盈亏比(%)"),
            "actual_quantity": _float_near_header(row_elements, headers, "实际数量"),
            "cost_price": _float_near_header(row_elements, headers, "成本价"),
            "market_value": _float_near_header(row_elements, headers, "市值"),
            "position_pct": _float_near_header(row_elements, headers, "仓位占比(%)"),
        }
        if position["actual_quantity"] and position["actual_quantity"] > 0:
            rows.append(position)
    return rows


def _nearest_element(
    elements: list[dict[str, Any]],
    role: str,
    x_pos: float,
    y_pos: float,
    max_distance: float = 80,
) -> Optional[dict[str, Any]]:
    candidates = [element for element in elements if element.get("role") == role]
    best = None
    best_distance = float("inf")
    for element in candidates:
        dx = float(element.get("x") or 0) - x_pos
        dy = float(element.get("y") or 0) - y_pos
        distance = abs(dx) + abs(dy) * 3
        if distance < best_distance and distance <= max_distance:
            best = element
            best_distance = distance
    return best


def _value_near_header(row_elements: list[dict[str, Any]], headers: dict[str, dict[str, Any]], header: str) -> str:
    element = _nearest_to_x(row_elements, float((headers.get(header) or {}).get("x") or 0))
    return _element_text(element) if element else ""


def _float_near_header(row_elements: list[dict[str, Any]], headers: dict[str, dict[str, Any]], header: str) -> Optional[float]:
    value = _value_near_header(row_elements, headers, header)
    return _parse_number(value)


def _nearest_to_x(row_elements: list[dict[str, Any]], x_pos: float) -> Optional[dict[str, Any]]:
    if x_pos <= 0:
        return None
    best = None
    best_distance = float("inf")
    for element in row_elements:
        distance = abs(float(element.get("x") or 0) - x_pos)
        if distance < best_distance:
            best = element
            best_distance = distance
    return best if best_distance <= 35 else None


def _element_text(element: Optional[dict[str, Any]]) -> str:
    if not element:
        return ""
    return str(element.get("value") or element.get("name") or "").strip()


def _numeric_text(element: Optional[dict[str, Any]]) -> Optional[float]:
    return _parse_number(_element_text(element))


def _parse_number(value: Any) -> Optional[float]:
    text = str(value or "").replace(",", "").replace("￥", "").strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _clean_ax_text(value: str) -> str:
    text = str(value or "").strip()
    return "" if text == "missing value" else text


def _safe_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if math.isfinite(out) else 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def normalize_stock_code(stock_code: str) -> str:
    digits = "".join(re.findall(r"\d", str(stock_code or "")))
    if len(digits) < 6:
        raise ValueError(f"非法股票代码：{stock_code}")
    return digits[-6:]


def normalize_action_type(action_type: str) -> str:
    clean = str(action_type or "buy").strip().lower()
    if clean not in {"buy", "sell"}:
        raise ValueError(f"action_type 只支持 buy/sell：{action_type}")
    return clean


def normalize_shares(shares: int, action_type: str = "buy") -> int:
    clean_action_type = normalize_action_type(action_type)
    clean = int(shares)
    if clean_action_type == "sell":
        if clean <= 0:
            raise ValueError(f"卖出股数必须大于 0：{shares}")
        return clean
    if clean < 100:
        raise ValueError(f"买入股数小于 100，放弃交易：{shares}")
    if clean % 100 != 0:
        raise ValueError(f"买入股数必须是 100 股整数倍：{shares}")
    return clean


def normalize_limit_price(limit_price: float) -> str:
    try:
        clean = float(limit_price)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"委托价不是有效数字：{limit_price}") from exc
    if not math.isfinite(clean) or clean <= 0:
        raise ValueError(f"委托价必须大于 0：{limit_price}")
    return f"{clean:.3f}".rstrip("0").rstrip(".")


def _enter_sequence(count: int) -> str:
    lines = []
    for index in range(max(0, count)):
        lines.append(f"    key code 36 -- 二次确认回车 {index + 1}/{count}")
        lines.append("    delay 0.25")
    return "\n".join(lines)


def _applescript_string(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def main() -> None:
    parser = argparse.ArgumentParser(description="macOS 本地看盘软件股票代码跳转器")
    parser.add_argument("stock_code", help="6 位 A 股代码，例如 002747")
    parser.add_argument("--app-name", default="同花顺", help="目标 App 名称，例如 同花顺 或 同花顺(企业版)")
    parser.add_argument("--action-type", choices=["buy", "sell"], default="buy", help="交易方向，默认 buy")
    parser.add_argument("--shares", type=int, default=None, help="传入后自动输入数量并二次回车确认")
    parser.add_argument("--price", type=float, default=None, help="全自动委托价，通常传当前价")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要执行的 AppleScript，不激活 App、不敲键盘")
    args = parser.parse_args()
    aim_and_fire(
        args.stock_code,
        app_name=args.app_name,
        shares=args.shares,
        limit_price=args.price,
        action_type=args.action_type,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
