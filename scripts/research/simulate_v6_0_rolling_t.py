from __future__ import annotations

import argparse
import io
import json
import math
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
MINUTE_ROOT = Path("/Users/eudis/5min/organized_5min_pre_adj")
REPORT_PATH = BASE_DIR / "scripts" / "research" / "simulate_v6_0_rolling_t_latest.json"
DETAIL_PATH = BASE_DIR / "scripts" / "research" / "simulate_v6_0_rolling_t_trades.csv"

DEFAULT_START_DATE = "2025-01-02"
DEFAULT_END_DATE = "2026-01-28"
INITIAL_CAPITAL = 200_000.0
BASE_POSITION_CASH = 100_000.0
BUY_COST_RATE = 0.0005
SELL_COST_RATE = 0.0015
LOT_SIZE = 100

RSI_WINDOW = 14
BB_WINDOW = 20
BB_STD_MULT = 2.0
DEFAULT_CODES = {
    "603268": "松发股份",
    "002052": "同洲电子",
    "605255": "天普股份",
}

CODE_RE = re.compile(r"(\d{6})")
ZIP_DATE_RE = re.compile(r"(\d{8})_5min\.zip$")


@dataclass
class Account:
    cash: float
    free_shares: int
    locked_shares: int


@dataclass(frozen=True)
class Trade:
    code: str
    name: str
    datetime: str
    action: str
    price: float
    shares: int
    gross_amount: float
    friction_cost: float
    cash_after: float
    free_shares_after: int
    locked_shares_after: int
    reason: str


def normalize_code(value: Any) -> str:
    text = str(value or "")
    match = CODE_RE.search(text)
    return match.group(1) if match else text.zfill(6)[-6:]


def symbol_for_code(code: str) -> str:
    clean = normalize_code(code)
    prefix = "sh" if clean.startswith(("5", "6", "9")) else "sz"
    return f"{prefix}{clean}"


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def discover_zip_paths(minute_root: Path, start_date: str, end_date: str) -> list[tuple[str, Path]]:
    paths: list[tuple[str, Path]] = []
    for path in sorted((minute_root / "sh_sz").glob("*/*_5min.zip")):
        match = ZIP_DATE_RE.search(path.name)
        if not match:
            continue
        raw = match.group(1)
        trade_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        if start_date <= trade_date <= end_date:
            paths.append((trade_date, path))
    return paths


def load_stock_5m(code: str, minute_root: Path, start_date: str, end_date: str) -> pd.DataFrame:
    member = f"{symbol_for_code(code)}.csv"
    frames: list[pd.DataFrame] = []
    for _, zip_path in discover_zip_paths(minute_root, start_date, end_date):
        try:
            with zipfile.ZipFile(zip_path) as zf:
                if member not in zf.namelist():
                    continue
                raw = zf.read(member)
        except Exception:
            continue
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty:
            continue
        out = pd.DataFrame()
        out["datetime"] = pd.to_datetime(df.get("时间"), errors="coerce")
        out["open"] = pd.to_numeric(df.get("开盘价"), errors="coerce")
        out["high"] = pd.to_numeric(df.get("最高价"), errors="coerce")
        out["low"] = pd.to_numeric(df.get("最低价"), errors="coerce")
        out["close"] = pd.to_numeric(df.get("收盘价"), errors="coerce")
        out["volume"] = pd.to_numeric(df.get("成交量"), errors="coerce")
        out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
        if not out.empty:
            frames.append(out)
    if not frames:
        return pd.DataFrame()
    bars = pd.concat(frames, ignore_index=True, sort=False)
    bars = bars.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)
    bars["date"] = bars["datetime"].dt.strftime("%Y-%m-%d")
    return add_indicators(bars)


def add_indicators(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.rolling(RSI_WINDOW, min_periods=RSI_WINDOW).mean()
    avg_loss = loss.rolling(RSI_WINDOW, min_periods=RSI_WINDOW).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.where(avg_loss != 0.0, 100.0)
    rsi = rsi.where(avg_gain != 0.0, 0.0)
    out["rsi_14"] = rsi
    mid = close.rolling(BB_WINDOW, min_periods=BB_WINDOW).mean()
    std = close.rolling(BB_WINDOW, min_periods=BB_WINDOW).std(ddof=0)
    out["bb_mid"] = mid
    out["bb_upper"] = mid + BB_STD_MULT * std
    out["bb_lower"] = mid - BB_STD_MULT * std
    return out


def buy_lot(account: Account, price: float, budget: float, code: str, name: str, ts: pd.Timestamp, reason: str) -> Trade | None:
    if price <= 0 or budget <= 0 or account.cash <= 0:
        return None
    usable = min(float(budget), account.cash)
    shares = int((usable / (price * (1.0 + BUY_COST_RATE))) // LOT_SIZE) * LOT_SIZE
    if shares <= 0:
        return None
    gross = float(shares * price)
    friction = gross * BUY_COST_RATE
    total_cost = gross + friction
    if total_cost > account.cash + 1e-6:
        return None
    account.cash -= total_cost
    account.locked_shares += shares
    return Trade(
        code=code,
        name=name,
        datetime=ts.strftime("%Y-%m-%d %H:%M:%S"),
        action="BUY",
        price=round(price, 4),
        shares=shares,
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        free_shares_after=account.free_shares,
        locked_shares_after=account.locked_shares,
        reason=reason,
    )


def sell_lot(account: Account, price: float, ratio: float, code: str, name: str, ts: pd.Timestamp, reason: str) -> Trade | None:
    if price <= 0 or account.free_shares <= 0:
        return None
    shares = int((account.free_shares * ratio) // LOT_SIZE) * LOT_SIZE
    if shares <= 0 and account.free_shares >= LOT_SIZE:
        shares = LOT_SIZE
    if shares <= 0:
        return None
    shares = min(shares, account.free_shares)
    gross = float(shares * price)
    friction = gross * SELL_COST_RATE
    proceeds = gross - friction
    account.cash += proceeds
    account.free_shares -= shares
    return Trade(
        code=code,
        name=name,
        datetime=ts.strftime("%Y-%m-%d %H:%M:%S"),
        action="SELL",
        price=round(price, 4),
        shares=shares,
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        free_shares_after=account.free_shares,
        locked_shares_after=account.locked_shares,
        reason=reason,
    )


def simulate_rolling_t(
    code: str,
    name: str,
    bars: pd.DataFrame,
    initial_capital: float,
    base_position_cash: float,
    buy_cash_ratio: float,
    sell_share_ratio: float,
) -> dict[str, Any]:
    account = Account(cash=float(initial_capital), free_shares=0, locked_shares=0)
    trades: list[Trade] = []
    daily_assets: list[dict[str, Any]] = []
    if bars.empty:
        raise ValueError(f"{code} 没有 5m 数据")

    first_day = True
    last_close = float(bars["close"].iloc[-1])
    for trade_date, day in bars.groupby("date", sort=True):
        day = day.sort_values("datetime")
        if first_day:
            first_bar = day.iloc[0]
            trade = buy_lot(
                account,
                price=float(first_bar["open"]),
                budget=base_position_cash,
                code=code,
                name=name,
                ts=pd.Timestamp(first_bar["datetime"]),
                reason="建底仓_半仓",
            )
            if trade is not None:
                trades.append(trade)
            first_day = False

        for row in day.itertuples(index=False):
            close = float(row.close)
            rsi = float(row.rsi_14) if pd.notna(row.rsi_14) else float("nan")
            bb_upper = float(row.bb_upper) if pd.notna(row.bb_upper) else float("nan")
            bb_lower = float(row.bb_lower) if pd.notna(row.bb_lower) else float("nan")
            ts = pd.Timestamp(row.datetime)

            if math.isfinite(rsi) and math.isfinite(bb_lower) and rsi < 25.0 and close < bb_lower:
                trade = buy_lot(
                    account,
                    price=close,
                    budget=account.cash * buy_cash_ratio,
                    code=code,
                    name=name,
                    ts=ts,
                    reason="RSI<25且跌破布林下轨_正向T买入",
                )
                if trade is not None:
                    trades.append(trade)

            if math.isfinite(rsi) and math.isfinite(bb_upper) and rsi > 75.0 and close > bb_upper:
                trade = sell_lot(
                    account,
                    price=close,
                    ratio=sell_share_ratio,
                    code=code,
                    name=name,
                    ts=ts,
                    reason="RSI>75且突破布林上轨_反向T卖出",
                )
                if trade is not None:
                    trades.append(trade)

        last_close = float(day["close"].iloc[-1])
        account.free_shares += account.locked_shares
        account.locked_shares = 0
        daily_assets.append(
            {
                "date": trade_date,
                "asset": account.cash + account.free_shares * last_close,
                "cash": account.cash,
                "free_shares": account.free_shares,
                "close": last_close,
            }
        )

    final_asset = float(account.cash + (account.free_shares + account.locked_shares) * last_close)
    buy_hold = simulate_buy_hold(bars, initial_capital)
    buy_count = sum(1 for item in trades if item.action == "BUY")
    sell_count = sum(1 for item in trades if item.action == "SELL")
    total_friction = sum(item.friction_cost for item in trades)
    trade_rows = [item.__dict__ for item in trades]
    return {
        "code": code,
        "name": name,
        "start": str(bars["date"].iloc[0]),
        "end": str(bars["date"].iloc[-1]),
        "bar_count": int(len(bars)),
        "day_count": int(bars["date"].nunique()),
        "initial_capital": round(float(initial_capital), 2),
        "rolling_t_final_asset": round(final_asset, 2),
        "rolling_t_return_pct": round((final_asset / initial_capital - 1.0) * 100.0, 4),
        "buy_hold_final_asset": round(buy_hold["final_asset"], 2),
        "buy_hold_return_pct": round(buy_hold["return_pct"], 4),
        "excess_return_pct": round((final_asset / initial_capital - 1.0) * 100.0 - buy_hold["return_pct"], 4),
        "cash": round(account.cash, 2),
        "free_shares": int(account.free_shares),
        "locked_shares": int(account.locked_shares),
        "last_close": round(last_close, 4),
        "trade_count": int(len(trades)),
        "buy_count": int(buy_count),
        "sell_count": int(sell_count),
        "total_friction_cost": round(float(total_friction), 2),
        "trade_rows": trade_rows,
        "buy_hold": buy_hold,
    }


def simulate_buy_hold(bars: pd.DataFrame, initial_capital: float) -> dict[str, Any]:
    first_bar = bars.iloc[0]
    buy_price = float(first_bar["open"])
    final_close = float(bars["close"].iloc[-1])
    shares = int((initial_capital / (buy_price * (1.0 + BUY_COST_RATE))) // LOT_SIZE) * LOT_SIZE
    gross = shares * buy_price
    friction = gross * BUY_COST_RATE
    cash_left = initial_capital - gross - friction
    final_asset = cash_left + shares * final_close
    return {
        "shares": int(shares),
        "buy_price": round(buy_price, 4),
        "final_close": round(final_close, 4),
        "cash_left": round(cash_left, 2),
        "buy_friction_cost": round(friction, 2),
        "final_asset": round(final_asset, 2),
        "return_pct": round((final_asset / initial_capital - 1.0) * 100.0, 4),
    }


def aggregate(results: list[dict[str, Any]], initial_capital: float) -> dict[str, Any]:
    if not results:
        return {}
    total_initial = initial_capital * len(results)
    rolling_asset = sum(float(item["rolling_t_final_asset"]) for item in results)
    hold_asset = sum(float(item["buy_hold_final_asset"]) for item in results)
    return {
        "stock_count": int(len(results)),
        "total_initial_capital": round(total_initial, 2),
        "rolling_t_total_asset": round(rolling_asset, 2),
        "rolling_t_return_pct": round((rolling_asset / total_initial - 1.0) * 100.0, 4),
        "buy_hold_total_asset": round(hold_asset, 2),
        "buy_hold_return_pct": round((hold_asset / total_initial - 1.0) * 100.0, 4),
        "excess_return_pct": round((rolling_asset / total_initial - hold_asset / total_initial) * 100.0, 4),
        "trade_count": int(sum(int(item["trade_count"]) for item in results)),
        "buy_count": int(sum(int(item["buy_count"]) for item in results)),
        "sell_count": int(sum(int(item["sell_count"]) for item in results)),
        "total_friction_cost": round(float(sum(float(item["total_friction_cost"]) for item in results)), 2),
    }


def parse_codes(raw: str) -> dict[str, str]:
    if not raw.strip():
        return dict(DEFAULT_CODES)
    out: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            code, name = item.split(":", 1)
            out[normalize_code(code)] = name.strip() or normalize_code(code)
        else:
            code = normalize_code(item)
            out[code] = DEFAULT_CODES.get(code, code)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="V6.0 pure-rule rolling T+0 simulator with T+1 share lock.")
    parser.add_argument("--minute-root", default=str(MINUTE_ROOT))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--codes", default="", help="Comma list, optionally code:name. Default: high-vol historical picks.")
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    parser.add_argument("--base-position-cash", type=float, default=BASE_POSITION_CASH)
    parser.add_argument("--buy-cash-ratio", type=float, default=0.5)
    parser.add_argument("--sell-share-ratio", type=float, default=0.5)
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--detail-path", default=str(DETAIL_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    codes = parse_codes(args.codes)
    results: list[dict[str, Any]] = []
    trade_details: list[dict[str, Any]] = []

    print("========== V6.0 Rolling T Pure Rule Simulator ==========")
    print(f"Date Range     : {args.start_date} -> {args.end_date}")
    print(f"Initial Capital: {float(args.initial_capital):,.2f} per stock")
    print(f"Buy Cost       : {BUY_COST_RATE * 100:.3f}%")
    print(f"Sell Cost      : {SELL_COST_RATE * 100:.3f}%")
    print(f"Codes          : {', '.join([f'{code} {name}' for code, name in codes.items()])}")

    for code, name in codes.items():
        bars = load_stock_5m(code, Path(args.minute_root), args.start_date, args.end_date)
        if bars.empty:
            print(f"[Skip] {code} {name}: no 5m bars")
            continue
        result = simulate_rolling_t(
            code=code,
            name=name,
            bars=bars,
            initial_capital=float(args.initial_capital),
            base_position_cash=float(args.base_position_cash),
            buy_cash_ratio=float(args.buy_cash_ratio),
            sell_share_ratio=float(args.sell_share_ratio),
        )
        results.append({key: value for key, value in result.items() if key != "trade_rows"})
        trade_details.extend(result["trade_rows"])
        print(
            f"[{code} {name}] 做T={result['rolling_t_return_pct']:.2f}% "
            f"死拿={result['buy_hold_return_pct']:.2f}% "
            f"超额={result['excess_return_pct']:.2f}% "
            f"trades={result['trade_count']} friction={result['total_friction_cost']:.2f}",
            flush=True,
        )

    portfolio = aggregate(results, float(args.initial_capital))
    elapsed = time.perf_counter() - started
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "scripts/research/simulate_v6_0_rolling_t.py",
        "minute_root": str(Path(args.minute_root)),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "params": {
            "initial_capital": float(args.initial_capital),
            "base_position_cash": float(args.base_position_cash),
            "buy_cost_rate": BUY_COST_RATE,
            "sell_cost_rate": SELL_COST_RATE,
            "buy_cash_ratio": float(args.buy_cash_ratio),
            "sell_share_ratio": float(args.sell_share_ratio),
            "rsi_window": RSI_WINDOW,
            "bb_window": BB_WINDOW,
            "bb_std_mult": BB_STD_MULT,
            "lot_size": LOT_SIZE,
        },
        "portfolio": portfolio,
        "stocks": results,
        "elapsed_seconds": round(elapsed, 3),
    }
    report_path = Path(args.report_path)
    detail_path = Path(args.detail_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(trade_details).to_csv(detail_path, index=False)

    print("\n---------- Final Battle Report ----------")
    if portfolio:
        print(
            f"组合做T收益率    : {portfolio['rolling_t_return_pct']:.2f}% "
            f"({portfolio['rolling_t_total_asset']:,.2f})"
        )
        print(
            f"组合死拿收益率   : {portfolio['buy_hold_return_pct']:.2f}% "
            f"({portfolio['buy_hold_total_asset']:,.2f})"
        )
        print(f"做T相对死拿超额  : {portfolio['excess_return_pct']:.2f}%")
        print(
            f"交易频次         : total={portfolio['trade_count']} "
            f"buy={portfolio['buy_count']} sell={portfolio['sell_count']}"
        )
        print(f"总摩擦成本       : {portfolio['total_friction_cost']:,.2f}")
    print(f"Report Path      : {report_path}")
    print(f"Trade Detail     : {detail_path}")
    print(f"Elapsed Seconds  : {elapsed:.3f}")
    print("========================================================\n")


if __name__ == "__main__":
    main()
