from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Optional

import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from quant_core.config import MIN_KLINE_DIR, PAUSED_STRATEGY_TYPES, SQLITE_PATH
except Exception:  # pragma: no cover - standalone fallback
    SQLITE_PATH = BASE_DIR / "data" / "core_db" / "quant_workstation.sqlite3"
    MIN_KLINE_DIR = BASE_DIR / "data" / "min_kline"
    PAUSED_STRATEGY_TYPES = ("右侧主升浪", "中线超跌反转")

PRE_ADJUSTED_5M_DIR = Path("/Users/eudis/5min/organized_5min_pre_adj")
PROJECT_HOT_5M_DIR = MIN_KLINE_DIR / "5m"
CACHE_DIR = BASE_DIR / "data" / "strategy_cache"
_PRE_ADJUSTED_SYMBOL_CACHE: dict[tuple[str, str], pd.DataFrame] = {}
_PRE_ADJUSTED_CACHE_READY: set[str] = set()

REGULAR_ARMY_STRATEGIES = {"右侧主升浪", "中线超跌反转"}
SNIPER_BREAKOUT_STRATEGIES = {"全局动量狙击", "尾盘突破"}
STRATEGY_PRIORITY = {
    "全局动量狙击": 4,
    "右侧主升浪": 3,
    "中线超跌反转": 2,
    "尾盘突破": 1,
    "首阴低吸": 0,
}

REGULAR_INTRADAY_DISASTER_STOP_PCT = 0.06
SNIPER_INTRADAY_DISASTER_STOP_PCT = 0.04
REGULAR_EOD_STRUCTURAL_STOP_PCT = 0.03
SNIPER_EOD_STRUCTURAL_STOP_PCT = 0.015
TRAILING_ARM_PCT = 0.04
TRAILING_PULLBACK_PCT = 0.02
DEFAULT_BUY_TIME = time(14, 50)
EOD_STRUCTURAL_STOP_TIMES = {time(14, 50), time(14, 55)}


@dataclass(frozen=True)
class BuyRecord:
    pick_id: int
    code: str
    name: str
    buy_date: str
    selected_at: str
    cost_price: float
    strategy_type: str
    tier: str
    target_date: str
    win_rate: float
    selection_change: float
    snapshot_vol_ratio: float
    suggested_position: Optional[float]
    close_date: str
    close_price: Optional[float]
    close_return_pct: Optional[float]
    close_reason: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class SimulationResult:
    pick_id: int
    code: str
    name: str
    buy_date: str
    strategy_type: str
    tier: str
    cost_price: float
    coverage_status: str
    exit_reason: str
    exit_time: Optional[str]
    exit_price: Optional[float]
    yield_pct: Optional[float]
    highest_price: Optional[float]
    highest_gain_pct: Optional[float]
    bars_replayed: int
    t3_date: Optional[str]
    warning: str = ""


def previous_month_27(today: Optional[date] = None) -> str:
    current = today or date.today()
    year = current.year
    month = current.month - 1
    if month <= 0:
        month = 12
        year -= 1
    return date(year, month, 27).isoformat()


def normalize_code(value: Any) -> str:
    text = str(value or "")
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else text.zfill(6)[-6:]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def safe_optional_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def safe_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def load_buy_records(db_path: Path, start_date: str, end_date: str) -> list[BuyRecord]:
    with connect_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, selection_date, target_date, selected_at, code, name,
                   strategy_type, win_rate, selection_change, snapshot_price,
                   selection_price, snapshot_vol_ratio, suggested_position,
                   tier, close_date, close_price, close_return_pct, close_reason,
                   raw_json
            FROM daily_picks
            WHERE selection_date >= ?
              AND selection_date <= ?
            ORDER BY selection_date ASC, id ASC
            """,
            (start_date, end_date),
        ).fetchall()

    records: list[BuyRecord] = []
    for row in rows:
        raw = safe_json(row["raw_json"])
        winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
        cost_price = safe_float(row["snapshot_price"]) or safe_float(row["selection_price"])
        if cost_price <= 0:
            print(f"⚠️ [Warning] daily_picks#{row['id']} 缺少有效 cost_price，已跳过。")
            continue
        strategy_type = str(row["strategy_type"] or winner.get("strategy_type") or "未知策略")
        if strategy_type in PAUSED_STRATEGY_TYPES:
            continue
        tier = str(row["tier"] or winner.get("selection_tier") or "base")
        name = str(row["name"] or winner.get("name") or row["code"])
        suggested_position = row["suggested_position"]
        if suggested_position is None:
            suggested_position = winner.get("suggested_position")
        suggested_position_float = safe_optional_float(suggested_position)
        records.append(
            BuyRecord(
                pick_id=int(row["id"]),
                code=normalize_code(row["code"]),
                name=name,
                buy_date=str(row["selection_date"])[:10],
                selected_at=str(row["selected_at"] or ""),
                cost_price=float(cost_price),
                strategy_type=strategy_type,
                tier=tier,
                target_date=str(row["target_date"] or "")[:10],
                win_rate=safe_float(row["win_rate"]),
                selection_change=safe_float(row["selection_change"]),
                snapshot_vol_ratio=safe_float(row["snapshot_vol_ratio"]),
                suggested_position=suggested_position_float,
                close_date=str(row["close_date"] or "")[:10],
                close_price=safe_optional_float(row["close_price"]),
                close_return_pct=safe_optional_float(row["close_return_pct"]),
                close_reason=str(row["close_reason"] or ""),
                raw=raw,
            )
        )
    return records


def _record_winner(record: BuyRecord) -> dict[str, Any]:
    raw = record.raw if isinstance(record.raw, dict) else {}
    winner = raw.get("winner")
    return winner if isinstance(winner, dict) else {}


def _first_record_number(record: BuyRecord, keys: tuple[str, ...], default: float = 0.0) -> float:
    winner = _record_winner(record)
    for key in keys:
        value = winner.get(key)
        if value is None:
            value = record.raw.get(key) if isinstance(record.raw, dict) else None
        parsed = safe_optional_float(value)
        if parsed is not None:
            return parsed
    return default


def record_top1_sort_key(record: BuyRecord) -> tuple[float, float, float, float, float]:
    score = _first_record_number(
        record,
        (
            "sort_score",
            "selection_score",
            "composite_score",
            "global_probability_pct",
            "expected_t3_max_gain_pct",
            "expected_premium",
        ),
        record.win_rate,
    )
    expected = _first_record_number(record, ("expected_t3_max_gain_pct", "expected_premium"), 0.0)
    return (
        float(STRATEGY_PRIORITY.get(record.strategy_type, 0)),
        score,
        expected,
        record.win_rate,
        -float(record.pick_id),
    )


def collapse_records_to_strategy_top1(records: list[BuyRecord]) -> list[BuyRecord]:
    by_date_strategy: dict[tuple[str, str], list[BuyRecord]] = {}
    for record in records:
        by_date_strategy.setdefault((record.buy_date, record.strategy_type), []).append(record)

    selected: list[BuyRecord] = []
    for key in sorted(by_date_strategy):
        strategy_records = by_date_strategy[key]
        selected.append(max(strategy_records, key=record_top1_sort_key))

    dropped = len(records) - len(selected)
    if dropped > 0:
        print(
            f"[PickMode] daily_strategy_top1 raw={len(records)} selected={len(selected)} dropped_legacy_extra={dropped}",
            flush=True,
        )
    return selected


def load_trading_dates(db_path: Path) -> list[str]:
    with connect_db(db_path) as conn:
        rows = conn.execute("SELECT DISTINCT date FROM stock_daily ORDER BY date ASC").fetchall()
    return [str(row["date"])[:10] for row in rows if row["date"]]


def t_plus_n_date(buy_date: str, trading_dates: list[str], n: int = 3) -> Optional[str]:
    future = [item for item in trading_dates if item > buy_date]
    if len(future) >= n:
        return future[n - 1]
    return None


def minute_paths_for_code(minute_root: Path, code: str) -> list[Path]:
    clean = normalize_code(code)
    prefix = "sh" if clean.startswith("6") else "sz"
    candidates = [
        minute_root / f"{clean}.parquet",
        minute_root / f"{prefix}{clean}.parquet",
        minute_root / "5m" / f"{clean}.parquet",
        minute_root / "5m" / f"{prefix}{clean}.parquet",
    ]
    return [path for path in candidates if path.exists()]


def is_pre_adjusted_zip_root(minute_root: Path) -> bool:
    return (minute_root / "sh_sz").exists()


def symbol_for_code(code: str) -> str:
    clean = normalize_code(code)
    prefix = "sh" if clean.startswith(("5", "6", "9")) else "sz"
    return f"{prefix}{clean}"


def iter_pre_adjusted_zip_paths(minute_root: Path, start_ts: pd.Timestamp, end_ts: Optional[pd.Timestamp]) -> list[Path]:
    root = minute_root / "sh_sz"
    paths: list[Path] = []
    for path in sorted(root.glob("*/*_5min.zip")):
        date_text = path.name.split("_", 1)[0]
        if len(date_text) != 8:
            continue
        day = pd.Timestamp(f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:]}")
        if day.date() < start_ts.date():
            continue
        if end_ts is not None and day.date() > end_ts.date():
            continue
        paths.append(path)
    return paths


def read_pre_adjusted_member_from_zip(zf: zipfile.ZipFile, member_name: str) -> pd.DataFrame:
    with zf.open(member_name) as handle:
        raw = pd.read_csv(handle, encoding="utf-8-sig")
    if raw.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["datetime"] = pd.to_datetime(raw.get("时间"), errors="coerce")
    out["open"] = pd.to_numeric(raw.get("开盘价"), errors="coerce")
    out["high"] = pd.to_numeric(raw.get("最高价"), errors="coerce")
    out["low"] = pd.to_numeric(raw.get("最低价"), errors="coerce")
    out["close"] = pd.to_numeric(raw.get("收盘价"), errors="coerce")
    out["volume"] = pd.to_numeric(raw.get("成交量"), errors="coerce") * 100.0
    out["amount"] = pd.to_numeric(raw.get("成交额"), errors="coerce")
    out["source"] = "organized_5min_pre_adj"
    return out.dropna(subset=["datetime", "open", "high", "low", "close"])


def read_pre_adjusted_member(zip_path: Path, member_name: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        if member_name not in zf.namelist():
            return pd.DataFrame()
        return read_pre_adjusted_member_from_zip(zf, member_name)


def prime_pre_adjusted_zip_cache(records: list[BuyRecord], minute_root: Path, replay_end_date: Optional[str]) -> None:
    if not records or not is_pre_adjusted_zip_root(minute_root):
        return
    root_key = str(minute_root.resolve())
    if root_key in _PRE_ADJUSTED_CACHE_READY:
        return

    start_ts = min(selected_at_timestamp(record) for record in records)
    replay_end = pd.Timestamp(f"{replay_end_date} 15:00:00") if replay_end_date else None
    paths = iter_pre_adjusted_zip_paths(minute_root, start_ts, replay_end)
    symbols = sorted({symbol_for_code(record.code) for record in records})
    wanted_members = {f"{symbol}.csv": symbol for symbol in symbols}
    frames_by_symbol: dict[str, list[pd.DataFrame]] = {symbol: [] for symbol in symbols}

    print(
        f"[DataSource] pre_adjusted_zip root={minute_root} zips={len(paths)} symbols={len(symbols)}",
        flush=True,
    )
    for path in paths:
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
                for member_name, symbol in wanted_members.items():
                    if member_name not in names:
                        continue
                    frame = read_pre_adjusted_member_from_zip(zf, member_name)
                    if not frame.empty:
                        frames_by_symbol[symbol].append(frame)
        except Exception as exc:
            print(f"⚠️ [Warning] 读取前复权 zip 失败：{path}：{exc}", flush=True)

    for symbol, frames in frames_by_symbol.items():
        cache_key = (root_key, symbol)
        if not frames:
            _PRE_ADJUSTED_SYMBOL_CACHE[cache_key] = pd.DataFrame()
            continue
        out = pd.concat(frames, ignore_index=True, sort=False)
        out = out.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)
        out["trade_date"] = out["datetime"].dt.strftime("%Y-%m-%d")
        out["trade_time"] = out["datetime"].dt.strftime("%H:%M:%S")
        _PRE_ADJUSTED_SYMBOL_CACHE[cache_key] = out
    _PRE_ADJUSTED_CACHE_READY.add(root_key)


def selected_at_timestamp(record: BuyRecord) -> pd.Timestamp:
    parsed = pd.to_datetime(record.selected_at, errors="coerce")
    if pd.notna(parsed):
        return pd.Timestamp(parsed)
    return pd.Timestamp.combine(pd.Timestamp(record.buy_date).date(), DEFAULT_BUY_TIME)


def normalize_5m_frame(
    df: pd.DataFrame,
    record: BuyRecord,
    replay_end_date: Optional[str],
    source_label: str,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    if "datetime" not in df.columns:
        print(f"⚠️ [Warning] {record.name}({record.code}) {source_label} 缺少 datetime 字段，已跳过。")
        return pd.DataFrame()

    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"])
    for col in ("open", "high", "low", "close"):
        if col not in out.columns:
            print(f"⚠️ [Warning] {record.name}({record.code}) {source_label} 缺少 {col} 字段，已跳过。")
            return pd.DataFrame()
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close"])
    if out.empty:
        return out
    if "source" not in out.columns:
        out["source"] = source_label

    buy_ts = selected_at_timestamp(record)
    mask = out["datetime"] > buy_ts
    if replay_end_date:
        replay_end = pd.Timestamp(f"{replay_end_date} 15:00:00")
        mask = mask & (out["datetime"] <= replay_end)
    out = out.loc[mask].copy()
    if out.empty:
        return out

    out = out.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)
    out["trade_date"] = out["datetime"].dt.strftime("%Y-%m-%d")
    out["trade_time"] = out["datetime"].dt.strftime("%H:%M:%S")
    return out


def load_parquet_5m_window(
    record: BuyRecord,
    minute_root: Path,
    replay_end_date: Optional[str],
    source_label: str,
    warn_missing: bool = True,
) -> pd.DataFrame:
    paths = minute_paths_for_code(minute_root, record.code)
    if not paths:
        if warn_missing:
            print(f"⚠️ [Warning] {record.name}({record.code}) 找不到 {source_label} Parquet，已跳过。")
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            print(f"⚠️ [Warning] {record.name}({record.code}) 读取 {path} 失败：{exc}")
    if not frames:
        return pd.DataFrame()
    return normalize_5m_frame(pd.concat(frames, ignore_index=True, sort=False), record, replay_end_date, source_label)


def load_project_hot_5m_window(record: BuyRecord, replay_end_date: Optional[str]) -> pd.DataFrame:
    if not PROJECT_HOT_5M_DIR.exists():
        return pd.DataFrame()
    return load_parquet_5m_window(
        record,
        PROJECT_HOT_5M_DIR,
        replay_end_date,
        source_label="项目内每日热更新 5m",
        warn_missing=False,
    )


def merge_5m_windows(primary: pd.DataFrame, supplemental: pd.DataFrame) -> pd.DataFrame:
    if primary.empty:
        return supplemental.copy()
    if supplemental.empty:
        return primary.copy()

    primary_times = set(pd.to_datetime(primary["datetime"], errors="coerce").dropna())
    extra = supplemental.loc[~pd.to_datetime(supplemental["datetime"], errors="coerce").isin(primary_times)].copy()
    if extra.empty:
        return primary.sort_values("datetime").reset_index(drop=True)

    out = pd.concat([primary, extra], ignore_index=True, sort=False)
    out = out.sort_values("datetime").drop_duplicates("datetime", keep="first").reset_index(drop=True)
    out["trade_date"] = pd.to_datetime(out["datetime"]).dt.strftime("%Y-%m-%d")
    out["trade_time"] = pd.to_datetime(out["datetime"]).dt.strftime("%H:%M:%S")
    return out


def load_5m_window(record: BuyRecord, minute_root: Path, replay_end_date: Optional[str]) -> pd.DataFrame:
    if is_pre_adjusted_zip_root(minute_root):
        cold = load_pre_adjusted_zip_window(record, minute_root, replay_end_date, warn_missing=False)
        hot = load_project_hot_5m_window(record, replay_end_date)
        merged = merge_5m_windows(cold, hot)
        if merged.empty:
            print(
                f"⚠️ [Warning] {record.name}({record.code}) 前复权冷库与每日热更新 5m 均无覆盖，已跳过。"
            )
        return merged

    local = load_parquet_5m_window(
        record,
        minute_root,
        replay_end_date,
        source_label="本地 5m",
        warn_missing=True,
    )
    if local.empty and minute_root.resolve() != PROJECT_HOT_5M_DIR.resolve():
        hot = load_project_hot_5m_window(record, replay_end_date)
        if not hot.empty:
            return hot
    return local


def load_pre_adjusted_zip_window(
    record: BuyRecord,
    minute_root: Path,
    replay_end_date: Optional[str],
    warn_missing: bool = True,
) -> pd.DataFrame:
    buy_ts = selected_at_timestamp(record)
    replay_end = pd.Timestamp(f"{replay_end_date} 15:00:00") if replay_end_date else None
    symbol = symbol_for_code(record.code)
    member_name = f"{symbol}.csv"
    root_key = str(minute_root.resolve())
    cache_key = (root_key, symbol)
    if root_key in _PRE_ADJUSTED_CACHE_READY:
        cached = _PRE_ADJUSTED_SYMBOL_CACHE.get(cache_key, pd.DataFrame())
        if cached.empty:
            if warn_missing:
                print(f"⚠️ [Warning] {record.name}({record.code}) 前复权缓存中找不到成员 {member_name}，已跳过。")
            return pd.DataFrame()
        mask = cached["datetime"] > buy_ts
        if replay_end is not None:
            mask = mask & (cached["datetime"] <= replay_end)
        return cached.loc[mask].copy().reset_index(drop=True)

    paths = iter_pre_adjusted_zip_paths(minute_root, buy_ts, replay_end)
    if not paths:
        if warn_missing:
            print(
                f"⚠️ [Warning] {record.name}({record.code}) 前复权 5m 目录无覆盖 zip："
                f"{minute_root}/sh_sz，窗口 {buy_ts.date()} -> {replay_end_date or 'latest'}，已跳过。"
            )
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frame = read_pre_adjusted_member(path, member_name)
        except Exception as exc:
            print(f"⚠️ [Warning] {record.name}({record.code}) 读取前复权 5m {path.name}:{member_name} 失败：{exc}")
            continue
        if not frame.empty:
            frames.append(frame)
    if not frames:
        if warn_missing:
            print(f"⚠️ [Warning] {record.name}({record.code}) 前复权 5m zip 找不到成员 {member_name}，已跳过。")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True, sort=False)
    mask = out["datetime"] > buy_ts
    if replay_end is not None:
        mask = mask & (out["datetime"] <= replay_end)
    out = out.loc[mask].copy()
    if out.empty:
        return out
    out = out.sort_values("datetime").drop_duplicates("datetime", keep="last").reset_index(drop=True)
    out["trade_date"] = out["datetime"].dt.strftime("%Y-%m-%d")
    out["trade_time"] = out["datetime"].dt.strftime("%H:%M:%S")
    return out


def infer_t3_date(record: BuyRecord, trading_dates: list[str], bars: pd.DataFrame) -> Optional[str]:
    calendar_t3 = t_plus_n_date(record.buy_date, trading_dates, n=3)
    if calendar_t3:
        return calendar_t3
    if bars.empty or "trade_date" not in bars.columns:
        return None
    bar_dates = sorted({str(item) for item in bars["trade_date"].dropna().unique() if str(item) > record.buy_date})
    if len(bar_dates) >= 3:
        return bar_dates[2]
    return None


def t3_close_bar_index(bars: pd.DataFrame, t3_date: Optional[str]) -> Optional[int]:
    if not t3_date:
        return None
    t3_rows = bars.loc[bars["trade_date"] == t3_date]
    if t3_rows.empty:
        return None
    last_idx = int(t3_rows.index[-1])
    last_ts = pd.Timestamp(t3_rows.loc[last_idx, "datetime"])
    if last_ts.time() < time(14, 55):
        return None
    return last_idx


def is_sniper_record(record: BuyRecord) -> bool:
    strategy = record.strategy_type
    if strategy in SNIPER_BREAKOUT_STRATEGIES:
        return True
    if strategy in REGULAR_ARMY_STRATEGIES:
        return False
    return record.tier == "dynamic_floor"


def intraday_disaster_stop_config(record: BuyRecord) -> tuple[str, float]:
    if is_sniper_record(record):
        return "盘中暴雷止损_敢死队_4pct", 1.0 - SNIPER_INTRADAY_DISASTER_STOP_PCT
    return "盘中暴雷止损_正规军_6pct", 1.0 - REGULAR_INTRADAY_DISASTER_STOP_PCT


def eod_structural_stop_config(record: BuyRecord) -> tuple[str, float]:
    if is_sniper_record(record):
        return "尾盘破位卖出_敢死队_1_5pct", 1.0 - SNIPER_EOD_STRUCTURAL_STOP_PCT
    return "尾盘破位卖出_正规军_3pct", 1.0 - REGULAR_EOD_STRUCTURAL_STOP_PCT


def strict_trailing_exit_price(highest_price: float, close_price: float) -> float:
    trigger_price = highest_price * (1.0 - TRAILING_PULLBACK_PCT)
    return min(trigger_price, close_price)


def simulate_one(
    record: BuyRecord,
    minute_root: Path,
    trading_dates: list[str],
    replay_end_date: Optional[str],
) -> SimulationResult:
    bars = load_5m_window(record, minute_root, replay_end_date)
    if bars.empty:
        fallback = historical_t3_fallback_result(record, record.cost_price, 0, None, "缺失5m数据")
        if fallback:
            return fallback
        return SimulationResult(
            pick_id=record.pick_id,
            code=record.code,
            name=record.name,
            buy_date=record.buy_date,
            strategy_type=record.strategy_type,
            tier=record.tier,
            cost_price=record.cost_price,
            coverage_status="missing_5m",
            exit_reason="缺失5m数据",
            exit_time=None,
            exit_price=None,
            yield_pct=None,
            highest_price=None,
            highest_gain_pct=None,
            bars_replayed=0,
            t3_date=None,
            warning="找不到 buy_date 14:50 之后的 5m bars。",
        )

    t3_date = infer_t3_date(record, trading_dates, bars)
    if t3_date:
        t3_end = pd.Timestamp(f"{t3_date} 15:00:00")
        bars = bars.loc[bars["datetime"] <= t3_end].copy()

    highest_price = record.cost_price
    trailing_active = False
    disaster_stop_reason, disaster_stop_ratio = intraday_disaster_stop_config(record)
    disaster_stop_price = record.cost_price * disaster_stop_ratio
    eod_stop_reason, eod_stop_ratio = eod_structural_stop_config(record)
    eod_stop_price = record.cost_price * eod_stop_ratio

    t3_last_index = t3_close_bar_index(bars, t3_date)

    for idx, bar in bars.iterrows():
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])
        bar_close = float(bar["close"])
        bar_ts = pd.Timestamp(bar["datetime"])
        bar_time = bar_ts.strftime("%Y-%m-%d %H:%M:%S")

        highest_price = max(highest_price, bar_high)

        if bar_low <= disaster_stop_price:
            return build_result(
                record=record,
                coverage_status="covered",
                exit_reason=disaster_stop_reason,
                exit_time=bar_time,
                exit_price=disaster_stop_price,
                highest_price=highest_price,
                bars_replayed=idx + 1,
                t3_date=t3_date,
            )

        if highest_price >= record.cost_price * (1.0 + TRAILING_ARM_PCT):
            trailing_active = True

        trailing_trigger_price = highest_price * (1.0 - TRAILING_PULLBACK_PCT)
        if trailing_active and bar_low <= trailing_trigger_price:
            exit_price = strict_trailing_exit_price(highest_price, bar_close)
            return build_result(
                record=record,
                coverage_status="covered",
                exit_reason="动态追踪止盈_4pct引信_2pct回撤",
                exit_time=bar_time,
                exit_price=exit_price,
                highest_price=highest_price,
                bars_replayed=idx + 1,
                t3_date=t3_date,
            )

        if bar_ts.time() in EOD_STRUCTURAL_STOP_TIMES and bar_close <= eod_stop_price:
            return build_result(
                record=record,
                coverage_status="covered",
                exit_reason=eod_stop_reason,
                exit_time=bar_time,
                exit_price=bar_close,
                highest_price=highest_price,
                bars_replayed=idx + 1,
                t3_date=t3_date,
            )

        if t3_last_index is not None and int(idx) == t3_last_index:
            return build_result(
                record=record,
                coverage_status="covered",
                exit_reason="T+3强制平仓",
                exit_time=bar_time,
                exit_price=bar_close,
                highest_price=highest_price,
                bars_replayed=idx + 1,
                t3_date=t3_date,
            )

    fallback = historical_t3_fallback_result(record, highest_price, len(bars), t3_date, "未到T+3或数据未覆盖到T+3")
    if fallback:
        return fallback
    return SimulationResult(
        pick_id=record.pick_id,
        code=record.code,
        name=record.name,
        buy_date=record.buy_date,
        strategy_type=record.strategy_type,
        tier=record.tier,
        cost_price=record.cost_price,
        coverage_status="open_or_incomplete",
        exit_reason="未到T+3或数据未覆盖到T+3",
        exit_time=None,
        exit_price=None,
        yield_pct=None,
        highest_price=round(highest_price, 4),
        highest_gain_pct=round((highest_price / record.cost_price - 1.0) * 100.0, 4),
        bars_replayed=len(bars),
        t3_date=t3_date,
        warning="有 5m 数据但未触发止损/追踪止盈，且未覆盖到 T+3 14:55-15:00 bar。",
    )


def build_result(
    record: BuyRecord,
    coverage_status: str,
    exit_reason: str,
    exit_time: str,
    exit_price: float,
    highest_price: float,
    bars_replayed: int,
    t3_date: Optional[str],
) -> SimulationResult:
    yield_pct = (exit_price / record.cost_price - 1.0) * 100.0
    highest_gain_pct = (highest_price / record.cost_price - 1.0) * 100.0
    return SimulationResult(
        pick_id=record.pick_id,
        code=record.code,
        name=record.name,
        buy_date=record.buy_date,
        strategy_type=record.strategy_type,
        tier=record.tier,
        cost_price=round(record.cost_price, 4),
        coverage_status=coverage_status,
        exit_reason=exit_reason,
        exit_time=exit_time,
        exit_price=round(exit_price, 4),
        yield_pct=round(yield_pct, 4),
        highest_price=round(highest_price, 4),
        highest_gain_pct=round(highest_gain_pct, 4),
        bars_replayed=int(bars_replayed),
        t3_date=t3_date,
    )


def historical_t3_fallback_result(
    record: BuyRecord,
    highest_price: float,
    bars_replayed: int,
    t3_date: Optional[str],
    trigger_reason: str,
) -> Optional[SimulationResult]:
    close_price = record.close_price
    close_return = record.close_return_pct
    close_date = record.close_date or record.target_date or t3_date or ""
    winner = record.raw.get("winner") if isinstance(record.raw.get("winner"), dict) else {}

    if close_price is None or close_price <= 0:
        close_price = _first_number(
            winner.get("t3_settlement_price"),
            winner.get("t3_close"),
            winner.get("close_price"),
        )
    if close_return is None:
        close_return = _first_number(
            winner.get("t3_settlement_return_pct"),
            winner.get("t3_close_return_pct"),
            winner.get("close_return_pct"),
        )
    close_date = (
        close_date
        or str(winner.get("t3_exit_date") or winner.get("close_date") or winner.get("target_date") or "")[:10]
    )

    if close_price is None or close_price <= 0:
        return None
    if close_return is None:
        close_return = (close_price / record.cost_price - 1.0) * 100.0

    effective_highest = max(float(highest_price or 0), record.cost_price, close_price)
    highest_gain_pct = (effective_highest / record.cost_price - 1.0) * 100.0
    exit_time = f"{close_date} 15:00:00" if close_date else None
    return SimulationResult(
        pick_id=record.pick_id,
        code=record.code,
        name=record.name,
        buy_date=record.buy_date,
        strategy_type=record.strategy_type,
        tier=record.tier,
        cost_price=round(record.cost_price, 4),
        coverage_status="daily_t3_fallback",
        exit_reason="T+3日线兜底平仓",
        exit_time=exit_time,
        exit_price=round(float(close_price), 4),
        yield_pct=round(float(close_return), 4),
        highest_price=round(effective_highest, 4),
        highest_gain_pct=round(highest_gain_pct, 4),
        bars_replayed=int(bars_replayed),
        t3_date=t3_date or close_date or record.target_date or None,
        warning=f"{trigger_reason}；使用 daily_picks 历史 T+3 闭环结算价兜底。",
    )


def results_to_dataframe(results: list[SimulationResult]) -> pd.DataFrame:
    return pd.DataFrame([item.__dict__ for item in results])


def classify_exit_category(reason: Any) -> str:
    text = str(reason or "")
    if text == "T+3日线兜底平仓":
        return "T+3日线兜底平仓"
    if text.startswith("盘中暴雷止损"):
        return "盘中暴雷止损"
    if text.startswith("尾盘破位卖出"):
        return "尾盘破位卖出"
    if text.startswith("硬止损"):
        return "硬止损"
    if "追踪止盈" in text:
        return "追踪止盈"
    if "T+3" in text:
        return "T+3强制平仓"
    if "缺失" in text or "未到" in text or "无法" in text:
        return "缺失或未结算"
    return text or "未知"


def build_report_payload(
    records: list[BuyRecord],
    results: list[SimulationResult],
    start_date: str,
    end_date: str,
    db_path: Path,
    minute_root: Path,
) -> dict[str, Any]:
    df = results_to_dataframe(results)
    if df.empty:
        summary = {
            "start_date": start_date,
            "end_date": end_date,
            "total_count": len(records),
            "any_5m_count": 0,
            "covered_count": 0,
            "daily_t3_fallback_count": 0,
            "evaluated_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0.0,
            "mean_yield": 0.0,
            "median_yield": 0.0,
            "incomplete_count": 0,
            "reason_counts": [],
            "category_counts": [],
            "strategy_performance": [],
        }
        return _payload_envelope(summary, [], start_date, end_date, db_path, minute_root)

    df["exit_category"] = df["exit_reason"].map(classify_exit_category)
    evaluated = df[pd.to_numeric(df["yield_pct"], errors="coerce").notna()].copy()
    any_5m_count = int(pd.to_numeric(df["bars_replayed"], errors="coerce").fillna(0).gt(0).sum())
    covered_count = int((df["coverage_status"] == "covered").sum())
    daily_t3_fallback_count = int((df["coverage_status"] == "daily_t3_fallback").sum())
    win_count = int((evaluated["yield_pct"] > 0).sum()) if not evaluated.empty else 0
    loss_count = int(len(evaluated) - win_count)

    category_counts = (
        df["exit_category"]
        .value_counts(dropna=False)
        .rename_axis("exit_category")
        .reset_index(name="count")
        .to_dict(orient="records")
    )
    reason_counts = (
        df["exit_reason"]
        .value_counts(dropna=False)
        .rename_axis("exit_reason")
        .reset_index(name="count")
        .to_dict(orient="records")
    )
    strategy_performance = []
    if not evaluated.empty:
        strategy_report = (
            evaluated.groupby("strategy_type")
            .agg(
                trades=("yield_pct", "count"),
                win_rate_pct=("yield_pct", lambda values: round(float((values > 0).mean() * 100.0), 4)),
                mean_yield_pct=("yield_pct", lambda values: round(float(values.mean()), 4)),
                median_yield_pct=("yield_pct", lambda values: round(float(values.median()), 4)),
                best_yield_pct=("yield_pct", lambda values: round(float(values.max()), 4)),
                worst_yield_pct=("yield_pct", lambda values: round(float(values.min()), 4)),
            )
            .reset_index()
        )
        strategy_performance = strategy_report.to_dict(orient="records")

    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "total_count": len(records),
        "any_5m_count": any_5m_count,
        "covered_count": covered_count,
        "daily_t3_fallback_count": daily_t3_fallback_count,
        "evaluated_count": int(len(evaluated)),
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": round(float(win_count / len(evaluated) * 100.0), 4) if not evaluated.empty else 0.0,
        "mean_yield": round(float(evaluated["yield_pct"].mean()), 4) if not evaluated.empty else 0.0,
        "median_yield": round(float(evaluated["yield_pct"].median()), 4) if not evaluated.empty else 0.0,
        "incomplete_count": int(df["yield_pct"].isna().sum()),
        "reason_counts": reason_counts,
        "category_counts": category_counts,
        "strategy_performance": strategy_performance,
        "rule": "V5.6 5m 前复权回放：盘中防爆 -6%/-4%，+4% 激活后回撤 -2% 追踪止盈，14:50/14:55 尾盘结构止损 -3%/-1.5%，T+3 最后一根 5m 强制结算；若本地 5m 后续缺口但 daily_picks 已有历史闭环，则用日线 T+3 结算价兜底纳入胜率。",
    }
    record_by_id = {record.pick_id: record for record in records}
    rows = [
        result_to_ledger_row(record_by_id[result.pick_id], result)
        for result in results
        if result.pick_id in record_by_id
    ]
    rows.sort(key=lambda row: (str(row.get("selection_date") or ""), int(row.get("pick_id") or 0)), reverse=True)
    return _payload_envelope(summary, rows, start_date, end_date, db_path, minute_root)


def _payload_envelope(
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
    db_path: Path,
    minute_root: Path,
) -> dict[str, Any]:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "sentinel_5m_backtest",
        "start_date": start_date,
        "end_date": end_date,
        "db_path": str(db_path),
        "minute_root": str(minute_root),
        "summary": summary,
        "rows": rows,
        "paused_strategy_types": list(PAUSED_STRATEGY_TYPES),
        "pick_mode": "daily_strategy_top1",
    }


def result_to_ledger_row(record: BuyRecord, result: SimulationResult) -> dict[str, Any]:
    raw = record.raw if isinstance(record.raw, dict) else {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    winner = dict(winner)
    close_date = str(result.exit_time or "")[:10] if result.exit_time else ""
    is_closed = result.yield_pct is not None
    close_reason = human_exit_reason(result.exit_reason)
    sell_strategy = sell_strategy_label(result)
    expected_t3 = _first_number(
        winner.get("expected_t3_max_gain_pct"),
        winner.get("expected_premium"),
        winner.get("composite_score"),
    )
    position = record.suggested_position
    if position is None:
        position = safe_optional_float(winner.get("suggested_position"))
    core_theme = _first_text(
        winner.get("core_theme"),
        winner.get("theme_name"),
        raw.get("core_theme"),
        "",
    )
    theme_momentum = _first_number(
        winner.get("theme_momentum_3d"),
        winner.get("theme_momentum"),
        winner.get("theme_pct_chg_3"),
        raw.get("theme_momentum_3d"),
    )
    raw_sentinel = {
        "coverage_status": result.coverage_status,
        "exit_reason": result.exit_reason,
        "exit_time": result.exit_time,
        "exit_price": result.exit_price,
        "yield_pct": result.yield_pct,
        "highest_price": result.highest_price,
        "highest_gain_pct": result.highest_gain_pct,
        "bars_replayed": result.bars_replayed,
        "t3_date": result.t3_date,
        "warning": result.warning,
        "sell_strategy": sell_strategy,
    }
    merged_winner = {
        **winner,
        "code": record.code,
        "name": record.name,
        "strategy_type": record.strategy_type,
        "price": record.cost_price,
        "selection_tier": record.tier,
        "suggested_position": position,
        "expected_t3_max_gain_pct": expected_t3,
        "t3_max_gain_pct": result.highest_gain_pct,
        "t3_settlement_price": result.exit_price,
        "t3_settlement_return_pct": result.yield_pct,
        "core_theme": core_theme,
        "theme_name": core_theme,
        "theme_momentum_3d": theme_momentum,
        "theme_momentum": theme_momentum,
        "theme_pct_chg_3": theme_momentum,
        "sentinel_5m": raw_sentinel,
        "sell_strategy": sell_strategy,
        "exit_policy": sell_strategy,
    }
    return {
        "id": f"sentinel5m-{record.pick_id}",
        "pick_id": record.pick_id,
        "selection_date": record.buy_date,
        "date": record.buy_date,
        "target_date": result.t3_date or record.target_date or "",
        "selected_at": record.selected_at or f"{record.buy_date}T14:50:00",
        "snapshot_time": "14:50:00",
        "snapshot_price": round(record.cost_price, 4),
        "selection_price": round(record.cost_price, 4),
        "price": round(record.cost_price, 4),
        "code": record.code,
        "name": record.name,
        "strategy_type": record.strategy_type,
        "selection_tier": record.tier,
        "tier": record.tier,
        "win_rate": record.win_rate,
        "selection_change": record.selection_change,
        "snapshot_vol_ratio": record.snapshot_vol_ratio,
        "suggested_position": position,
        "expected_t3_max_gain_pct": expected_t3,
        "expected_premium": expected_t3,
        "composite_score": _first_number(winner.get("composite_score"), winner.get("selection_score"), record.win_rate),
        "sort_score": _first_number(winner.get("sort_score"), winner.get("selection_score"), winner.get("composite_score")),
        "core_theme": core_theme,
        "theme_name": core_theme,
        "theme_momentum": theme_momentum,
        "theme_momentum_3d": theme_momentum,
        "theme_pct_chg_3": theme_momentum,
        "close_date": close_date,
        "close_time": result.exit_time,
        "close_reason": close_reason,
        "sell_strategy": sell_strategy,
        "exit_policy": sell_strategy,
        "close_price": result.exit_price,
        "close_return_pct": result.yield_pct,
        "t3_settlement_price": result.exit_price,
        "t3_settlement_return_pct": result.yield_pct,
        "t3_max_gain_pct": result.highest_gain_pct,
        "highest_price": result.highest_price,
        "highest_gain_pct": result.highest_gain_pct,
        "exit_category": classify_exit_category(result.exit_reason),
        "bars_replayed": result.bars_replayed,
        "coverage_status": result.coverage_status,
        "warning": result.warning,
        "is_closed": is_closed,
        "success": result.yield_pct > 0 if is_closed else None,
        "status": "sentinel_5m_closed" if is_closed else "sentinel_5m_incomplete",
        "raw": {
            **raw,
            "source": "sentinel_5m_backtest",
            "daily_pick_source": raw.get("source") or "",
            "winner": merged_winner,
        },
    }


def human_exit_reason(reason: Any) -> str:
    text = str(reason or "")
    labels = {
        "动态追踪止盈_4pct引信_2pct回撤": "5m动态追踪止盈",
        "T+3强制平仓": "5m T+3强制平仓",
        "盘中暴雷止损_敢死队_4pct": "5m盘中暴雷止损：敢死队-4%",
        "盘中暴雷止损_正规军_6pct": "5m盘中暴雷止损：正规军-6%",
        "尾盘破位卖出_正规军_3pct": "5m尾盘破位卖出：正规军-3%",
        "尾盘破位卖出_敢死队_1_5pct": "5m尾盘破位卖出：敢死队-1.5%",
        "T+3日线兜底平仓": "历史日线闭环兜底",
        "未到T+3或数据未覆盖到T+3": "5m未覆盖到T+3",
        "缺失5m数据": "缺失5m数据",
    }
    return labels.get(text, text or "未知")


def sell_strategy_label(result: SimulationResult) -> str:
    reason = str(result.exit_reason or "")
    if reason == "动态追踪止盈_4pct引信_2pct回撤":
        return "V5.6非对称5m风控：+4%激活/-2%回撤追踪止盈"
    if reason.startswith("盘中暴雷止损"):
        return "V5.6非对称5m风控：盘中防爆止损"
    if reason.startswith("尾盘破位卖出"):
        return "V5.6非对称5m风控：尾盘结构止损"
    if reason == "T+3强制平仓":
        return "V5.6非对称5m风控：T+3最后5m强制平仓"
    if reason == "T+3日线兜底平仓":
        return "历史日线闭环兜底结算（本地5m后续缺失）"
    if result.coverage_status == "daily_t3_fallback":
        return "历史日线闭环兜底结算（本地5m后续缺失）"
    if result.coverage_status == "missing_5m":
        return "5m数据缺失：等待真实账本闭环"
    if result.coverage_status == "open_or_incomplete":
        return "5m数据未覆盖到结算点：等待真实账本闭环"
    return "V5.6非对称5m风控"


def _first_number(*values: Any) -> Optional[float]:
    for value in values:
        parsed = safe_optional_float(value)
        if parsed is not None:
            return parsed
    return None


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text != "-":
            return text
    return ""


def default_cache_path(start_date: str, end_date: str) -> Path:
    start_key = start_date.replace("-", "")
    end_key = end_date.replace("-", "")
    return CACHE_DIR / f"sentinel_5m_backtest_{start_key}_{end_key}.json"


def write_report_payload(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path = path.parent / "sentinel_5m_backtest_latest.json"
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def print_report(records: list[BuyRecord], results: list[SimulationResult], start_date: str, end_date: str) -> None:
    df = results_to_dataframe(results)
    if df.empty:
        print("没有可展示的模拟结果。")
        return

    df["exit_category"] = df["exit_reason"].map(classify_exit_category)
    evaluated = df[pd.to_numeric(df["yield_pct"], errors="coerce").notna()].copy()
    total_count = len(records)
    covered_count = int((df["coverage_status"] == "covered").sum())
    any_5m_count = int(pd.to_numeric(df["bars_replayed"], errors="coerce").fillna(0).gt(0).sum())
    daily_t3_fallback_count = int((df["coverage_status"] == "daily_t3_fallback").sum())
    win_rate = float((evaluated["yield_pct"] > 0).mean() * 100.0) if not evaluated.empty else 0.0
    mean_yield = float(evaluated["yield_pct"].mean()) if not evaluated.empty else 0.0
    median_yield = float(evaluated["yield_pct"].median()) if not evaluated.empty else 0.0

    print("\n========== V5.6 盘中巡逻兵 5m 离线回测报告 ==========")
    print(f"回测区间：{start_date} -> {end_date}")
    print(f"出票总数：{total_count}")
    print(f"有任意 5m 数据数：{any_5m_count}")
    print(f"完整可结算覆盖数：{covered_count}")
    print(f"日线T+3兜底结算数：{daily_t3_fallback_count}")
    print(f"纳入收益统计数：{len(evaluated)}")
    print(f"新卖出引擎胜率：{win_rate:.2f}%")
    print(f"新单笔平均期望 Mean Yield：{mean_yield:.4f}%")
    print(f"新单笔中位收益 Median Yield：{median_yield:.4f}%")

    print("\n---------- 卖出原因分布（验收口径） ----------")
    category_counts = df["exit_category"].value_counts(dropna=False).rename_axis("exit_category").reset_index(name="count")
    print(category_counts.to_string(index=False))

    print("\n---------- 卖出原因分布（细分口径） ----------")
    reason_counts = df["exit_reason"].value_counts(dropna=False).rename_axis("exit_reason").reset_index(name="count")
    print(reason_counts.to_string(index=False))

    print("\n---------- 策略维度收益 ----------")
    if evaluated.empty:
        print("无可结算样本。")
    else:
        strategy_report = (
            evaluated.groupby("strategy_type")
            .agg(
                trades=("yield_pct", "count"),
                win_rate_pct=("yield_pct", lambda values: round(float((values > 0).mean() * 100.0), 4)),
                mean_yield_pct=("yield_pct", lambda values: round(float(values.mean()), 4)),
                median_yield_pct=("yield_pct", lambda values: round(float(values.median()), 4)),
                best_yield_pct=("yield_pct", lambda values: round(float(values.max()), 4)),
                worst_yield_pct=("yield_pct", lambda values: round(float(values.min()), 4)),
            )
            .reset_index()
        )
        print(strategy_report.to_string(index=False))

    print("\n---------- 单票明细 ----------")
    detail_cols = [
        "pick_id",
        "buy_date",
        "t3_date",
        "code",
        "name",
        "strategy_type",
        "tier",
        "cost_price",
        "exit_reason",
        "exit_time",
        "exit_price",
        "yield_pct",
        "highest_gain_pct",
        "bars_replayed",
        "coverage_status",
        "warning",
    ]
    print(df[detail_cols].to_string(index=False))
    print("====================================================\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="V5.6 盘中巡逻兵 5m K-Line Simulator")
    parser.add_argument("--start-date", default=previous_month_27(), help="默认上个月 27 日")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="默认今天")
    parser.add_argument("--db-path", default=str(SQLITE_PATH))
    parser.add_argument("--minute-root", default=str(PRE_ADJUSTED_5M_DIR))
    parser.add_argument("--output-json", default="", help="输出标准账本缓存 JSON；默认写入 data/strategy_cache/sentinel_5m_backtest_*.json")
    parser.add_argument("--no-cache", action="store_true", help="只打印报告，不写 JSON 缓存")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    minute_root = Path(args.minute_root)
    records = collapse_records_to_strategy_top1(load_buy_records(db_path, args.start_date, args.end_date))
    trading_dates = load_trading_dates(db_path)

    if not records:
        print(f"区间 {args.start_date} -> {args.end_date} 没有读取到买入记录。")
        return

    prime_pre_adjusted_zip_cache(records, minute_root, args.end_date)
    results = [simulate_one(record, minute_root, trading_dates, args.end_date) for record in records]
    payload = build_report_payload(records, results, args.start_date, args.end_date, db_path, minute_root)
    if not args.no_cache:
        output_path = Path(args.output_json).expanduser() if args.output_json else default_cache_path(args.start_date, args.end_date)
        write_report_payload(payload, output_path)
        print(f"[Cache] sentinel_5m_backtest 写入：{output_path}", flush=True)
    print_report(records, results, args.start_date, args.end_date)


if __name__ == "__main__":
    main()
