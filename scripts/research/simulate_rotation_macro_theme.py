from __future__ import annotations

import argparse
import json
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
DEFAULT_PANEL_PATH = BASE_DIR / "data" / "strategy_cache" / "experiments" / "global_5m_full_panel_20250102_20260506_all_allcodes.parquet"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_macro_theme_latest.json"
TRADE_DETAIL_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_macro_theme_trades.csv"
EQUITY_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_macro_theme_equity.csv"

CONCEPT_STOCK_MAP_PATH = BASE_DIR / "data" / "concept_stock_map.json"
CONCEPT_CATALOG_PATH = BASE_DIR / "data" / "concept_catalog.json"
CONCEPT_INDEX_PATH = BASE_DIR / "data" / "concept_kline" / "concept_index.parquet"
SECTOR_STOCK_MAP_PATH = BASE_DIR / "data" / "sector_kline" / "stock_sector_map.parquet"
SECTOR_INDEX_PATH = BASE_DIR / "data" / "sector_kline" / "sector_index.parquet"

DEFAULT_START_DATE = "2025-01-02"
DEFAULT_END_DATE = "2025-12-31"
INITIAL_CAPITAL = 200_000.0
MAX_POSITION_CASH = 100_000.0
BUY_COST_RATE = 0.0005
SELL_COST_RATE = 0.0015
SWITCH_BUFFER = 0.05
MAX_HOLDINGS = 2
TARGET_POOL_SIZE = 5
HOT_THEME_QUANTILE = 0.90
LOT_SIZE = 100

CODE_RE = re.compile(r"(\d{6})")


@dataclass
class Position:
    code: str
    name: str
    core_theme: str
    shares: int
    cost_price: float
    last_price: float
    theme_score: float
    amount_score: float
    score_date: str


@dataclass(frozen=True)
class PendingOrder:
    action: str
    code: str
    name: str
    core_theme: str
    theme_score: float
    amount_score: float
    decision_date: str
    reason: str
    switch_id: str = ""
    threshold_score: Optional[float] = None


@dataclass(frozen=True)
class Trade:
    trade_date: str
    action: str
    code: str
    name: str
    core_theme: str
    price: float
    shares: int
    gross_amount: float
    friction_cost: float
    cash_after: float
    position_count_after: int
    reason: str
    decision_date: str
    theme_score: Optional[float] = None
    amount_score: Optional[float] = None
    threshold_score: Optional[float] = None


@dataclass
class Account:
    cash: float
    positions: dict[str, Position]
    total_friction_cost: float = 0.0
    buy_count: int = 0
    sell_count: int = 0
    switch_count: int = 0
    theme_cooldown_count: int = 0
    ma20_stop_count: int = 0
    skipped_order_count: int = 0


def normalize_code(value: Any) -> str:
    text = str(value or "")
    match = CODE_RE.search(text)
    return match.group(1) if match else text.zfill(6)[-6:]


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


def finite_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def read_source_frame(panel_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    if not panel_path.exists():
        raise FileNotFoundError(f"历史复盘面板不存在：{panel_path}")

    if panel_path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq

            header = pq.ParquetFile(panel_path).schema_arrow.names
        except Exception:
            header = pd.read_parquet(panel_path).head(0).columns.tolist()
    else:
        header = pd.read_csv(panel_path, nrows=0).columns.tolist()

    required = {"code", "date", "open", "close", "amount"}
    missing = sorted(required - set(header))
    if missing:
        raise ValueError(f"历史复盘面板缺少必要字段：{missing}")

    optional = [
        "name",
        "volume",
        "entry_price",
        "buy5m_entry_price",
        "ma20_bias",
        "ma20",
        "MA20",
        "core_theme",
        "theme_name",
        "theme_momentum_3d",
        "theme_pct_chg_3",
    ]
    usecols = [col for col in [*required, *optional] if col in header]
    if panel_path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(panel_path, columns=list(dict.fromkeys(usecols)))
    else:
        frame = pd.read_csv(panel_path, usecols=list(dict.fromkeys(usecols)), dtype={"code": "string"}, low_memory=False)

    frame["code"] = frame["code"].map(normalize_code)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["code", "date"]).copy()
    frame = frame[(frame["date"] >= start_date) & (frame["date"] <= end_date)].copy()
    if frame.empty:
        raise SystemExit(f"区间 {start_date} -> {end_date} 在历史复盘面板中没有记录。")

    if "name" not in frame.columns:
        frame["name"] = frame["code"]
    else:
        frame["name"] = frame["name"].fillna("").astype(str).str.strip()
        bad_name = frame["name"].isin(["", "nan", "NaN", "None", "null"])
        frame.loc[bad_name, "name"] = frame.loc[bad_name, "code"]

    numeric_cols = [
        "open",
        "close",
        "amount",
        "volume",
        "entry_price",
        "buy5m_entry_price",
        "ma20_bias",
        "ma20",
        "MA20",
        "theme_momentum_3d",
        "theme_pct_chg_3",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    return frame.replace([np.inf, -np.inf], np.nan)


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_concept_maps() -> tuple[dict[str, str], dict[str, str], dict[str, Path]]:
    raw_map = load_json(CONCEPT_STOCK_MAP_PATH)
    stock_to_concept: dict[str, str] = {}
    if isinstance(raw_map, dict):
        for code, concepts in raw_map.items():
            clean = normalize_code(code)
            if not clean:
                continue
            if isinstance(concepts, list) and concepts:
                stock_to_concept[clean] = str(concepts[0])
            elif isinstance(concepts, str) and concepts:
                stock_to_concept[clean] = concepts

    concept_code_to_name: dict[str, str] = {}
    catalog = load_json(CONCEPT_CATALOG_PATH)
    if isinstance(catalog, list):
        for item in catalog:
            code = str(item.get("concept_code") or "").strip()
            name = str(item.get("concept_name") or "").strip()
            if code and name:
                concept_code_to_name[code] = name

    concept_paths: dict[str, Path] = {}
    if CONCEPT_INDEX_PATH.exists():
        try:
            index = pd.read_parquet(CONCEPT_INDEX_PATH)
            for row in index.to_dict("records"):
                code = str(row.get("concept_code") or "").strip()
                path = Path(str(row.get("path") or ""))
                if code and path.exists():
                    concept_paths[code] = path
                name = str(row.get("concept_name") or "").strip()
                if code and name:
                    concept_code_to_name.setdefault(code, name)
        except Exception:
            pass
    return stock_to_concept, concept_code_to_name, concept_paths


def load_sector_maps() -> tuple[dict[str, str], dict[str, Path]]:
    stock_to_sector: dict[str, str] = {}
    if SECTOR_STOCK_MAP_PATH.exists():
        try:
            mapping = pd.read_parquet(SECTOR_STOCK_MAP_PATH, columns=["code", "sector_name"])
            mapping["code"] = mapping["code"].map(normalize_code)
            mapping["sector_name"] = mapping["sector_name"].fillna("").astype(str)
            stock_to_sector = dict(zip(mapping["code"], mapping["sector_name"]))
        except Exception:
            stock_to_sector = {}

    sector_paths: dict[str, Path] = {}
    if SECTOR_INDEX_PATH.exists():
        try:
            index = pd.read_parquet(SECTOR_INDEX_PATH)
            for row in index.to_dict("records"):
                name = str(row.get("sector_name") or "").strip()
                path = Path(str(row.get("path") or ""))
                if name and path.exists():
                    sector_paths[name] = path
        except Exception:
            pass
    return stock_to_sector, sector_paths


def attach_theme_identity(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = frame.copy()
    if "core_theme" in out.columns:
        out["core_theme"] = out["core_theme"].fillna("").astype(str)
    elif "theme_name" in out.columns:
        out["core_theme"] = out["theme_name"].fillna("").astype(str)
    else:
        out["core_theme"] = ""

    stock_to_concept, concept_names, concept_paths = load_concept_maps()
    stock_to_sector, sector_paths = load_sector_maps()

    missing_theme = out["core_theme"].str.strip().eq("")
    out["theme_key"] = ""
    if missing_theme.any():
        codes = out.loc[missing_theme, "code"].astype(str)
        concept_codes = codes.map(stock_to_concept).fillna("")
        concept_theme = concept_codes.map(concept_names).fillna("")
        sector_theme = codes.map(stock_to_sector).fillna("")
        use_concept = concept_theme.astype(str).str.strip().ne("")
        out.loc[missing_theme, "core_theme"] = np.where(use_concept, concept_theme, sector_theme)
        out.loc[missing_theme, "theme_key"] = np.where(
            use_concept,
            "concept:" + concept_codes.astype(str),
            "sector:" + sector_theme.astype(str),
        )

    known_theme = out["theme_key"].astype(str).str.strip().eq("")
    if known_theme.any():
        out.loc[known_theme, "theme_key"] = "theme:" + out.loc[known_theme, "core_theme"].astype(str)

    out["core_theme"] = out["core_theme"].replace("", np.nan)
    out = out.dropna(subset=["core_theme"]).copy()
    meta = {
        "concept_stock_map_rows": int(len(stock_to_concept)),
        "concept_kline_count": int(len(concept_paths)),
        "sector_stock_map_rows": int(len(stock_to_sector)),
        "sector_kline_count": int(len(sector_paths)),
        "theme_identity_source": "input_or_local_theme_pipeline",
    }
    return out, meta


def normalize_pct_series(value: pd.Series) -> pd.Series:
    series = pd.to_numeric(value, errors="coerce")
    finite = series.replace([np.inf, -np.inf], np.nan).dropna()
    if not finite.empty and finite.abs().quantile(0.95) > 1.5:
        series = series / 100.0
    return series


def theme_momentum_frame(theme_keys: set[str]) -> pd.DataFrame:
    _, _, concept_paths = load_concept_maps()
    _, sector_paths = load_sector_maps()
    rows: list[pd.DataFrame] = []
    for key in sorted(theme_keys):
        if key.startswith("concept:"):
            ident = key.split(":", 1)[1]
            path = concept_paths.get(ident)
        elif key.startswith("sector:"):
            ident = key.split(":", 1)[1]
            path = sector_paths.get(ident)
        else:
            path = None
        if path is None or not path.exists():
            continue
        try:
            theme = pd.read_parquet(path)
        except Exception:
            continue
        if theme.empty or "datetime" not in theme.columns or "close" not in theme.columns:
            continue
        theme = theme.copy()
        theme["date"] = pd.to_datetime(theme["datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
        theme = theme.dropna(subset=["date"]).drop_duplicates("date", keep="last").sort_values("date")
        close = pd.to_numeric(theme["close"], errors="coerce")
        if "pct_chg" in theme.columns:
            ret = normalize_pct_series(theme["pct_chg"])
        else:
            ret = close.pct_change(fill_method=None)
        item = pd.DataFrame(
            {
                "theme_key": key,
                "date": theme["date"],
                "theme_momentum_3d": (1.0 + ret).rolling(3, min_periods=1).apply(np.prod, raw=True) - 1.0,
            }
        )
        rows.append(item)
    if not rows:
        return pd.DataFrame(columns=["theme_key", "date", "theme_momentum_3d"])
    out = pd.concat(rows, ignore_index=True, sort=False)
    out["theme_momentum_3d"] = pd.to_numeric(out["theme_momentum_3d"], errors="coerce")
    return out.dropna(subset=["date", "theme_key", "theme_momentum_3d"])


def attach_theme_momentum(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = frame.copy()
    if "theme_momentum_3d" not in out.columns:
        out["theme_momentum_3d"] = np.nan
    if "theme_pct_chg_3" in out.columns:
        out["theme_momentum_3d"] = out["theme_momentum_3d"].combine_first(pd.to_numeric(out["theme_pct_chg_3"], errors="coerce"))

    need = out["theme_momentum_3d"].isna()
    momentum = theme_momentum_frame(set(out.loc[need, "theme_key"].astype(str).unique().tolist()))
    meta = {"theme_momentum_source": "input_or_local_theme_kline", "theme_momentum_rows": int(len(momentum))}
    if not momentum.empty and need.any():
        out = out.merge(momentum, on=["theme_key", "date"], how="left", suffixes=("", "_derived"))
        out["theme_momentum_3d"] = out["theme_momentum_3d"].combine_first(out["theme_momentum_3d_derived"])
        out = out.drop(columns=["theme_momentum_3d_derived"], errors="ignore")

    out["theme_momentum_3d"] = pd.to_numeric(out["theme_momentum_3d"], errors="coerce")
    out = out.dropna(subset=["theme_momentum_3d"]).copy()
    return out, meta


def prepare_panel(panel_path: Path, start_date: str, end_date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = read_source_frame(panel_path, start_date, end_date)
    frame, identity_meta = attach_theme_identity(frame)
    frame, momentum_meta = attach_theme_momentum(frame)

    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce").fillna(0.0)
    frame["volume"] = pd.to_numeric(frame.get("volume", 0.0), errors="coerce").fillna(0.0)
    frame["effective_amount"] = frame["amount"]
    fallback_mask = frame["effective_amount"].le(0.0) & frame["volume"].gt(0.0) & pd.to_numeric(frame["close"], errors="coerce").gt(0.0)
    frame.loc[fallback_mask, "effective_amount"] = frame.loc[fallback_mask, "volume"] * pd.to_numeric(frame.loc[fallback_mask, "close"], errors="coerce")

    frame["ma_check_price"] = pd.Series(np.nan, index=frame.index, dtype="float64")
    for col in ("buy5m_entry_price", "entry_price", "close"):
        if col in frame.columns:
            frame["ma_check_price"] = frame["ma_check_price"].combine_first(pd.to_numeric(frame[col], errors="coerce"))

    if "ma20" in frame.columns:
        frame["ma20_line"] = pd.to_numeric(frame["ma20"], errors="coerce")
    elif "MA20" in frame.columns:
        frame["ma20_line"] = pd.to_numeric(frame["MA20"], errors="coerce")
    elif "ma20_bias" in frame.columns:
        denom = 1.0 + pd.to_numeric(frame["ma20_bias"], errors="coerce") / 100.0
        frame["ma20_line"] = np.where(denom > 0, pd.to_numeric(frame["close"], errors="coerce") / denom, np.nan)
    else:
        frame["ma20_line"] = np.nan

    frame = frame.dropna(subset=["open", "close", "core_theme", "theme_momentum_3d"]).copy()
    frame = frame.drop_duplicates(["date", "code"], keep="last")
    frame = frame.sort_values(["date", "theme_momentum_3d", "effective_amount"], ascending=[True, False, False])
    meta = {
        **identity_meta,
        **momentum_meta,
        "amount_zero_fallback_rows": int(fallback_mask.sum()),
        "input_rows_after_date_filter": int(len(frame)),
    }
    return frame.reset_index(drop=True), meta


def row_price(row: pd.Series, column: str) -> Optional[float]:
    value = finite_float(row.get(column))
    if value is None or value <= 0:
        return None
    return value


def with_trade_date(trade: Trade, trade_date: str) -> dict[str, Any]:
    row = asdict(trade)
    row["trade_date"] = trade_date
    return row


def buy_position(account: Account, order: PendingOrder, price: float, max_position_cash: float, buy_cost_rate: float) -> Optional[Trade]:
    if len(account.positions) >= MAX_HOLDINGS or order.code in account.positions:
        account.skipped_order_count += 1
        return None
    budget = min(float(max_position_cash), account.cash)
    shares = int((budget / (price * (1.0 + buy_cost_rate))) // LOT_SIZE) * LOT_SIZE
    if shares <= 0:
        account.skipped_order_count += 1
        return None
    gross = float(shares * price)
    friction = gross * buy_cost_rate
    total_cost = gross + friction
    if total_cost > account.cash + 1e-6:
        account.skipped_order_count += 1
        return None
    account.cash -= total_cost
    account.total_friction_cost += friction
    account.buy_count += 1
    account.positions[order.code] = Position(
        code=order.code,
        name=order.name,
        core_theme=order.core_theme,
        shares=shares,
        cost_price=price,
        last_price=price,
        theme_score=float(order.theme_score),
        amount_score=float(order.amount_score),
        score_date=order.decision_date,
    )
    return Trade(
        trade_date="",
        action="BUY",
        code=order.code,
        name=order.name,
        core_theme=order.core_theme,
        price=round(price, 4),
        shares=int(shares),
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        position_count_after=len(account.positions),
        reason=order.reason,
        decision_date=order.decision_date,
        theme_score=round(float(order.theme_score), 8),
        amount_score=round(float(order.amount_score), 2),
        threshold_score=round(float(order.threshold_score), 8) if order.threshold_score is not None else None,
    )


def sell_position(
    account: Account,
    position: Position,
    price: float,
    sell_cost_rate: float,
    reason: str,
    decision_date: str,
    threshold_score: Optional[float] = None,
) -> Trade:
    shares = int(position.shares)
    gross = float(shares * price)
    friction = gross * sell_cost_rate
    proceeds = gross - friction
    account.cash += proceeds
    account.total_friction_cost += friction
    account.sell_count += 1
    if reason == "BUFFERED_LEADER_SWITCH_SELL":
        account.switch_count += 1
    if reason == "THEME_COOLDOWN_SELL":
        account.theme_cooldown_count += 1
    if reason == "MA20_STOP":
        account.ma20_stop_count += 1
    del account.positions[position.code]
    return Trade(
        trade_date="",
        action="SELL",
        code=position.code,
        name=position.name,
        core_theme=position.core_theme,
        price=round(price, 4),
        shares=shares,
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        position_count_after=len(account.positions),
        reason=reason,
        decision_date=decision_date,
        theme_score=round(float(position.theme_score), 8) if math.isfinite(position.theme_score) else None,
        amount_score=round(float(position.amount_score), 2),
        threshold_score=round(float(threshold_score), 8) if threshold_score is not None else None,
    )


def execute_pending_orders(
    account: Account,
    pending_orders: list[PendingOrder],
    trade_date: str,
    by_code: pd.DataFrame,
    buy_cost_rate: float,
    sell_cost_rate: float,
    max_position_cash: float,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    executed_switch_sells: set[str] = set()
    sell_orders = [order for order in pending_orders if order.action == "SELL"]
    buy_orders = [order for order in pending_orders if order.action == "BUY"]

    for order in sell_orders:
        position = account.positions.get(order.code)
        if position is None or order.code not in by_code.index:
            account.skipped_order_count += 1
            continue
        price = row_price(by_code.loc[order.code], "open")
        if price is None:
            account.skipped_order_count += 1
            continue
        trade = sell_position(
            account=account,
            position=position,
            price=price,
            sell_cost_rate=sell_cost_rate,
            reason=order.reason,
            decision_date=order.decision_date,
            threshold_score=order.threshold_score,
        )
        trades.append(with_trade_date(trade, trade_date))
        if order.switch_id:
            executed_switch_sells.add(order.switch_id)

    for order in buy_orders:
        if order.switch_id and order.switch_id not in executed_switch_sells:
            account.skipped_order_count += 1
            continue
        if order.code not in by_code.index:
            account.skipped_order_count += 1
            continue
        price = row_price(by_code.loc[order.code], "open")
        if price is None:
            account.skipped_order_count += 1
            continue
        trade = buy_position(account, order, price, max_position_cash, buy_cost_rate)
        if trade is not None:
            trades.append(with_trade_date(trade, trade_date))
    return trades


def run_ma20_stops(account: Account, trade_date: str, by_code: pd.DataFrame, sell_cost_rate: float) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for code in list(account.positions):
        position = account.positions.get(code)
        if position is None or code not in by_code.index:
            continue
        row = by_code.loc[code]
        ma_check_price = row_price(row, "ma_check_price")
        ma20_line = row_price(row, "ma20_line")
        if ma_check_price is None or ma20_line is None:
            continue
        if ma_check_price < ma20_line:
            trade = sell_position(account, position, ma_check_price, sell_cost_rate, "MA20_STOP", trade_date)
            trades.append(with_trade_date(trade, trade_date))
    return trades


def mark_positions_to_close(account: Account, trade_date: str, by_code: pd.DataFrame) -> None:
    for code, position in list(account.positions.items()):
        if code not in by_code.index:
            continue
        row = by_code.loc[code]
        close = row_price(row, "close")
        if close is not None:
            position.last_price = close
        theme = str(row.get("core_theme") or position.core_theme)
        score = finite_float(row.get("theme_momentum_3d"))
        amount = finite_float(row.get("effective_amount"))
        position.core_theme = theme
        if score is not None:
            position.theme_score = score
        if amount is not None:
            position.amount_score = amount
        position.score_date = trade_date


def portfolio_value(account: Account) -> float:
    return float(account.cash + sum(pos.shares * pos.last_price for pos in account.positions.values()))


def hot_themes_and_targets(day: pd.DataFrame) -> tuple[set[str], pd.DataFrame, pd.DataFrame]:
    theme_rank = (
        day[["core_theme", "theme_momentum_3d"]]
        .dropna()
        .drop_duplicates("core_theme", keep="first")
        .sort_values(["theme_momentum_3d", "core_theme"], ascending=[False, True])
        .reset_index(drop=True)
    )
    if theme_rank.empty:
        return set(), pd.DataFrame(), theme_rank
    cutoff_count = max(1, int(math.ceil(len(theme_rank) * (1.0 - HOT_THEME_QUANTILE))))
    hot = theme_rank.head(cutoff_count)
    hot_set = set(hot["core_theme"].astype(str).tolist())
    targets = (
        day[day["core_theme"].astype(str).isin(hot_set)]
        .sort_values(["effective_amount", "theme_momentum_3d", "code"], ascending=[False, False, True])
        .head(TARGET_POOL_SIZE)
        .copy()
    )
    return hot_set, targets, theme_rank


def order_from_row(row: pd.Series, decision_date: str, reason: str, switch_id: str = "", threshold_score: Optional[float] = None) -> PendingOrder:
    return PendingOrder(
        action="BUY",
        code=str(row["code"]),
        name=str(row.get("name") or row["code"]),
        core_theme=str(row.get("core_theme") or ""),
        theme_score=float(row.get("theme_momentum_3d") or 0.0),
        amount_score=float(row.get("effective_amount") or 0.0),
        decision_date=decision_date,
        reason=reason,
        switch_id=switch_id,
        threshold_score=threshold_score,
    )


def build_next_orders(account: Account, trade_date: str, hot_themes: set[str], targets: pd.DataFrame, switch_buffer: float) -> list[PendingOrder]:
    orders: list[PendingOrder] = []
    planned_exit_codes: set[str] = set()
    for position in sorted(account.positions.values(), key=lambda item: item.code):
        if position.core_theme not in hot_themes:
            orders.append(
                PendingOrder(
                    action="SELL",
                    code=position.code,
                    name=position.name,
                    core_theme=position.core_theme,
                    theme_score=position.theme_score,
                    amount_score=position.amount_score,
                    decision_date=trade_date,
                    reason="THEME_COOLDOWN_SELL",
                )
            )
            planned_exit_codes.add(position.code)

    planned_kept_codes = set(account.positions) - planned_exit_codes
    planned_slots = MAX_HOLDINGS - len(planned_kept_codes)
    for row in targets.itertuples(index=False):
        if planned_slots <= 0:
            break
        code = str(getattr(row, "code"))
        if code in planned_kept_codes or code in planned_exit_codes:
            continue
        orders.append(order_from_row(pd.Series(row._asdict()), trade_date, "BUY_HOT_LEADER"))
        planned_kept_codes.add(code)
        planned_slots -= 1

    if orders or len(account.positions) < MAX_HOLDINGS or targets.empty:
        return orders

    worst = min(account.positions.values(), key=lambda item: (item.theme_score, item.amount_score))
    threshold = worst.theme_score + switch_buffer
    for row in targets.itertuples(index=False):
        code = str(getattr(row, "code"))
        if code in account.positions:
            continue
        candidate_score = float(getattr(row, "theme_momentum_3d"))
        if candidate_score <= threshold:
            return orders
        switch_id = f"{trade_date}:{worst.code}->{code}"
        orders.append(
            PendingOrder(
                action="SELL",
                code=worst.code,
                name=worst.name,
                core_theme=worst.core_theme,
                theme_score=worst.theme_score,
                amount_score=worst.amount_score,
                decision_date=trade_date,
                reason="BUFFERED_LEADER_SWITCH_SELL",
                switch_id=switch_id,
                threshold_score=threshold,
            )
        )
        orders.append(order_from_row(pd.Series(row._asdict()), trade_date, "BUFFERED_LEADER_SWITCH_BUY", switch_id, threshold))
        return orders
    return orders


def positions_snapshot(account: Account) -> str:
    if not account.positions:
        return ""
    parts = []
    for pos in sorted(account.positions.values(), key=lambda item: item.code):
        parts.append(f"{pos.code}:{pos.shares}@{pos.last_price:.3f}/{pos.core_theme}/theme={pos.theme_score:.6f}")
    return ";".join(parts)


def simulate_rotation(
    panel: pd.DataFrame,
    initial_capital: float,
    max_position_cash: float,
    buy_cost_rate: float,
    sell_cost_rate: float,
    switch_buffer: float,
) -> dict[str, Any]:
    account = Account(cash=float(initial_capital), positions={})
    pending_orders: list[PendingOrder] = []
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    day_count = 0

    for trade_date, day in panel.groupby("date", sort=True):
        day_count += 1
        by_code = day.set_index("code", drop=False)
        hot_themes, targets, theme_rank = hot_themes_and_targets(day)

        trade_rows.extend(
            execute_pending_orders(
                account=account,
                pending_orders=pending_orders,
                trade_date=str(trade_date),
                by_code=by_code,
                buy_cost_rate=buy_cost_rate,
                sell_cost_rate=sell_cost_rate,
                max_position_cash=max_position_cash,
            )
        )
        pending_orders = []

        trade_rows.extend(run_ma20_stops(account, str(trade_date), by_code, sell_cost_rate))
        mark_positions_to_close(account, str(trade_date), by_code)

        top_theme = theme_rank.iloc[0].to_dict() if not theme_rank.empty else {}
        top_target = targets.iloc[0].to_dict() if not targets.empty else {}
        equity_rows.append(
            {
                "date": str(trade_date),
                "equity": round(portfolio_value(account), 2),
                "cash": round(account.cash, 2),
                "position_count": len(account.positions),
                "positions": positions_snapshot(account),
                "hot_theme_count": int(len(hot_themes)),
                "top_theme": str(top_theme.get("core_theme") or ""),
                "top_theme_momentum_3d": round(float(top_theme.get("theme_momentum_3d") or 0.0), 8),
                "top_target_code": str(top_target.get("code") or ""),
                "top_target_name": str(top_target.get("name") or ""),
                "top_target_amount": round(float(top_target.get("effective_amount") or 0.0), 2),
                "total_friction_cost": round(account.total_friction_cost, 2),
                "buy_count": account.buy_count,
                "sell_count": account.sell_count,
                "switch_count": account.switch_count,
                "theme_cooldown_count": account.theme_cooldown_count,
                "ma20_stop_count": account.ma20_stop_count,
            }
        )

        pending_orders = build_next_orders(account, str(trade_date), hot_themes, targets, switch_buffer)

    final_asset = portfolio_value(account)
    return_pct = (final_asset / initial_capital - 1.0) * 100.0
    return {
        "portfolio": {
            "initial_capital": round(float(initial_capital), 2),
            "final_asset": round(final_asset, 2),
            "total_return_pct": round(return_pct, 4),
            "cash": round(account.cash, 2),
            "holding_market_value": round(final_asset - account.cash, 2),
            "open_position_count": len(account.positions),
            "open_positions": [asdict(pos) for pos in account.positions.values()],
            "total_turnover_count": int(account.sell_count),
            "total_trade_count": int(account.buy_count + account.sell_count),
            "buy_count": int(account.buy_count),
            "sell_count": int(account.sell_count),
            "switch_count": int(account.switch_count),
            "theme_cooldown_count": int(account.theme_cooldown_count),
            "ma20_stop_count": int(account.ma20_stop_count),
            "skipped_order_count": int(account.skipped_order_count),
            "total_friction_cost": round(float(account.total_friction_cost), 2),
        },
        "trade_rows": trade_rows,
        "equity_rows": equity_rows,
        "day_count": day_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="宏观题材与龙头溢价动态轮动模拟器。")
    parser.add_argument("--panel-path", default=str(DEFAULT_PANEL_PATH))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    parser.add_argument("--max-position-cash", type=float, default=MAX_POSITION_CASH)
    parser.add_argument("--buy-cost-rate", type=float, default=BUY_COST_RATE)
    parser.add_argument("--sell-cost-rate", type=float, default=SELL_COST_RATE)
    parser.add_argument("--switch-buffer", type=float, default=SWITCH_BUFFER)
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--trade-detail-path", default=str(TRADE_DETAIL_PATH))
    parser.add_argument("--equity-path", default=str(EQUITY_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    panel_path = Path(args.panel_path)

    print("========== Macro Theme Rotation Simulator ==========")
    print(f"Panel            : {panel_path}")
    print(f"Date Range       : {args.start_date} -> {args.end_date}")
    print(f"Initial Capital  : {float(args.initial_capital):,.2f}")
    print(f"Max Position Cash: {float(args.max_position_cash):,.2f}")
    print(f"Buy/Sell Cost    : {float(args.buy_cost_rate) * 100:.3f}% / {float(args.sell_cost_rate) * 100:.3f}%")
    print(f"Switch Buffer    : {float(args.switch_buffer) * 100:.2f}% absolute theme momentum")

    panel, source_meta = prepare_panel(panel_path, args.start_date, args.end_date)
    actual_start_date = str(panel["date"].min())
    actual_end_date = str(panel["date"].max())
    print(
        f"Panel Loaded     : rows={len(panel):,} days={panel['date'].nunique():,} "
        f"codes={panel['code'].nunique():,} themes={panel['core_theme'].nunique():,}"
    )
    if actual_start_date > args.start_date or actual_end_date < args.end_date:
        print(f"[Warn] 可用历史复盘面板实际覆盖: {actual_start_date} -> {actual_end_date}")

    result = simulate_rotation(
        panel=panel,
        initial_capital=float(args.initial_capital),
        max_position_cash=float(args.max_position_cash),
        buy_cost_rate=float(args.buy_cost_rate),
        sell_cost_rate=float(args.sell_cost_rate),
        switch_buffer=float(args.switch_buffer),
    )
    elapsed = time.perf_counter() - started

    report_path = Path(args.report_path)
    trade_path = Path(args.trade_detail_path)
    equity_path = Path(args.equity_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    trade_path.parent.mkdir(parents=True, exist_ok=True)
    equity_path.parent.mkdir(parents=True, exist_ok=True)

    portfolio = result["portfolio"]
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "scripts/research/simulate_rotation_macro_theme.py",
        "panel_path": str(panel_path),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "signal_contract": "Top 10% core_theme by theme_momentum_3d; within hot themes select amount/effective_amount Top 5 leaders.",
        "ma20_stop_note": "14:50 price uses buy5m_entry_price/entry_price fallback; MA20 is direct ma20/MA20 when present, otherwise approximated from close and ma20_bias.",
        "params": {
            "initial_capital": float(args.initial_capital),
            "max_position_cash": float(args.max_position_cash),
            "max_holdings": MAX_HOLDINGS,
            "target_pool_size": TARGET_POOL_SIZE,
            "hot_theme_quantile": HOT_THEME_QUANTILE,
            "buy_cost_rate": float(args.buy_cost_rate),
            "sell_cost_rate": float(args.sell_cost_rate),
            "round_trip_cost_rate": float(args.buy_cost_rate) + float(args.sell_cost_rate),
            "switch_buffer": float(args.switch_buffer),
            "lot_size": LOT_SIZE,
        },
        "dataset": {
            "row_count": int(len(panel)),
            "day_count": int(panel["date"].nunique()),
            "code_count": int(panel["code"].nunique()),
            "theme_count": int(panel["core_theme"].nunique()),
            "actual_start_date": actual_start_date,
            "actual_end_date": actual_end_date,
            **source_meta,
        },
        "portfolio": portfolio,
        "elapsed_seconds": round(elapsed, 3),
    }
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(result["trade_rows"]).to_csv(trade_path, index=False)
    pd.DataFrame(result["equity_rows"]).to_csv(equity_path, index=False)

    print("\n---------- Macro Rotation Health Report ----------")
    print(f"最终总资金收益率 : {portfolio['total_return_pct']:.2f}%")
    print(f"最终总资产       : {portfolio['final_asset']:,.2f}")
    print(f"总换手次数       : {portfolio['total_turnover_count']} (按持仓卖出/换仓事件计)")
    print(f"总买卖订单       : {portfolio['total_trade_count']} (buy={portfolio['buy_count']}, sell={portfolio['sell_count']})")
    print(f"摩擦成本消耗     : {portfolio['total_friction_cost']:,.2f}")
    print(f"MA20 强平次数    : {portfolio['ma20_stop_count']}")
    print(f"主线退潮清仓次数 : {portfolio['theme_cooldown_count']}")
    print(f"缓冲换龙头次数   : {portfolio['switch_count']}")
    print(f"未成交/跳过订单  : {portfolio['skipped_order_count']}")
    print(f"Report Path      : {report_path}")
    print(f"Trade Detail     : {trade_path}")
    print(f"Equity Curve     : {equity_path}")
    print(f"Elapsed Seconds  : {elapsed:.3f}")
    print("==================================================\n")


if __name__ == "__main__":
    main()
