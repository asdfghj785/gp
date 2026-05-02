from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
import importlib
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Optional, Union

import numpy as np
import pandas as pd
import requests

from quant_core.config import BASE_DIR, DATA_DIR


SECTOR_KLINE_DIR = BASE_DIR / "data" / "sector_kline"
SECTOR_INDEX_PATH = SECTOR_KLINE_DIR / "sector_index.parquet"
STOCK_SECTOR_MAP_PATH = SECTOR_KLINE_DIR / "stock_sector_map.parquet"
STOCK_SECTOR_MAP_CSV_PATH = SECTOR_KLINE_DIR / "stock_sector_map.csv"
STOCK_SECTOR_MAP_JSON_PATH = SECTOR_KLINE_DIR / "stock_sector_map.json"
DEFAULT_SECTOR_START_DATE = "20250121"
DEFAULT_DUMMY_SECTOR = "全指工业"
SECTOR_FACTOR_COLUMNS = [
    "sector_pct_chg_1",
    "sector_pct_chg_3",
    "rs_stock_vs_sector",
    "rs_ema_5",
    "sector_volatility_5",
]
SECTOR_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("全指金融", ("银行", "证券", "保险", "信托", "金融", "期货", "资管", "财富", "地产", "置业", "房产")),
    ("全指医药", ("医", "药", "生物", "疫苗", "医疗", "诊断", "健康", "眼科", "牙科", "制药", "药业")),
    ("全指通信", ("通信", "通讯", "电信", "移动", "联通", "广电", "光纤", "光缆", "光迅", "传媒", "出版", "影视")),
    ("全指信息", ("软件", "科技", "电子", "半导体", "芯片", "信息", "计算机", "数字", "网络", "数据", "光电", "微电", "集成", "智能", "互联")),
    ("全指能源", ("煤", "炭", "石油", "油气", "天然气", "能源", "矿业", "煤业", "油服", "焦煤", "焦化")),
    ("全指材料", ("钢", "铁", "铜", "铝", "锌", "镍", "锂", "稀土", "有色", "金属", "化工", "化学", "材料", "水泥", "建材", "玻璃", "塑料", "橡胶", "造纸", "钛", "磷", "硅", "钨", "钼", "黄金")),
    ("全指消费", ("食品", "饮料", "白酒", "啤酒", "乳", "农业", "农", "牧", "渔", "肉", "糖", "种业", "粮", "盐", "调味")),
    ("全指可选", ("汽车", "汽配", "家电", "旅游", "酒店", "餐饮", "服装", "纺织", "零售", "商贸", "百货", "珠宝", "教育", "游戏", "影院", "文旅", "家居", "家装")),
    ("全指公用", ("电力", "水务", "环保", "环境", "公用", "热电", "水电", "发电", "核电", "供水", "城投", "高速", "港口", "机场")),
]


def sync_sector_daily(
    *,
    start_date: str = DEFAULT_SECTOR_START_DATE,
    end_date: Optional[str] = None,
    refresh_mapping: bool = True,
    limit: int = 0,
) -> dict[str, Any]:
    """Fetch and cache industry daily bars through mpquant/Ashare.

    Ashare exposes a single `get_price` K-line interface. It does not provide
    an industry board list or stock-to-industry mapping, so those tables must
    come from `data/sector_kline` local cache. This function never falls back
    to any other board-data provider; missing sector cache is recorded as a best-effort
    sync failure and the factor factory will fill sector factors with 0.
    """
    SECTOR_KLINE_DIR.mkdir(parents=True, exist_ok=True)
    end = _yyyymmdd(end_date or date.today())
    started_at = datetime.now().isoformat(timespec="seconds")
    summary: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": started_at,
        "status": "running",
        "source": "mpquant/Ashare.get_price",
        "sector_count": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "mapping_rows": 0,
        "errors": [],
    }

    boards = _sector_boards_from_local_cache()
    if boards.empty:
        _append_error(
            summary,
            "本地缺少 sector_index/stock_sector_map 行业缓存；Ashare 只提供 get_price K 线，不提供行业列表或成分映射。",
        )
    if limit and limit > 0:
        boards = boards.head(int(limit))
    summary["sector_count"] = int(len(boards))

    index_rows: list[dict[str, Any]] = []
    for row in boards.to_dict("records"):
        sector_name = str(row.get("sector_name") or "").strip()
        if not sector_name:
            continue
        sector_code = normalize_ashare_code(row.get("ashare_code") or row.get("sector_code") or row.get("code") or row.get("symbol"))
        path = sector_cache_path(sector_name)
        if not sector_code:
            summary["failed"] += 1
            _append_error(summary, f"{sector_name}: 本地行业索引缺少 Ashare 可识别代码")
            if path.exists():
                index_rows.append(_sector_index_row(sector_name, "", path))
            continue
        fetch_start = _next_fetch_start(path, start_date)
        if fetch_start > end:
            summary["skipped"] += 1
            index_rows.append(_sector_index_row(sector_name, sector_code, path))
            continue
        try:
            fresh = _fetch_sector_daily_from_ashare(sector_code, sector_name, fetch_start, end)
            if fresh.empty:
                summary["skipped"] += 1
            else:
                merged = _merge_sector_daily_cache(path, fresh)
                merged.to_parquet(path, index=False)
                summary["updated"] += 1
            index_rows.append(_sector_index_row(sector_name, sector_code, path))
        except Exception as exc:
            summary["failed"] += 1
            _append_error(summary, f"{sector_name}: {exc}")
            if path.exists():
                index_rows.append(_sector_index_row(sector_name, sector_code, path))

    if index_rows:
        pd.DataFrame(index_rows).drop_duplicates(subset=["sector_name"], keep="last").to_parquet(SECTOR_INDEX_PATH, index=False)

    if refresh_mapping:
        mapping_summary = sync_stock_sector_map(boards)
        summary["mapping_rows"] = int(mapping_summary.get("rows") or 0)
        if mapping_summary.get("status") == "fail":
            summary["failed"] += 1
            _append_error(summary, str(mapping_summary.get("error") or "stock sector mapping failed"))

    if int(summary["sector_count"] or 0) == 0 and summary.get("errors"):
        summary["status"] = "fail"
    else:
        summary["status"] = "success" if summary["failed"] == 0 and not summary.get("errors") else "partial"
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    summary_path = SECTOR_KLINE_DIR / f"sector_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        summary["summary_path"] = str(summary_path)
    except Exception:
        pass
    return summary


def sync_stock_sector_map(boards: Optional[pd.DataFrame] = None) -> dict[str, Any]:
    SECTOR_KLINE_DIR.mkdir(parents=True, exist_ok=True)
    mapping = _cached_stock_sector_map()
    if mapping.empty:
        return {
            "status": "fail",
            "rows": 0,
            "error": "Ashare.get_price 不提供个股行业映射；请先维护 data/sector_kline/stock_sector_map.parquet 或 .csv。",
        }
    mapping.to_parquet(STOCK_SECTOR_MAP_PATH, index=False)
    return {
        "status": "cached",
        "rows": int(len(mapping)),
        "path": str(STOCK_SECTOR_MAP_PATH),
    }


def build_stock_sector_map_from_local_universe(
    *,
    data_dir: Optional[Union[str, Path]] = None,
    default_sector: str = DEFAULT_DUMMY_SECTOR,
) -> dict[str, Any]:
    """Build a coarse stock -> sector mapping from local daily files.

    The mapper is intentionally offline and deterministic. It uses stock-name
    keywords for the 10 cached CSI all-share sector indexes and falls back to
    `default_sector` for unclassified stocks, so the factor factory can always
    run without external industry classification APIs.
    """
    SECTOR_KLINE_DIR.mkdir(parents=True, exist_ok=True)
    sector_index = _cached_sector_index()
    if sector_index.empty:
        raise RuntimeError("缺少 data/sector_kline/sector_index.parquet，无法确认可用行业指数名称")
    sector_code_map = {
        str(row.get("sector_name") or "").strip(): str(row.get("sector_code") or "").strip()
        for row in sector_index.to_dict("records")
    }
    if default_sector not in sector_code_map:
        default_sector = str(sector_index["sector_name"].iloc[0])

    rows = []
    for item in _local_stock_universe(data_dir or DATA_DIR):
        sector_name, method = classify_stock_sector(str(item.get("name") or ""), default_sector=default_sector)
        if sector_name not in sector_code_map:
            sector_name = default_sector
            method = "fallback_missing_sector"
        rows.append(
            {
                "code": item["code"],
                "name": item.get("name") or "",
                "sector_name": sector_name,
                "sector_code": sector_code_map.get(sector_name, ""),
                "mapping_method": method,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )

    if not rows:
        raise RuntimeError(f"本地日线目录没有可用股票文件: {data_dir or DATA_DIR}")

    mapping = pd.DataFrame(rows).drop_duplicates(subset=["code"], keep="first").sort_values("code").reset_index(drop=True)
    mapping.to_parquet(STOCK_SECTOR_MAP_PATH, index=False)
    mapping.to_csv(STOCK_SECTOR_MAP_CSV_PATH, index=False)
    STOCK_SECTOR_MAP_JSON_PATH.write_text(
        json.dumps(mapping.to_dict("records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    counts = mapping["sector_name"].value_counts().to_dict()
    methods = mapping["mapping_method"].value_counts().to_dict()
    return {
        "status": "success",
        "rows": int(len(mapping)),
        "sector_counts": {str(k): int(v) for k, v in counts.items()},
        "method_counts": {str(k): int(v) for k, v in methods.items()},
        "parquet_path": str(STOCK_SECTOR_MAP_PATH),
        "csv_path": str(STOCK_SECTOR_MAP_CSV_PATH),
        "json_path": str(STOCK_SECTOR_MAP_JSON_PATH),
    }


def classify_stock_sector(name: str, *, default_sector: str = DEFAULT_DUMMY_SECTOR) -> tuple[str, str]:
    text = str(name or "")
    for sector_name, keywords in SECTOR_KEYWORDS:
        for keyword in keywords:
            if keyword and keyword in text:
                return sector_name, f"keyword:{keyword}"
    return default_sector, "fallback_default"


def get_stock_sector_map(refresh: bool = False) -> dict[str, str]:
    """Return a 6-digit stock code -> industry board name mapping."""
    if refresh:
        sync_stock_sector_map()
    df = _cached_stock_sector_map()
    if df.empty or "code" not in df.columns or "sector_name" not in df.columns:
        return {}
    codes = df["code"].map(normalize_stock_code)
    names = df["sector_name"].fillna("").astype(str)
    return {code: name for code, name in zip(codes, names) if code and name}


def load_sector_daily(sector_name: str) -> pd.DataFrame:
    path = sector_cache_path(sector_name)
    if not path.exists():
        return pd.DataFrame()
    try:
        return normalize_sector_daily_frame(pd.read_parquet(path), sector_name=sector_name)
    except Exception:
        return pd.DataFrame()


def sector_relative_factor_frame(
    stock_frame: pd.DataFrame,
    *,
    code: Optional[str] = None,
    sector_map: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """Build sector resonance and relative-strength factors for one stock.

    Missing mapping or missing sector bars returns a zero-filled factor block
    with the same index as stock_frame.
    """
    zero = pd.DataFrame(0.0, index=stock_frame.index, columns=SECTOR_FACTOR_COLUMNS)
    if stock_frame.empty or "datetime" not in stock_frame.columns or "close" not in stock_frame.columns:
        return zero

    stock_code = normalize_stock_code(code or _first_non_empty(stock_frame.get("code")) or _first_non_empty(stock_frame.get("symbol")))
    if not stock_code:
        return zero
    mapping = sector_map if sector_map is not None else get_stock_sector_map(refresh=False)
    sector_name = mapping.get(stock_code, "")
    if not sector_name:
        return zero
    sector = load_sector_daily(sector_name)
    if sector.empty:
        return zero

    stock_dates = pd.to_datetime(stock_frame["datetime"], errors="coerce").dt.normalize()
    stock_close = pd.to_numeric(stock_frame["close"], errors="coerce")
    stock_ret = stock_close.pct_change()

    sector = sector.copy()
    sector["datetime"] = pd.to_datetime(sector["datetime"], errors="coerce").dt.normalize()
    sector = sector.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")
    sector_close = pd.to_numeric(sector["close"], errors="coerce")
    if "pct_chg" in sector.columns:
        sector_ret = _normalize_pct_series(sector["pct_chg"])
    else:
        sector_ret = sector_close.pct_change()
    sector["sector_pct_chg_1"] = sector_ret
    sector["sector_pct_chg_3"] = (1 + sector_ret).rolling(3, min_periods=1).apply(np.prod, raw=True) - 1
    sector["sector_volatility_5"] = _sector_true_range_pct(sector).rolling(5, min_periods=1).mean()

    aligned = pd.DataFrame({"datetime": stock_dates}, index=stock_frame.index).merge(
        sector[["datetime", "sector_pct_chg_1", "sector_pct_chg_3", "sector_volatility_5"]],
        on="datetime",
        how="left",
    )
    out = pd.DataFrame(index=stock_frame.index)
    out["sector_pct_chg_1"] = pd.to_numeric(aligned["sector_pct_chg_1"], errors="coerce")
    out["sector_pct_chg_3"] = pd.to_numeric(aligned["sector_pct_chg_3"], errors="coerce")
    out["rs_stock_vs_sector"] = stock_ret.reset_index(drop=True) - out["sector_pct_chg_1"].reset_index(drop=True)
    out["rs_ema_5"] = out["rs_stock_vs_sector"].ewm(span=5, adjust=False, min_periods=1).mean()
    out["sector_volatility_5"] = pd.to_numeric(aligned["sector_volatility_5"], errors="coerce")
    out.index = stock_frame.index
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)[SECTOR_FACTOR_COLUMNS]


def sector_cache_path(sector_name: str) -> Path:
    digest = hashlib.md5(str(sector_name).encode("utf-8")).hexdigest()[:10]
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", str(sector_name)).strip("_")[:40] or "sector"
    return SECTOR_KLINE_DIR / f"{slug}_{digest}.parquet"


def normalize_stock_code(value: Any) -> str:
    text = str(value or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits[-6:] if len(digits) >= 6 else digits.zfill(6)


def normalize_ashare_code(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value or "").strip()
    if not text:
        return ""
    upper = text.upper()
    if upper.endswith((".XSHG", ".XSHE")):
        return text
    lower = text.lower()
    if lower.startswith(("sh", "sz")) and len(lower) >= 8:
        return lower[:2] + re.sub(r"\D", "", lower[2:])[:6]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        code = digits[-6:]
        return f"sh{code}" if code.startswith(("5", "6", "9")) else f"sz{code}"
    return text


def normalize_sector_daily_frame(df: pd.DataFrame, *, sector_name: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "date", "sector_name", "open", "high", "low", "close", "volume", "amount", "pct_chg"])
    out = pd.DataFrame(index=df.index)
    datetime_col = _col(df, ["datetime", "date", "time", "日期", "day"])
    if pd.to_datetime(datetime_col, errors="coerce").notna().any():
        out["datetime"] = pd.to_datetime(datetime_col, errors="coerce")
    else:
        out["datetime"] = pd.to_datetime(df.index, errors="coerce")
    out["date"] = out["datetime"].dt.date.astype(str)
    out["sector_name"] = sector_name or str(_first_non_empty(df.get("sector_name")) or "")
    out["open"] = _numeric_col(df, ["open", "开盘"])
    out["high"] = _numeric_col(df, ["high", "最高"])
    out["low"] = _numeric_col(df, ["low", "最低"])
    out["close"] = _numeric_col(df, ["close", "收盘"])
    out["volume"] = _numeric_col(df, ["volume", "vol", "成交量"])
    out["amount"] = _numeric_col(df, ["amount", "money", "成交额"])
    pct_chg = _normalize_pct_series(_numeric_col(df, ["pct_chg", "pctChg", "change_pct", "涨跌幅"]))
    out["pct_chg"] = pct_chg if pct_chg.notna().any() else out["close"].pct_change(fill_method=None)
    out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
    out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)
    return out


def _fetch_sector_daily_from_ashare(ashare_code: str, sector_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    get_price = _import_ashare_get_price()
    start_ts = pd.Timestamp(_yyyymmdd(start_date))
    end_ts = pd.Timestamp(_yyyymmdd(end_date))
    count = max(80, int((end_ts - start_ts).days) + 20)
    raw = get_price(ashare_code, frequency="1d", count=count, end_date=end_ts.strftime("%Y-%m-%d"))
    frame = normalize_sector_daily_frame(raw, sector_name=sector_name)
    if frame.empty:
        return _fetch_sector_daily_from_ashare_tencent_backup(ashare_code, sector_name, count, start_ts, end_ts)
    mask = (frame["datetime"] >= start_ts) & (frame["datetime"] <= end_ts + pd.Timedelta(days=1))
    filtered = frame.loc[mask].copy()
    if filtered.empty:
        return _fetch_sector_daily_from_ashare_tencent_backup(ashare_code, sector_name, count, start_ts, end_ts)
    return filtered


def _fetch_sector_daily_from_ashare_tencent_backup(
    ashare_code: str,
    sector_name: str,
    count: int,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    # mpquant/Ashare contains this Tencent backup URL, but its parser casts the
    # date column to float for some index payloads. Keep the same source shape
    # and parse the date column explicitly here.
    url = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        url,
        params={"param": f"{ashare_code},day,,{end_ts.strftime('%Y-%m-%d')},{count},qfq"},
        timeout=12,
        proxies={},
    )
    response.raise_for_status()
    payload = response.json()
    data = (payload.get("data") or {}).get(ashare_code) or {}
    rows = data.get("qfqday") or data.get("day") or []
    parsed = pd.DataFrame(rows, columns=["datetime", "open", "close", "high", "low", "volume"])
    frame = normalize_sector_daily_frame(parsed, sector_name=sector_name)
    if frame.empty:
        return frame
    mask = (frame["datetime"] >= start_ts) & (frame["datetime"] <= end_ts + pd.Timedelta(days=1))
    return frame.loc[mask].copy()


def _import_ashare_get_price() -> Callable[..., pd.DataFrame]:
    root = str(BASE_DIR)
    if root not in sys.path:
        sys.path.insert(0, root)
    last_error: Optional[Exception] = None
    for module_name in ("Ashare", "ashare"):
        try:
            module = importlib.import_module(module_name)
            get_price = getattr(module, "get_price", None)
            if callable(get_price):
                return get_price
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        "未找到 mpquant/Ashare 的 get_price 接口；请将 https://github.com/mpquant/Ashare 的 Ashare.py "
        f"放入项目根目录或 PYTHONPATH。本模块不会回退其他板块数据源。last_error={last_error}"
    )


def _merge_sector_daily_cache(path: Path, fresh: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if path.exists():
        try:
            frames.append(pd.read_parquet(path))
        except Exception:
            pass
    frames.append(fresh)
    merged = normalize_sector_daily_frame(pd.concat(frames, ignore_index=True), sector_name=str(fresh["sector_name"].iloc[-1]))
    return merged


def _sector_boards_from_local_cache() -> pd.DataFrame:
    frames = []
    sector_index = _cached_sector_index()
    if not sector_index.empty:
        frames.append(sector_index)
    mapping = _cached_stock_sector_map()
    if not mapping.empty:
        frames.append(mapping)
    if not frames:
        return pd.DataFrame(columns=["sector_name", "sector_code"])
    return _normalize_sector_board_frame(pd.concat(frames, ignore_index=True))


def _cached_sector_index() -> pd.DataFrame:
    frame = _read_local_table(SECTOR_INDEX_PATH)
    if frame.empty:
        frame = _read_local_table(SECTOR_INDEX_PATH.with_suffix(".csv"))
    if frame.empty:
        return pd.DataFrame(columns=["sector_name", "sector_code", "path"])
    return _normalize_sector_board_frame(frame)


def _cached_stock_sector_map() -> pd.DataFrame:
    frame = _read_local_table(STOCK_SECTOR_MAP_PATH)
    if frame.empty:
        frame = _read_local_table(STOCK_SECTOR_MAP_PATH.with_suffix(".csv"))
    if frame.empty:
        return pd.DataFrame(columns=["code", "name", "sector_name", "sector_code"])
    return _normalize_stock_sector_map_frame(frame)


def _local_stock_universe(data_dir: str | Path) -> list[dict[str, str]]:
    directory = Path(data_dir)
    files = sorted(directory.glob("*_daily.parquet"))
    if not files:
        files = sorted(directory.glob("*.parquet")) + sorted(directory.glob("*.csv"))
    rows: list[dict[str, str]] = []
    for path in files:
        code = normalize_stock_code(path.stem.split("_")[0])
        name = ""
        try:
            if path.suffix.lower() == ".csv":
                frame = pd.read_csv(path, usecols=lambda col: col in {"code", "symbol", "name"}, nrows=10)
            else:
                try:
                    frame = pd.read_parquet(path, columns=["code", "name"])
                except Exception:
                    frame = pd.read_parquet(path)
            if not frame.empty:
                if "code" in frame.columns:
                    code = normalize_stock_code(_first_non_empty(frame["code"])) or code
                elif "symbol" in frame.columns:
                    code = normalize_stock_code(_first_non_empty(frame["symbol"])) or code
                if "name" in frame.columns:
                    name = _first_non_empty(frame["name"])
        except Exception:
            pass
        if code:
            rows.append({"code": code, "name": name or code})
    return rows


def _read_local_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _normalize_sector_board_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["sector_name", "sector_code"])
    out = pd.DataFrame()
    out["sector_name"] = _col(
        df,
        ["sector_name", "industry_name", "board_name", "name", "行业名称", "所属行业", "板块名称", "行业"],
    ).fillna("").astype(str).str.strip()
    out["sector_code"] = _col(
        df,
        ["ashare_code", "sector_code", "board_code", "index_code", "symbol", "code", "指数代码", "板块代码", "代码"],
    ).map(normalize_ashare_code)
    out = out[out["sector_name"] != ""].copy()
    return out.drop_duplicates(subset=["sector_name"], keep="last").reset_index(drop=True)


def _normalize_stock_sector_map_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "name", "sector_name", "sector_code"])
    out = pd.DataFrame()
    out["code"] = _col(df, ["code", "symbol", "股票代码", "证券代码", "代码"]).map(normalize_stock_code)
    out["name"] = _col(df, ["name", "stock_name", "股票名称", "证券简称", "名称"]).fillna("").astype(str)
    out["sector_name"] = _col(
        df,
        ["sector_name", "industry_name", "board_name", "所属行业", "行业名称", "板块名称", "行业"],
    ).fillna("").astype(str).str.strip()
    out["sector_code"] = _col(
        df,
        ["ashare_code", "sector_code", "industry_code", "board_code", "index_code", "行业代码", "板块代码"],
    ).map(normalize_ashare_code)
    out["updated_at"] = datetime.now().isoformat(timespec="seconds")
    out = out[(out["code"] != "") & (out["sector_name"] != "")].copy()
    return out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)


def _sector_index_row(sector_name: str, sector_code: str, path: Path) -> dict[str, Any]:
    return {
        "sector_name": sector_name,
        "sector_code": sector_code,
        "path": str(path),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _next_fetch_start(path: Path, default_start: str) -> str:
    default = _yyyymmdd(default_start)
    if not path.exists():
        return default
    try:
        cached = pd.read_parquet(path, columns=["datetime"])
        if cached.empty:
            return default
        dates = pd.to_datetime(cached["datetime"], errors="coerce").dropna()
        if dates.empty:
            return default
        first_dt = pd.Timestamp(dates.min())
        last_dt = pd.Timestamp(dates.max())
        if first_dt > pd.Timestamp(default):
            return default
        return max(default, last_dt.strftime("%Y%m%d"))
    except Exception:
        return default


def _sector_true_range_pct(sector: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(sector["high"], errors="coerce")
    low = pd.to_numeric(sector["low"], errors="coerce")
    close = pd.to_numeric(sector["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr / close.replace(0, np.nan)


def _normalize_pct_series(value: Any) -> pd.Series:
    series = pd.to_numeric(value, errors="coerce") if not isinstance(value, pd.Series) else pd.to_numeric(value, errors="coerce")
    finite = series.replace([np.inf, -np.inf], np.nan).dropna()
    if not finite.empty and finite.abs().quantile(0.95) > 1.5:
        series = series / 100.0
    return series


def _col(df: pd.DataFrame, names: Iterable[str]) -> Any:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([np.nan] * len(df), index=df.index)


def _numeric_col(df: pd.DataFrame, names: Iterable[str]) -> pd.Series:
    return pd.to_numeric(_col(df, names), errors="coerce")


def _first_non_empty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Series):
        for item in value.dropna().astype(str):
            if item:
                return item
        return ""
    text = str(value or "")
    return text.strip()


def _yyyymmdd(value: Any) -> str:
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
        return pd.to_datetime(value).strftime("%Y%m%d")
    return pd.Timestamp(value).strftime("%Y%m%d")


def _append_error(summary: dict[str, Any], message: str) -> None:
    errors = summary.setdefault("errors", [])
    if isinstance(errors, list) and len(errors) < 20:
        errors.append(message)
