from __future__ import annotations

import argparse
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from jqdatasdk import auth, get_price

from quant_core.config import MIN_KLINE_DIR
from quant_core.data_pipeline.tencent_engine import get_tencent_m5


SUPPORTED_PERIODS = {"1", "5", "15", "30", "60"}
PRICE_FIELDS = ["open", "close", "high", "low", "volume", "money"]
REQUIRED_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "money"]
JQ_DELAY_CUTOFF = datetime(2026, 1, 25, 15, 0, 0)
_JQ_AUTHED = False


def init_jq(username: str | None = None, password: str | None = None, force: bool = False) -> None:
    """Authenticate jqdatasdk once for the current process."""
    global _JQ_AUTHED
    if _JQ_AUTHED and not force:
        return

    import os

    user = (username or os.getenv("JQ_USERNAME") or "").strip()
    pwd = (password or os.getenv("JQ_PASSWORD") or "").strip()
    if not user or not pwd:
        raise RuntimeError("缺少聚宽鉴权信息：请在 /Users/eudis/ths/.env 配置 JQ_USERNAME 和 JQ_PASSWORD")

    try:
        auth(user, pwd)
    except Exception as exc:  # pragma: no cover - depends on external API
        raise RuntimeError(f"聚宽登录失败，请检查 JQ_USERNAME/JQ_PASSWORD 或网络连接：{exc}") from exc
    _JQ_AUTHED = True


def get_stock_min_data(
    code: str,
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Fetch A-share minute K lines through the dual-source smart router.

    Output columns follow the local Parquet contract:
    ``datetime, open, close, high, low, volume, money`` plus compatibility
    metadata. ``amount`` is kept as an alias of ``money`` for existing APIs.
    """
    return smart_fetch_minute(code, period=period, start_date=start_date, end_date=end_date)


def smart_fetch_minute(
    code: str,
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Route delayed historical data to JoinQuant and recent data to Tencent.

    JoinQuant is used for data ending on or before ``JQ_DELAY_CUTOFF``.
    Any request containing data after that cutoff is filled by Tencent M5.
    Spanning requests are split and merged with deterministic de-duplication.
    """
    safe_code = normalize_stock_code(code)
    safe_period = normalize_period(period)
    start_dt = parse_minute_datetime(start_date, is_end=False)
    end_dt = parse_minute_datetime(end_date, is_end=True)
    if start_dt > end_dt:
        raise ValueError(f"开始时间不能晚于结束时间：{start_dt} > {end_dt}")

    frames: list[pd.DataFrame] = []
    if end_dt <= JQ_DELAY_CUTOFF:
        frames.append(fetch_from_jq(safe_code, safe_period, start_dt, end_dt))
    elif start_dt > JQ_DELAY_CUTOFF:
        frames.append(fetch_from_tencent(safe_code, safe_period, start_dt, end_dt))
    else:
        frames.append(fetch_from_jq(safe_code, safe_period, start_dt, JQ_DELAY_CUTOFF))
        tencent_start = max(start_dt, datetime.combine((JQ_DELAY_CUTOFF + timedelta(days=1)).date(), dt_time(9, 30)))
        if tencent_start <= end_dt:
            frames.append(fetch_from_tencent(safe_code, safe_period, tencent_start, end_dt))

    if not frames:
        return normalize_minute_frame(pd.DataFrame(), code=safe_code, period=safe_period)
    if len(frames) == 1:
        return normalize_minute_frame(frames[0], code=safe_code, period=safe_period)
    merged = pd.concat(frames, ignore_index=True)
    merged = normalize_minute_frame(merged, code=safe_code, period=safe_period, source="mixed.jq_tencent")
    return merged.drop_duplicates(subset=["code", "period", "datetime"], keep="last").sort_values("datetime").reset_index(drop=True)


def fetch_from_jq(
    code: str,
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Fetch delayed historical minute data from JoinQuant."""
    init_jq()
    safe_code = normalize_stock_code(code)
    jq_code = normalize_code(safe_code)
    safe_period = normalize_period(period)
    frequency = f"{safe_period}m"
    start_text = normalize_minute_datetime(start_date, is_end=False)
    end_text = normalize_minute_datetime(end_date, is_end=True)

    try:
        raw = get_price(
            jq_code,
            start_date=start_text,
            end_date=end_text,
            frequency=frequency,
            fields=PRICE_FIELDS,
            fq="pre",
            panel=False,
        )
    except Exception as exc:  # pragma: no cover - depends on external API
        raise RuntimeError(f"聚宽分钟线获取失败：{safe_code} {frequency} {start_text} -> {end_text}: {exc}") from exc

    return normalize_minute_frame(raw, safe_code, safe_period, jq_code=jq_code, source="jqdatasdk.get_price")


def fetch_from_tencent(
    code: str,
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Fetch recent 5-minute data from Tencent and trim to the requested range."""
    safe_code = normalize_stock_code(code)
    safe_period = normalize_period(period)
    if safe_period != "5":
        raise ValueError("腾讯实时热数据当前只支持 5 分钟周期")
    start_dt = parse_minute_datetime(start_date, is_end=False)
    end_dt = parse_minute_datetime(end_date, is_end=True)
    count = _estimate_tencent_count(start_dt, end_dt)
    raw = get_tencent_m5(safe_code, count=count)
    if raw.empty:
        return normalize_minute_frame(raw, safe_code, safe_period, jq_code=normalize_code(safe_code), source="tencent.m5")
    raw = raw[(raw["datetime"] >= start_dt) & (raw["datetime"] <= end_dt)].copy()
    raw["money"] = 0.0
    return normalize_minute_frame(raw, safe_code, safe_period, jq_code=normalize_code(safe_code), source="tencent.m5")


def save_stock_min_data(
    code: str,
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    output_root: str | Path = MIN_KLINE_DIR,
    merge_existing: bool = True,
) -> dict[str, object]:
    """Fetch one stock and persist it to ``data/min_kline/{period}m/{symbol}.parquet``."""
    safe_code = normalize_stock_code(code)
    df = get_stock_min_data(safe_code, period=period, start_date=start_date, end_date=end_date)
    path = minute_parquet_path(safe_code, period=period, output_root=output_root)
    written_rows = write_minute_parquet(df, path, code=safe_code, period=period, merge_existing=merge_existing)
    return {
        "code": safe_code,
        "jq_code": normalize_code(safe_code),
        "symbol": prefixed_symbol(safe_code),
        "period": f"{normalize_period(period)}m",
        "path": str(path),
        "fetched_rows": len(df),
        "written_rows": written_rows,
        "status": "saved" if written_rows else "empty",
    }


def batch_fetch_min_data(
    codes: Iterable[str],
    period: str = "5",
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    output_root: str | Path = MIN_KLINE_DIR,
    merge_existing: bool = True,
) -> dict[str, object]:
    """Fetch multiple stocks through JoinQuant. Rate limiting is handled by quota checks upstream."""
    safe_period = normalize_period(period)
    code_list = [normalize_stock_code(code) for code in codes]
    results: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    init_jq()
    for index, code in enumerate(code_list, start=1):
        try:
            result = save_stock_min_data(
                code,
                period=safe_period,
                start_date=start_date,
                end_date=end_date,
                output_root=output_root,
                merge_existing=merge_existing,
            )
            results.append(result)
            print(f"[jq-min-kline] {index}/{len(code_list)} saved {code}: {result['written_rows']} rows")
        except Exception as exc:
            item = {"code": code, "error": str(exc)}
            failed.append(item)
            print(f"[jq-min-kline][ERROR] {index}/{len(code_list)} failed {code}: {exc}")

    return {
        "period": f"{safe_period}m",
        "total": len(code_list),
        "success": len(results),
        "failed": len(failed),
        "results": results,
        "errors": failed,
    }


def write_minute_parquet(
    df: pd.DataFrame,
    path: str | Path,
    code: str | None = None,
    period: str | None = None,
    merge_existing: bool = True,
) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    inferred_code = code or _infer_code_from_path(path)
    inferred_period = period or _infer_period_from_path(path)
    clean = normalize_minute_frame(df, code=inferred_code, period=inferred_period)
    if merge_existing and path.exists():
        existing = pd.read_parquet(path)
        clean = pd.concat([existing, clean], ignore_index=True)
        clean = normalize_minute_frame(clean, code=inferred_code, period=inferred_period)
    clean = clean.drop_duplicates(subset=["code", "period", "datetime"], keep="last")
    clean = clean.sort_values(["datetime", "code"]).reset_index(drop=True)
    if clean.empty:
        return 0
    clean.to_parquet(path, engine="pyarrow", index=False)
    return len(clean)


def normalize_minute_frame(
    df: pd.DataFrame,
    code: str | None = None,
    period: str | None = None,
    jq_code: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    columns = [*REQUIRED_COLUMNS, "amount", "code", "jq_code", "symbol", "period", "source", "ingested_at"]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    out = df.copy()
    out = _lift_datetime_index(out)
    out = out.rename(
        columns={
            "time": "datetime",
            "date": "datetime",
            "index": "datetime",
            "时间": "datetime",
            "日期": "datetime",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "money",
        }
    )
    if "money" not in out.columns and "amount" in df.columns:
        out["money"] = df["amount"]
    if "datetime" not in out.columns:
        raise ValueError(f"聚宽返回数据缺少 datetime/time/date 列，实际列：{list(df.columns)}")
    for col in REQUIRED_COLUMNS:
        if col not in out.columns:
            raise ValueError(f"聚宽返回数据缺少必要列 {col}，实际列：{list(df.columns)}")

    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "money"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    safe_code = normalize_stock_code(code or _infer_code_from_frame(out) or "")
    out["code"] = safe_code
    out["jq_code"] = jq_code or normalize_code(safe_code)
    out["symbol"] = prefixed_symbol(safe_code)
    out["period"] = normalize_period(period or (str(out["period"].iloc[0]) if "period" in out.columns and len(out) else "5"))
    out["amount"] = out["money"]
    if source is not None:
        out["source"] = source
    elif "source" not in out.columns:
        out["source"] = "unknown"
    else:
        out["source"] = out["source"].fillna("unknown").astype(str)
    out["ingested_at"] = datetime.now().isoformat(timespec="seconds")

    out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
    out = out.replace([float("inf"), float("-inf")], pd.NA)
    out = out.dropna(subset=["datetime"])
    return out[columns]


def _lift_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        out = df.reset_index()
        first = out.columns[0]
        if first != "datetime":
            out = out.rename(columns={first: "datetime"})
        return out
    if df.index.name in {"time", "date", "datetime"}:
        return df.reset_index()
    return df


def minute_parquet_path(code: str, period: str = "5", output_root: str | Path = MIN_KLINE_DIR) -> Path:
    safe_period = normalize_period(period)
    return Path(output_root) / f"{safe_period}m" / f"{prefixed_symbol(code)}.parquet"


def _infer_code_from_path(path: Path) -> str | None:
    digits = "".join(ch for ch in path.stem if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else None


def _infer_period_from_path(path: Path) -> str | None:
    parent = path.parent.name.lower().removesuffix("m")
    return parent if parent in SUPPORTED_PERIODS else None


def _infer_code_from_frame(df: pd.DataFrame) -> str | None:
    for col in ("code", "jq_code", "security"):
        if col in df.columns and len(df):
            value = str(df[col].iloc[0])
            digits = "".join(ch for ch in value if ch.isdigit())
            if len(digits) >= 6:
                return digits[-6:]
    return None


def normalize_stock_code(code: str) -> str:
    text = str(code).strip().upper()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 6:
        raise ValueError(f"非法股票代码：{code}")
    return digits[-6:]


def normalize_code(code: str) -> str:
    """Convert local 6-digit/Sina code to JoinQuant code."""
    text = str(code).strip().upper()
    if text.endswith((".XSHE", ".XSHG")) and len(text.split(".", 1)[0]) == 6:
        return text
    safe_code = normalize_stock_code(text)
    if safe_code.startswith(("5", "6", "9")):
        return f"{safe_code}.XSHG"
    return f"{safe_code}.XSHE"


def prefixed_symbol(code: str) -> str:
    safe_code = normalize_stock_code(code)
    if safe_code.startswith(("5", "6", "9")):
        return f"sh{safe_code}"
    return f"sz{safe_code}"


def normalize_period(period: str | int) -> str:
    value = str(period).strip().replace("m", "")
    if value not in SUPPORTED_PERIODS:
        raise ValueError(f"不支持的分钟周期：{period}，可选 {sorted(SUPPORTED_PERIODS)}")
    return value


def normalize_minute_datetime(value: str | date | datetime | None, is_end: bool) -> str:
    return parse_minute_datetime(value, is_end=is_end).strftime("%Y-%m-%d %H:%M:%S")


def parse_minute_datetime(value: str | date | datetime | None, is_end: bool) -> datetime:
    if value is None:
        return datetime.combine(datetime.now().date(), dt_time(15, 0)) if is_end else datetime(1979, 9, 1, 9, 30)
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        default_time = dt_time(15, 0) if is_end else dt_time(9, 30)
        return datetime.combine(value, default_time)

    text = str(value).strip()
    if not text:
        return parse_minute_datetime(None, is_end=is_end)
    if len(text) == 8 and text.isdigit():
        parsed = datetime.strptime(text, "%Y%m%d")
        default_time = dt_time(15, 0) if is_end else dt_time(9, 30)
        return datetime.combine(parsed.date(), default_time)
    if len(text) == 10:
        parsed = datetime.fromisoformat(text)
        default_time = dt_time(15, 0) if is_end else dt_time(9, 30)
        return datetime.combine(parsed.date(), default_time)
    return pd.to_datetime(text).to_pydatetime()


def _estimate_tencent_count(start_dt: datetime, end_dt: datetime) -> int:
    day_span = max(1, (end_dt.date() - start_dt.date()).days + 1)
    return min(max(day_span * 48, 48), 800)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="双源分钟级 K 线抓取并落 Parquet（聚宽历史 + 腾讯近期）")
    parser.add_argument("codes", nargs="+", help="股票代码，例如 600000 000001")
    parser.add_argument("--period", default="5", choices=sorted(SUPPORTED_PERIODS), help="分钟周期")
    parser.add_argument("--start-date", help="开始时间，例如 2026-04-01 09:30:00")
    parser.add_argument("--end-date", help="结束时间，例如 2026-04-28 15:00:00")
    parser.add_argument("--no-merge", action="store_true", help="不合并旧 Parquet，直接覆盖")
    args = parser.parse_args(argv)

    try:
        result = batch_fetch_min_data(
            args.codes,
            period=args.period,
            start_date=args.start_date,
            end_date=args.end_date,
            merge_existing=not args.no_merge,
        )
    except RuntimeError as exc:
        raise SystemExit(f"[jq-min-kline][ERROR] {exc}") from exc
    print(result)


if __name__ == "__main__":
    main()
