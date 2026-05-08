from __future__ import annotations

import argparse
import io
import json
import math
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None


BASE_DIR = Path("/Users/eudis/ths")
DATASET_PATH = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
MODEL_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_1_candidate.json"
META_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v5_7_1_candidate.meta.json"
MINUTE_ROOT = Path("/Users/eudis/5min/organized_5min_pre_adj")
REPORT_PATH = BASE_DIR / "scripts" / "research" / "simulate_end_to_end_v5_7_latest.json"
DETAIL_PATH = BASE_DIR / "scripts" / "research" / "simulate_end_to_end_v5_7_trades.csv"

DEFAULT_START_DATE = "2025-11-03"
DEFAULT_END_DATE = "2026-01-28"
BASELINE_WIN_RATE = 67.15
BASELINE_MEAN_YIELD = 2.11

HARD_STOP_PCT = -0.04
TRAILING_ACTIVE_PCT = 1.04
TRAILING_RETRACEMENT_PCT = 0.02
EOD_STRUCTURAL_STOP_PCT = -0.015
EOD_STRUCTURAL_STOP_TIMES = {dtime(14, 50), dtime(14, 55)}
DEFAULT_BUY_TIME = dtime(14, 50)
CODE_RE = re.compile(r"(\d{6})")
ZIP_DATE_RE = re.compile(r"(\d{8})_5min\.zip$")


@dataclass(frozen=True)
class MockPick:
    date: str
    code: str
    name: str
    cost_price: float
    probability: float


@dataclass(frozen=True)
class TradeResult:
    date: str
    code: str
    name: str
    probability: float
    cost_price: float
    exit_reason: str
    exit_time: str
    exit_price: float
    yield_pct: float
    highest_gain_pct: float
    bars_replayed: int
    t3_date: str


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


def load_feature_columns(meta_path: Path) -> list[str]:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    cols = meta.get("feature_columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f"meta 缺少 feature_columns：{meta_path}")
    bad = [col for col in cols if str(col).startswith("market_") or col == "smart_money_ratio"]
    if bad:
        raise ValueError(f"V5.7.1 meta 仍包含已剔除特征：{bad}")
    return [str(col) for col in cols]


def read_validation_panel(
    dataset_path: Path,
    feature_columns: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    header = pd.read_csv(dataset_path, nrows=0).columns.tolist()
    base_cols = ["code", "name", "date", "entry_price", "buy5m_entry_price", "close"]
    optional_cols = ["rule_return_pct", "t3_close_return_pct_5m", "label_rule_win"]
    usecols = [col for col in [*base_cols, *optional_cols, *feature_columns] if col in header]
    frame = pd.read_csv(dataset_path, usecols=list(dict.fromkeys(usecols)), dtype={"code": "string"}, low_memory=False)
    for col in feature_columns:
        if col not in frame.columns:
            frame[col] = 0.0
    frame["code"] = frame["code"].map(normalize_code)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["code", "date"]).copy()
    frame = frame[(frame["date"] >= start_date) & (frame["date"] <= end_date)].copy()
    if frame.empty:
        raise SystemExit(f"验证区间 {start_date} -> {end_date} 在宽表中没有记录。")
    return frame.sort_values(["date", "code"]).reset_index(drop=True)


def align_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for col in feature_columns:
        out[col] = pd.to_numeric(frame.get(col, 0.0), errors="coerce")
    out = out.replace([np.inf, -np.inf], np.nan)
    medians = out.median(numeric_only=True).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out.fillna(medians).astype("float32", copy=False)


def load_model(model_path: Path) -> Any:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    model = XGBClassifier()
    model.load_model(str(model_path))
    return model


def score_panel(model: Any, panel: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = panel.copy()
    out["model_probability"] = model.predict_proba(align_features(out, feature_columns))[:, 1]
    return out


def generate_mock_picks(scored: pd.DataFrame, pick_mode: str, top_n: int, threshold: float) -> list[MockPick]:
    frame = scored.copy()
    if pick_mode == "threshold":
        selected = frame[frame["model_probability"] >= threshold].copy()
        selected = selected.sort_values(["date", "model_probability"], ascending=[True, False])
    elif pick_mode == "threshold_or_topn":
        selected = frame[frame["model_probability"] >= threshold].copy()
        if selected.empty:
            selected = (
                frame.sort_values(["date", "model_probability"], ascending=[True, False])
                .groupby("date", sort=False)
                .head(int(top_n))
                .copy()
            )
        else:
            selected = selected.sort_values(["date", "model_probability"], ascending=[True, False])
    elif pick_mode == "topn":
        selected = (
            frame.sort_values(["date", "model_probability"], ascending=[True, False])
            .groupby("date", sort=False)
            .head(int(top_n))
            .copy()
        )
    else:
        raise ValueError(f"unknown pick_mode: {pick_mode}")

    picks: list[MockPick] = []
    for row in selected.itertuples(index=False):
        cost = first_positive(
            getattr(row, "entry_price", None),
            getattr(row, "buy5m_entry_price", None),
            getattr(row, "close", None),
        )
        if cost is None or cost <= 0:
            continue
        picks.append(
            MockPick(
                date=str(row.date)[:10],
                code=normalize_code(getattr(row, "code", "")),
                name=str(getattr(row, "name", "") or getattr(row, "code", "")),
                cost_price=float(cost),
                probability=float(getattr(row, "model_probability")),
            )
        )
    return picks


def first_positive(*values: Any) -> Optional[float]:
    for value in values:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed) and parsed > 0:
            return parsed
    return None


def discover_zip_paths(minute_root: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for path in sorted((minute_root / "sh_sz").glob("*/*_5min.zip")):
        match = ZIP_DATE_RE.search(path.name)
        if not match:
            continue
        raw = match.group(1)
        out[f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"] = path
    return out


def symbol_for_code(code: str) -> str:
    clean = normalize_code(code)
    prefix = "sh" if clean.startswith(("5", "6", "9")) else "sz"
    return f"{prefix}{clean}"


def read_member_5m(zip_path: Path, code: str) -> pd.DataFrame:
    member = f"{symbol_for_code(code)}.csv"
    try:
        with zipfile.ZipFile(zip_path) as zf:
            if member not in zf.namelist():
                return pd.DataFrame()
            raw = zf.read(member)
    except Exception:
        return pd.DataFrame()
    try:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["datetime"] = pd.to_datetime(df.get("时间"), errors="coerce")
    out["open"] = pd.to_numeric(df.get("开盘价"), errors="coerce")
    out["high"] = pd.to_numeric(df.get("最高价"), errors="coerce")
    out["low"] = pd.to_numeric(df.get("最低价"), errors="coerce")
    out["close"] = pd.to_numeric(df.get("收盘价"), errors="coerce")
    out = out.dropna(subset=["datetime", "open", "high", "low", "close"]).sort_values("datetime")
    return out.reset_index(drop=True)


def t_plus_n_date(buy_date: str, trading_dates: list[str], n: int = 3) -> Optional[str]:
    future = [date for date in trading_dates if date > buy_date]
    return future[n - 1] if len(future) >= n else None


def load_pick_bars(pick: MockPick, zip_paths: dict[str, Path], trading_dates: list[str]) -> tuple[pd.DataFrame, str]:
    t3_date = t_plus_n_date(pick.date, trading_dates, n=3)
    if not t3_date:
        return pd.DataFrame(), ""
    selected_dates = [date for date in trading_dates if pick.date <= date <= t3_date]
    frames = []
    for trade_date in selected_dates:
        path = zip_paths.get(trade_date)
        if path is None:
            continue
        frame = read_member_5m(path, pick.code)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(), t3_date
    bars = pd.concat(frames, ignore_index=True).sort_values("datetime").reset_index(drop=True)
    buy_ts = pd.Timestamp.combine(pd.Timestamp(pick.date).date(), DEFAULT_BUY_TIME)
    end_ts = pd.Timestamp(f"{t3_date} 15:00:00")
    bars = bars[(bars["datetime"] > buy_ts) & (bars["datetime"] <= end_ts)].copy().reset_index(drop=True)
    return bars, t3_date


def simulate_pick(pick: MockPick, zip_paths: dict[str, Path], trading_dates: list[str]) -> Optional[TradeResult]:
    bars, t3_date = load_pick_bars(pick, zip_paths, trading_dates)
    if bars.empty or not t3_date:
        return None

    cost = float(pick.cost_price)
    hard_stop_price = cost * (1.0 + HARD_STOP_PCT)
    eod_stop_price = cost * (1.0 + EOD_STRUCTURAL_STOP_PCT)
    highest_price = cost
    trailing_active = False

    t3_rows = bars[bars["datetime"].dt.strftime("%Y-%m-%d") == t3_date]
    if t3_rows.empty:
        return None
    t3_last_idx = int(t3_rows.index[-1])

    for idx, bar in bars.iterrows():
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        ts = pd.Timestamp(bar["datetime"])
        highest_price = max(highest_price, high)

        if low <= hard_stop_price:
            return build_result(pick, "盘中暴雷止损_敢死队_4pct", ts, hard_stop_price, highest_price, idx + 1, t3_date)

        if highest_price >= cost * TRAILING_ACTIVE_PCT:
            trailing_active = True

        trailing_trigger = highest_price * (1.0 - TRAILING_RETRACEMENT_PCT)
        if trailing_active and low <= trailing_trigger:
            exit_price = min(trailing_trigger, close)
            return build_result(pick, "动态追踪止盈_4pct引信_2pct回撤", ts, exit_price, highest_price, idx + 1, t3_date)

        if ts.time() in EOD_STRUCTURAL_STOP_TIMES and close <= eod_stop_price:
            return build_result(pick, "尾盘破位卖出_敢死队_1_5pct", ts, close, highest_price, idx + 1, t3_date)

        if idx == t3_last_idx:
            return build_result(pick, "T+3强制平仓", ts, close, highest_price, idx + 1, t3_date)

    return None


def build_result(
    pick: MockPick,
    reason: str,
    exit_ts: pd.Timestamp,
    exit_price: float,
    highest_price: float,
    bars_replayed: int,
    t3_date: str,
) -> TradeResult:
    yield_pct = (float(exit_price) / pick.cost_price - 1.0) * 100.0
    highest_gain_pct = (float(highest_price) / pick.cost_price - 1.0) * 100.0
    return TradeResult(
        date=pick.date,
        code=pick.code,
        name=pick.name,
        probability=round(float(pick.probability), 8),
        cost_price=round(float(pick.cost_price), 4),
        exit_reason=reason,
        exit_time=exit_ts.strftime("%Y-%m-%d %H:%M:%S"),
        exit_price=round(float(exit_price), 4),
        yield_pct=round(float(yield_pct), 4),
        highest_gain_pct=round(float(highest_gain_pct), 4),
        bars_replayed=int(bars_replayed),
        t3_date=t3_date,
    )


def summarize_results(results: list[TradeResult]) -> dict[str, Any]:
    if not results:
        return {
            "trades": 0,
            "win_rate_pct": 0.0,
            "mean_yield_pct": 0.0,
            "median_yield_pct": 0.0,
            "best_yield_pct": None,
            "worst_yield_pct": None,
            "reason_counts": {},
        }
    values = pd.Series([item.yield_pct for item in results], dtype="float64")
    reasons = pd.Series([item.exit_reason for item in results]).value_counts().to_dict()
    return {
        "trades": int(len(values)),
        "win_rate_pct": round(float((values > 0).mean() * 100.0), 4),
        "mean_yield_pct": round(float(values.mean()), 4),
        "median_yield_pct": round(float(values.median()), 4),
        "best_yield_pct": round(float(values.max()), 4),
        "worst_yield_pct": round(float(values.min()), 4),
        "reason_counts": {str(k): int(v) for k, v in reasons.items()},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end V5.7.1 candidate picks + V5.6 Sentinel replay.")
    parser.add_argument("--dataset", default=str(DATASET_PATH))
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    parser.add_argument("--minute-root", default=str(MINUTE_ROOT))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--pick-mode", choices=["topn", "threshold", "threshold_or_topn"], default="topn")
    parser.add_argument("--top-n", type=int, default=1)
    parser.add_argument("--threshold", type=float, default=0.6)
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--detail-path", default=str(DETAIL_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    feature_columns = load_feature_columns(Path(args.meta_path))
    panel = read_validation_panel(Path(args.dataset), feature_columns, args.start_date, args.end_date)
    model = load_model(Path(args.model_path))
    scored = score_panel(model, panel, feature_columns)
    threshold_count = int((scored["model_probability"] >= float(args.threshold)).sum())
    threshold_dates = int(scored.loc[scored["model_probability"] >= float(args.threshold), "date"].nunique())
    picks = generate_mock_picks(scored, args.pick_mode, int(args.top_n), float(args.threshold))

    zip_paths = discover_zip_paths(Path(args.minute_root))
    trading_dates = sorted(date for date in zip_paths if date >= str(scored["date"].min()))
    results = []
    missing = 0
    for pick in picks:
        result = simulate_pick(pick, zip_paths, trading_dates)
        if result is None:
            missing += 1
        else:
            results.append(result)

    summary = summarize_results(results)
    result_rows = [item.__dict__ for item in results]
    detail_path = Path(args.detail_path)
    report_path = Path(args.report_path)
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(result_rows).to_csv(detail_path, index=False)

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "scripts/research/simulate_end_to_end_v5_7.py",
        "dataset": str(Path(args.dataset)),
        "model_path": str(Path(args.model_path)),
        "meta_path": str(Path(args.meta_path)),
        "minute_root": str(Path(args.minute_root)),
        "requested_date_range": {"start": args.start_date, "end": args.end_date},
        "effective_date_range": {"start": str(scored["date"].min()), "end": str(scored["date"].max())},
        "panel_rows": int(len(scored)),
        "panel_dates": int(scored["date"].nunique()),
        "pick_mode": args.pick_mode,
        "top_n": int(args.top_n),
        "threshold": float(args.threshold),
        "threshold_count": threshold_count,
        "threshold_dates": threshold_dates,
        "mock_pick_count": int(len(picks)),
        "settled_count": int(len(results)),
        "missing_count": int(missing),
        "sentinel_rule": {
            "hard_stop_pct": HARD_STOP_PCT,
            "trailing_active_pct": TRAILING_ACTIVE_PCT,
            "trailing_retracement_pct": TRAILING_RETRACEMENT_PCT,
            "eod_structural_stop_pct": EOD_STRUCTURAL_STOP_PCT,
            "forced_exit": "T+3 last 5m bar",
        },
        "baseline": {
            "win_rate_pct": BASELINE_WIN_RATE,
            "mean_yield_pct": BASELINE_MEAN_YIELD,
        },
        "v5_7": summary,
        "detail_path": str(detail_path),
        "elapsed_seconds": round(float(time.perf_counter() - started), 3),
    }
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n========== V5.7.1 End-to-End Sentinel Replay ==========")
    print(f"Requested Range : {args.start_date} -> {args.end_date}")
    print(f"Effective Range : {scored['date'].min()} -> {scored['date'].max()} ({scored['date'].nunique()} dates)")
    print(f"Panel Rows      : {len(scored)}")
    print(f"Pick Mode       : {args.pick_mode} top_n={int(args.top_n)} threshold={float(args.threshold):.4f}")
    print(f"p>=threshold    : {threshold_count} rows / {threshold_dates} dates")
    print(f"Mock Picks      : {len(picks)}")
    print(f"Settled Trades  : {len(results)}")
    print(f"Missing Trades  : {missing}")
    print("\n---------- Final P&L ----------")
    print(f"旧版基准: 胜率 {BASELINE_WIN_RATE:.2f}%, Mean Yield {BASELINE_MEAN_YIELD:.2f}%")
    print(f"V5.7新版: 胜率 {summary['win_rate_pct']:.2f}%, Mean Yield {summary['mean_yield_pct']:.2f}%")
    print(f"V5.7中位收益: {summary['median_yield_pct']:.2f}%")
    print(f"V5.7最佳/最差: {summary['best_yield_pct']}% / {summary['worst_yield_pct']}%")
    print("\nExit Reasons:")
    for reason, count in summary["reason_counts"].items():
        print(f"  {reason}: {count}")
    print(f"\nReport Path     : {report_path}")
    print(f"Trade Detail    : {detail_path}")
    print(f"Elapsed Seconds : {payload['elapsed_seconds']:.3f}")
    print("=======================================================\n")


if __name__ == "__main__":
    main()
