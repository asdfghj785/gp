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
from sklearn.metrics import average_precision_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None


BASE_DIR = Path("/Users/eudis/ths")
DATASET_PATH = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
MINUTE_ROOT = Path("/Users/eudis/5min/organized_5min_pre_adj")
MODEL_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v6_0_extreme_burst_candidate.json"
META_PATH = BASE_DIR / "models" / "experiments" / "xgboost_global_sniper_v6_0_extreme_burst_candidate.meta.json"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_latest.json"
TRADE_DETAIL_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_trades.csv"
PICK_DETAIL_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_picks.csv"

EXTREME_RETURN_THRESHOLD = 0.06
N_SPLITS = 5
PURGE_DAYS = 3
CODE_RE = re.compile(r"(\d{6})")
ZIP_DATE_RE = re.compile(r"(\d{8})_5min\.zip$")

HARD_STOP_PCT = -0.04
TRAILING_ACTIVE_PCT = 1.04
TRAILING_RETRACEMENT_PCT = 0.02
EOD_STRUCTURAL_STOP_PCT = -0.015
EOD_STRUCTURAL_STOP_TIMES = {dtime(14, 50), dtime(14, 55)}
DEFAULT_BUY_TIME = dtime(14, 50)

IGNITION_FEATURES = ["vol_surge_ratio", "price_compression", "close_to_high_proximity"]
REMOVED_FEATURES = {"smart_money_ratio"}
LEAK_OR_LABEL_COLS = {
    "future_days",
    "rule_exit_price",
    "rule_return_pct",
    "rule_highest_gain_pct",
    "t3_close_price_5m",
    "t3_close_return_pct_5m",
    "oracle_max_gain_pct_5m",
    "label_rule_win",
    "label_t3_win",
    "future_max_high_t1_t3",
    "future_max_return_3d",
    "label_extreme_burst",
}
STRING_SKIP_COLS = {"rule_exit_action", "rule_exit_checked_at"}


@dataclass(frozen=True)
class MockPick:
    date: str
    code: str
    name: str
    cost_price: float
    probability: float
    label_extreme_burst: int
    future_max_return_3d: float


@dataclass(frozen=True)
class TradeResult:
    date: str
    code: str
    name: str
    probability: float
    label_extreme_burst: int
    future_max_return_3d: float
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


def read_dataset(path: Path) -> pd.DataFrame:
    header = pd.read_csv(path, nrows=0).columns.tolist()
    usecols = [col for col in header if col not in STRING_SKIP_COLS]
    df = pd.read_csv(path, usecols=usecols, dtype={"code": "string"}, low_memory=False)
    df["code"] = df["code"].map(normalize_code)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["code", "date", "high", "low", "close"]).copy()
    for col in ("high", "low", "close", "volume", "entry_price", "buy5m_entry_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["code", "date"]).reset_index(drop=True)


def add_extreme_label(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby("code", sort=False)
    out["future_high_1"] = grouped["high"].shift(-1)
    out["future_high_2"] = grouped["high"].shift(-2)
    out["future_high_3"] = grouped["high"].shift(-3)
    out["future_max_high_t1_t3"] = out[["future_high_1", "future_high_2", "future_high_3"]].max(axis=1)
    close = pd.to_numeric(out["close"], errors="coerce").replace(0.0, np.nan)
    out["future_max_return_3d"] = out["future_max_high_t1_t3"] / close - 1.0
    out["label_extreme_burst"] = (out["future_max_return_3d"] >= EXTREME_RETURN_THRESHOLD).astype("int8")
    out = out.dropna(subset=["future_high_1", "future_high_2", "future_high_3", "future_max_return_3d"]).copy()
    return out.drop(columns=["future_high_1", "future_high_2", "future_high_3"], errors="ignore")


def add_ignition_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby("code", sort=False)
    volume = pd.to_numeric(out["volume"], errors="coerce")
    prior_volume_ma5 = grouped["volume"].transform(lambda s: pd.to_numeric(s, errors="coerce").shift(1).rolling(5, min_periods=3).mean())
    out["vol_surge_ratio"] = volume / prior_volume_ma5.replace(0.0, np.nan)

    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    prev_close = grouped["close"].shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["_true_range"] = tr
    atr10 = out.groupby("code", sort=False)["_true_range"].transform(lambda s: s.rolling(10, min_periods=5).mean())
    out["price_compression"] = atr10 / close.replace(0.0, np.nan)
    spread = (high - low).replace(0.0, np.nan)
    out["close_to_high_proximity"] = ((high - close) / spread).clip(lower=0.0, upper=1.0)
    out["close_to_high_proximity"] = out["close_to_high_proximity"].fillna(0.0)
    return out.drop(columns=["_true_range"], errors="ignore")


def infer_feature_columns(df: pd.DataFrame) -> list[str]:
    exclude = {"code", "name", "date", *LEAK_OR_LABEL_COLS, *REMOVED_FEATURES}
    cols: list[str] = []
    for col in df.columns:
        if col in exclude or col.startswith("market_"):
            continue
        if col.startswith("future_") or col.startswith("label_") or col.startswith("rule_") or col.startswith("t3_") or col.startswith("oracle_"):
            continue
        if col in STRING_SKIP_COLS:
            continue
        if col in df.select_dtypes(include=[np.number]).columns:
            cols.append(col)
    for col in IGNITION_FEATURES:
        if col not in cols:
            cols.append(col)
    return list(dict.fromkeys(cols))


def align_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for col in feature_columns:
        out[col] = pd.to_numeric(frame.get(col, 0.0), errors="coerce")
    out = out.replace([np.inf, -np.inf], np.nan)
    medians = out.median(numeric_only=True).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out.fillna(medians).astype("float32", copy=False)


def build_model(y_train: pd.Series, args: argparse.Namespace) -> Any:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    return XGBClassifier(
        objective="binary:logistic",
        max_depth=int(args.max_depth),
        learning_rate=float(args.learning_rate),
        n_estimators=int(args.n_estimators),
        min_child_weight=float(args.min_child_weight),
        subsample=float(args.subsample),
        colsample_bytree=float(args.colsample_bytree),
        reg_lambda=float(args.reg_lambda),
        scale_pos_weight=neg / max(1, pos),
        eval_metric="logloss",
        early_stopping_rounds=int(args.early_stopping_rounds),
        tree_method="hist",
        random_state=int(args.random_state),
        n_jobs=-1,
    )


def train_oof(df: pd.DataFrame, feature_columns: list[str], args: argparse.Namespace) -> tuple[list[dict[str, Any]], pd.DataFrame, Any]:
    dates = np.asarray(sorted(df["date"].astype(str).unique().tolist()))
    splitter = TimeSeriesSplit(n_splits=int(args.n_splits), gap=int(args.purge_days))
    folds: list[dict[str, Any]] = []
    oof_parts: list[pd.DataFrame] = []
    last_model = None
    for fold_id, (train_idx, test_idx) in enumerate(splitter.split(dates), start=1):
        train_dates = set(dates[train_idx].tolist())
        test_dates = set(dates[test_idx].tolist())
        train = df[df["date"].isin(train_dates)].copy()
        test = df[df["date"].isin(test_dates)].copy()
        y_train = train["label_extreme_burst"].astype(int)
        y_test = test["label_extreme_burst"].astype(int)
        model = build_model(y_train, args)
        model.fit(align_features(train, feature_columns), y_train, eval_set=[(align_features(test, feature_columns), y_test)], verbose=False)
        proba = model.predict_proba(align_features(test, feature_columns))[:, 1]
        pred = proba >= 0.5
        fold = {
            "fold": int(fold_id),
            "train_start": str(dates[train_idx[0]]),
            "train_end": str(dates[train_idx[-1]]),
            "test_start": str(dates[test_idx[0]]),
            "test_end": str(dates[test_idx[-1]]),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_positive_rate": float(y_train.mean()),
            "test_positive_rate": float(y_test.mean()),
            "scale_pos_weight": float((y_train == 0).sum() / max(1, (y_train == 1).sum())),
            "auc": float(roc_auc_score(y_test, proba)),
            "average_precision": float(average_precision_score(y_test, proba)),
            "precision_at_05": float(precision_score(y_test, pred, zero_division=0)),
            "recall_at_05": float(recall_score(y_test, pred, zero_division=0)),
            "best_iteration": int(getattr(model, "best_iteration", -1)),
        }
        folds.append(fold)
        scored = test[["code", "name", "date", "close", "entry_price", "buy5m_entry_price", "label_extreme_burst", "future_max_return_3d"]].copy()
        scored["probability"] = proba
        oof_parts.append(scored)
        last_model = model
        print(
            f"[Fold {fold_id}] {fold['test_start']}->{fold['test_end']} "
            f"AUC={fold['auc']:.6f} AP={fold['average_precision']:.6f} "
            f"pos={fold['test_positive_rate']:.4f} spw={fold['scale_pos_weight']:.2f}",
            flush=True,
        )
    if last_model is None:
        raise RuntimeError("No model trained")
    return folds, pd.concat(oof_parts, ignore_index=True), last_model


def select_mock_picks(scored: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    high_conf = scored[scored["probability"] >= float(args.threshold_all)].copy()
    top1 = (
        scored.sort_values(["date", "probability", "future_max_return_3d"], ascending=[True, False, False])
        .groupby("date", sort=False)
        .head(1)
        .copy()
    )
    top1 = top1[top1["probability"] >= float(args.threshold_top1)].copy()
    if args.selection_mode == "p80":
        selected = high_conf
    elif args.selection_mode == "union":
        selected = pd.concat([high_conf, top1], ignore_index=True, sort=False)
    else:
        selected = top1
    selected = selected.drop_duplicates(["date", "code"], keep="first")
    selected = selected.sort_values(["date", "probability"], ascending=[True, False]).reset_index(drop=True)
    return selected


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
        raw_df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if raw_df.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["datetime"] = pd.to_datetime(raw_df.get("时间"), errors="coerce")
    out["open"] = pd.to_numeric(raw_df.get("开盘价"), errors="coerce")
    out["high"] = pd.to_numeric(raw_df.get("最高价"), errors="coerce")
    out["low"] = pd.to_numeric(raw_df.get("最低价"), errors="coerce")
    out["close"] = pd.to_numeric(raw_df.get("收盘价"), errors="coerce")
    return out.dropna(subset=["datetime", "open", "high", "low", "close"]).sort_values("datetime").reset_index(drop=True)


def t_plus_n_date(buy_date: str, trading_dates: list[str], n: int = 3) -> Optional[str]:
    future = [date for date in trading_dates if date > buy_date]
    return future[n - 1] if len(future) >= n else None


def load_pick_bars(pick: MockPick, zip_paths: dict[str, Path], trading_dates: list[str]) -> tuple[pd.DataFrame, str]:
    t3_date = t_plus_n_date(pick.date, trading_dates, n=3)
    if not t3_date:
        return pd.DataFrame(), ""
    frames = []
    for trade_date in [date for date in trading_dates if pick.date <= date <= t3_date]:
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
    bars = bars[(bars["datetime"] > buy_ts) & (bars["datetime"] <= pd.Timestamp(f"{t3_date} 15:00:00"))].copy()
    return bars.reset_index(drop=True), t3_date


def row_to_pick(row: Any) -> Optional[MockPick]:
    cost = first_positive(getattr(row, "entry_price", None), getattr(row, "buy5m_entry_price", None), getattr(row, "close", None))
    if cost is None:
        return None
    return MockPick(
        date=str(row.date)[:10],
        code=normalize_code(getattr(row, "code", "")),
        name=str(getattr(row, "name", "") or getattr(row, "code", "")),
        cost_price=float(cost),
        probability=float(row.probability),
        label_extreme_burst=int(row.label_extreme_burst),
        future_max_return_3d=float(row.future_max_return_3d),
    )


def first_positive(*values: Any) -> Optional[float]:
    for value in values:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed) and parsed > 0:
            return parsed
    return None


def simulate_pick(pick: MockPick, zip_paths: dict[str, Path], trading_dates: list[str]) -> Optional[TradeResult]:
    bars, t3_date = load_pick_bars(pick, zip_paths, trading_dates)
    if bars.empty or not t3_date:
        return None
    t3_rows = bars[bars["datetime"].dt.strftime("%Y-%m-%d") == t3_date]
    if t3_rows.empty:
        return None
    t3_last_idx = int(t3_rows.index[-1])
    cost = float(pick.cost_price)
    hard_stop_price = cost * (1.0 + HARD_STOP_PCT)
    eod_stop_price = cost * (1.0 + EOD_STRUCTURAL_STOP_PCT)
    highest_price = cost
    trailing_active = False
    for idx, bar in bars.iterrows():
        ts = pd.Timestamp(bar["datetime"])
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        highest_price = max(highest_price, high)
        if low <= hard_stop_price:
            return build_trade_result(pick, "盘中暴雷止损_敢死队_4pct", ts, hard_stop_price, highest_price, idx + 1, t3_date)
        if highest_price >= cost * TRAILING_ACTIVE_PCT:
            trailing_active = True
        trailing_trigger = highest_price * (1.0 - TRAILING_RETRACEMENT_PCT)
        if trailing_active and low <= trailing_trigger:
            return build_trade_result(pick, "动态追踪止盈_4pct引信_2pct回撤", ts, min(trailing_trigger, close), highest_price, idx + 1, t3_date)
        if ts.time() in EOD_STRUCTURAL_STOP_TIMES and close <= eod_stop_price:
            return build_trade_result(pick, "尾盘破位卖出_敢死队_1_5pct", ts, close, highest_price, idx + 1, t3_date)
        if idx == t3_last_idx:
            return build_trade_result(pick, "T+3强制平仓", ts, close, highest_price, idx + 1, t3_date)
    return None


def build_trade_result(
    pick: MockPick,
    reason: str,
    exit_ts: pd.Timestamp,
    exit_price: float,
    highest_price: float,
    bars_replayed: int,
    t3_date: str,
) -> TradeResult:
    return_pct = (float(exit_price) / pick.cost_price - 1.0) * 100.0
    highest_gain_pct = (float(highest_price) / pick.cost_price - 1.0) * 100.0
    return TradeResult(
        date=pick.date,
        code=pick.code,
        name=pick.name,
        probability=round(float(pick.probability), 8),
        label_extreme_burst=pick.label_extreme_burst,
        future_max_return_3d=round(float(pick.future_max_return_3d) * 100.0, 4),
        cost_price=round(pick.cost_price, 4),
        exit_reason=reason,
        exit_time=exit_ts.strftime("%Y-%m-%d %H:%M:%S"),
        exit_price=round(float(exit_price), 4),
        yield_pct=round(float(return_pct), 4),
        highest_gain_pct=round(float(highest_gain_pct), 4),
        bars_replayed=int(bars_replayed),
        t3_date=t3_date,
    )


def summarize_trades(results: list[TradeResult]) -> dict[str, Any]:
    if not results:
        return {"trades": 0, "win_rate_pct": 0.0, "mean_yield_pct": 0.0, "median_yield_pct": 0.0, "reason_counts": {}}
    values = pd.Series([item.yield_pct for item in results], dtype="float64")
    reasons = pd.Series([item.exit_reason for item in results]).value_counts().to_dict()
    return {
        "trades": int(len(values)),
        "win_rate_pct": round(float((values > 0).mean() * 100.0), 4),
        "mean_yield_pct": round(float(values.mean()), 4),
        "median_yield_pct": round(float(values.median()), 4),
        "best_yield_pct": round(float(values.max()), 4),
        "worst_yield_pct": round(float(values.min()), 4),
        "extreme_label_rate_pct": round(float(np.mean([item.label_extreme_burst for item in results]) * 100.0), 4),
        "reason_counts": {str(k): int(v) for k, v in reasons.items()},
    }


def feature_importance(model: Any, feature_columns: list[str], topn: int = 30) -> list[dict[str, Any]]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return []
    pairs = sorted(zip(feature_columns, [float(value) for value in values]), key=lambda item: item[1], reverse=True)
    return [{"rank": idx + 1, "feature": name, "importance": value} for idx, (name, value) in enumerate(pairs[:topn])]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild V6.0 global sniper with extreme burst label and ignition features.")
    parser.add_argument("--dataset", default=str(DATASET_PATH))
    parser.add_argument("--minute-root", default=str(MINUTE_ROOT))
    parser.add_argument("--model-path", default=str(MODEL_PATH))
    parser.add_argument("--meta-path", default=str(META_PATH))
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--trade-detail-path", default=str(TRADE_DETAIL_PATH))
    parser.add_argument("--pick-detail-path", default=str(PICK_DETAIL_PATH))
    parser.add_argument("--n-splits", type=int, default=N_SPLITS)
    parser.add_argument("--purge-days", type=int, default=PURGE_DAYS)
    parser.add_argument("--threshold-all", type=float, default=0.8)
    parser.add_argument("--threshold-top1", type=float, default=0.6)
    parser.add_argument("--selection-mode", choices=["top1_p60", "p80", "union"], default="top1_p60")
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.02)
    parser.add_argument("--n-estimators", type=int, default=700)
    parser.add_argument("--early-stopping-rounds", type=int, default=50)
    parser.add_argument("--min-child-weight", type=float, default=8.0)
    parser.add_argument("--subsample", type=float, default=0.85)
    parser.add_argument("--colsample-bytree", type=float, default=0.85)
    parser.add_argument("--reg-lambda", type=float, default=5.0)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    print("========== V6.0 Extreme Burst Global Sniper ==========")
    print(f"Dataset       : {args.dataset}")
    print(f"Label Rule    : future 3d max high / close - 1 >= {EXTREME_RETURN_THRESHOLD:.2%}")
    print(
        f"Selection     : mode={args.selection_mode}; "
        f"p80={args.threshold_all:.2f}; top1={args.threshold_top1:.2f}"
    )

    df = read_dataset(Path(args.dataset))
    df = add_extreme_label(df)
    df = add_ignition_features(df)
    feature_columns = infer_feature_columns(df)
    print(f"Rows          : {len(df)}")
    print(f"Dates         : {df['date'].min()} -> {df['date'].max()} ({df['date'].nunique()} days)")
    print(f"Positive Rate : {df['label_extreme_burst'].mean() * 100.0:.4f}%")
    print(f"Features      : {len(feature_columns)} including {IGNITION_FEATURES}")

    folds, oof, last_model = train_oof(df, feature_columns, args)
    selected = select_mock_picks(oof, args)
    selected.to_csv(args.pick_detail_path, index=False)
    print(f"OOF Rows      : {len(oof)}")
    print(f"P>=0.8 Rows   : {int((oof['probability'] >= args.threshold_all).sum())}")
    print(f"Selected Picks: {len(selected)}")

    zip_paths = discover_zip_paths(Path(args.minute_root))
    trading_dates = sorted(zip_paths)
    results: list[TradeResult] = []
    missing = 0
    for row in selected.itertuples(index=False):
        pick = row_to_pick(row)
        if pick is None:
            missing += 1
            continue
        result = simulate_pick(pick, zip_paths, trading_dates)
        if result is None:
            missing += 1
        else:
            results.append(result)

    summary = summarize_trades(results)
    trade_rows = [item.__dict__ for item in results]
    pd.DataFrame(trade_rows).to_csv(args.trade_detail_path, index=False)

    fi = feature_importance(last_model, feature_columns, topn=30)
    cv_summary = {
        "mean_auc": float(np.mean([fold["auc"] for fold in folds])),
        "mean_average_precision": float(np.mean([fold["average_precision"] for fold in folds])),
        "mean_precision_at_05": float(np.mean([fold["precision_at_05"] for fold in folds])),
        "mean_recall_at_05": float(np.mean([fold["recall_at_05"] for fold in folds])),
    }

    Path(args.model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.meta_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_path).parent.mkdir(parents=True, exist_ok=True)
    last_model.save_model(args.model_path)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "scripts/research/rebuild_v6_global_sniper.py",
        "dataset": args.dataset,
        "model_path": args.model_path,
        "label_rule": {
            "name": "label_extreme_burst",
            "future_horizon_days": 3,
            "threshold": EXTREME_RETURN_THRESHOLD,
        },
        "rows": int(len(df)),
        "date_range": {"start": str(df["date"].min()), "end": str(df["date"].max())},
        "positive_rate": float(df["label_extreme_burst"].mean()),
        "feature_columns": feature_columns,
        "ignition_features": IGNITION_FEATURES,
        "cv_summary": cv_summary,
        "folds": folds,
        "selection": {
            "mode": args.selection_mode,
            "threshold_all": float(args.threshold_all),
            "threshold_top1": float(args.threshold_top1),
            "oof_rows": int(len(oof)),
            "p80_rows": int((oof["probability"] >= args.threshold_all).sum()),
            "selected_picks": int(len(selected)),
            "missing": int(missing),
        },
        "sentinel_summary": summary,
        "feature_importance_top30": fi,
        "pick_detail_path": args.pick_detail_path,
        "trade_detail_path": args.trade_detail_path,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    Path(args.meta_path).write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.report_path).write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n---------- CV Summary ----------")
    print(f"Mean AUC       : {cv_summary['mean_auc']:.6f}")
    print(f"Mean AP        : {cv_summary['mean_average_precision']:.6f}")
    print(f"Mean Precision : {cv_summary['mean_precision_at_05']:.6f}")
    print("\nTop 30 Feature Importance (Last Fold):")
    for row in fi:
        print(f"  #{row['rank']:02d} {row['feature']}: {row['importance']:.8f}")
    print("\n---------- V6.0 + V5.6 Sentinel Report ----------")
    print(f"虚拟出票次数     : {len(selected)}")
    print(f"可结算交易       : {len(results)}")
    print(f"胜率             : {summary['win_rate_pct']:.2f}%")
    print(f"Mean Yield       : {summary['mean_yield_pct']:.4f}%")
    print(f"Median Yield     : {summary['median_yield_pct']:.4f}%")
    print(f"Extreme命中率    : {summary.get('extreme_label_rate_pct', 0.0):.2f}%")
    print(f"卖出原因         : {summary['reason_counts']}")
    print(f"Report Path      : {args.report_path}")
    print("==================================================\n")


if __name__ == "__main__":
    main()
