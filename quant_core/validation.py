from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import DATA_DIR
from .market import fetch_sina_snapshot
from .storage import normalize_daily_frame, save_validation_report


REQUIRED_COLUMNS = {"code", "date", "open", "high", "low", "close"}


def _issue(level: str, stage: str, message: str, code: str | None = None, field: str | None = None, date: str | None = None, value: Any = None) -> dict[str, Any]:
    return {
        "level": level,
        "stage": stage,
        "code": code,
        "date": date,
        "field": field,
        "value": None if value is None or (isinstance(value, float) and np.isnan(value)) else value,
        "message": message,
    }


def validate_frame(raw_df: pd.DataFrame, source_name: str, code_hint: str | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    df = normalize_daily_frame(raw_df, source="validation")
    code = code_hint or (str(df["code"].dropna().iloc[0]) if not df.empty and df["code"].notna().any() else None)

    missing = REQUIRED_COLUMNS - set(df.columns)
    for col in sorted(missing):
        issues.append(_issue("error", "schema", f"缺少必要字段 {col}", code=code, field=col))

    if df.empty:
        issues.append(_issue("error", "schema", "文件为空或没有可解析的 code/date", code=code))
        return {"rows": 0, "min_date": None, "max_date": None}, issues

    duplicated = df.duplicated(subset=["code", "date"], keep=False)
    if duplicated.any():
        for _, row in df[duplicated].head(30).iterrows():
            issues.append(_issue("error", "schema", "同一股票同一日期存在重复记录", code=row["code"], date=row["date"]))

    for col in ["open", "high", "low", "close"]:
        bad = df[df[col].isna()]
        for _, row in bad.head(20).iterrows():
            issues.append(_issue("error", "schema", f"{col} 为空，无法用于训练或回测", code=row["code"], date=row["date"], field=col))

    price_bad = df[(df[["open", "high", "low", "close"]] <= 0).any(axis=1)]
    for _, row in price_bad.head(30).iterrows():
        issues.append(_issue("error", "logic", "OHLC 价格必须大于 0", code=row["code"], date=row["date"]))

    ohlc_bad = df[
        (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df["low"])
    ]
    for _, row in ohlc_bad.head(30).iterrows():
        issues.append(_issue("error", "logic", "OHLC 高低开收关系不成立", code=row["code"], date=row["date"]))

    volume_bad = df[((df["volume"].notna()) & (df["volume"] < 0)) | ((df["amount"].notna()) & (df["amount"] < 0))]
    for _, row in volume_bad.head(30).iterrows():
        issues.append(_issue("error", "logic", "成交量或成交额为负数", code=row["code"], date=row["date"]))

    pct_rows = df[(df["pre_close"] > 0) & df["change_pct"].notna() & df["close"].notna()]
    if not pct_rows.empty:
        expected = (pct_rows["close"] / pct_rows["pre_close"] - 1) * 100
        diff = (expected - pct_rows["change_pct"]).abs()
        mismatch = pct_rows[diff > 0.45]
        for idx, row in mismatch.head(30).iterrows():
            issues.append(
                _issue(
                    "warning",
                    "logic",
                    "涨跌幅与收盘价/昨收计算结果偏差超过 0.45%",
                    code=row["code"],
                    date=row["date"],
                    field="change_pct",
                    value=round(float(diff.loc[idx]), 4),
                )
            )

    dates = pd.to_datetime(df["date"], errors="coerce")
    summary = {
        "source": source_name,
        "rows": int(len(df)),
        "min_date": None if dates.isna().all() else dates.min().strftime("%Y-%m-%d"),
        "max_date": None if dates.isna().all() else dates.max().strftime("%Y-%m-%d"),
        "duplicate_rows": int(duplicated.sum()),
        "errors": sum(1 for item in issues if item["level"] == "error"),
        "warnings": sum(1 for item in issues if item["level"] == "warning"),
    }
    return summary, issues


def validate_repository(sample: int | None = 200, source_check: bool = False) -> dict[str, Any]:
    files = sorted(DATA_DIR.glob("*_daily.parquet"))
    selected = files if sample is None or sample <= 0 else files[:sample]
    all_issues: list[dict[str, Any]] = []
    totals = Counter()
    max_dates: list[str] = []
    failed_files: list[dict[str, str]] = []

    for path in selected:
        code_hint = path.name[:6]
        try:
            df = pd.read_parquet(path)
            summary, issues = validate_frame(df, str(path), code_hint=code_hint)
            totals["rows"] += int(summary["rows"])
            totals["errors"] += int(summary["errors"])
            totals["warnings"] += int(summary["warnings"])
            if summary["max_date"]:
                max_dates.append(summary["max_date"])
            all_issues.extend(issues)
        except Exception as exc:
            failed_files.append({"file": str(path), "error": str(exc)})
            all_issues.append(_issue("error", "schema", f"Parquet 文件读取失败: {exc}", code=code_hint))

    truth_summary = _run_source_truth_check(selected, all_issues) if source_check else {"checked": False}
    totals["errors"] += sum(1 for item in all_issues if item["level"] == "error") - totals["errors"]
    totals["warnings"] += sum(1 for item in all_issues if item["level"] == "warning") - totals["warnings"]

    status = "pass" if totals["errors"] == 0 and not failed_files else "fail"
    summary = {
        "files_total": len(files),
        "files_checked": len(selected),
        "rows_checked": int(totals["rows"]),
        "latest_date_seen": max(max_dates) if max_dates else None,
        "error_count": sum(1 for item in all_issues if item["level"] == "error"),
        "warning_count": sum(1 for item in all_issues if item["level"] == "warning"),
        "failed_files": failed_files[:30],
        "truth_check": truth_summary,
        "triple_validation": [
            "结构完整性：字段、日期、重复主键、空值",
            "金融逻辑正确性：OHLC、成交量、涨跌幅一致性",
            "来源真实性：可选实时行情源交叉核验最新快照",
        ],
    }
    report_id = save_validation_report("parquet_repository", status, summary, all_issues)
    return {"id": report_id, "status": status, "summary": summary, "issues": all_issues[:500]}


def validate_one_code(code: str, source_check: bool = False) -> dict[str, Any]:
    path = DATA_DIR / f"{code}_daily.parquet"
    if not path.exists():
        issues = [_issue("error", "schema", "找不到股票历史文件", code=code)]
        summary = {"files_checked": 0, "rows_checked": 0, "error_count": 1, "warning_count": 0}
        report_id = save_validation_report(f"code:{code}", "fail", summary, issues)
        return {"id": report_id, "status": "fail", "summary": summary, "issues": issues}
    df = pd.read_parquet(path)
    file_summary, issues = validate_frame(df, str(path), code_hint=code)
    if source_check:
        _run_source_truth_check([path], issues)
    status = "pass" if not any(item["level"] == "error" for item in issues) else "fail"
    summary = {
        "files_checked": 1,
        "rows_checked": file_summary["rows"],
        "min_date": file_summary["min_date"],
        "max_date": file_summary["max_date"],
        "error_count": sum(1 for item in issues if item["level"] == "error"),
        "warning_count": sum(1 for item in issues if item["level"] == "warning"),
    }
    report_id = save_validation_report(f"code:{code}", status, summary, issues)
    return {"id": report_id, "status": status, "summary": summary, "issues": issues[:500]}


def _run_source_truth_check(files: list[Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        snapshot = fetch_sina_snapshot()
    except Exception as exc:
        issues.append(_issue("warning", "truth", f"实时源交叉核验失败: {exc}"))
        return {"checked": False, "error": str(exc)}

    if snapshot.empty:
        issues.append(_issue("warning", "truth", "实时源返回空数据，无法核验真实性"))
        return {"checked": False, "error": "empty snapshot"}

    latest_by_code = snapshot.set_index("code").to_dict(orient="index")
    checked = 0
    mismatch = 0
    for path in files[:120]:
        code = path.name[:6]
        live = latest_by_code.get(code)
        if not live:
            continue
        checked += 1
        try:
            df = normalize_daily_frame(pd.read_parquet(path), source="truth")
            latest = df.sort_values("date").iloc[-1]
        except Exception:
            continue
        if latest["date"] == live.get("date") and abs(float(latest["close"]) - float(live["close"])) > 0.03:
            mismatch += 1
            issues.append(
                _issue(
                    "warning",
                    "truth",
                    "本地最新收盘价与实时源同日价格不一致",
                    code=code,
                    date=str(latest["date"]),
                    field="close",
                    value={"local": float(latest["close"]), "source": float(live["close"])},
                )
            )

    return {"checked": True, "sampled_codes": checked, "mismatch": mismatch, "source": "sina_hs_a"}
