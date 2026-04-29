from __future__ import annotations

import json
import re

import pandas as pd
import requests


def get_ashare_tencent_m5(symbol: str = "sh600000", count: int = 3000) -> pd.DataFrame:
    url = "http://ifzq.gtimg.cn/appstock/app/kline/mkline"
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        url,
        params={"param": f"{symbol},m5,,{count}"},
        timeout=15,
        proxies={},
    )
    response.raise_for_status()
    payload = response.json()
    rows = (((payload.get("data") or {}).get(symbol) or {}).get("m5") or [])
    parsed = []
    for row in rows:
        if len(row) < 6:
            continue
        parsed.append(
            {
                "datetime": pd.to_datetime(row[0], errors="coerce"),
                "open": pd.to_numeric(row[1], errors="coerce"),
                "close": pd.to_numeric(row[2], errors="coerce"),
                "high": pd.to_numeric(row[3], errors="coerce"),
                "low": pd.to_numeric(row[4], errors="coerce"),
                "volume": pd.to_numeric(row[5], errors="coerce"),
            }
        )
    df = pd.DataFrame(parsed)
    if df.empty:
        return df
    return df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)


def main() -> None:
    df = get_ashare_tencent_m5("sh600000", count=3000)
    if df.empty:
        print({"symbol": "sh600000", "count": 0, "earliest": None, "latest": None})
        return
    result = {
        "symbol": "sh600000",
        "requested_count": 3000,
        "row_count": int(len(df)),
        "earliest_datetime": df["datetime"].min().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_datetime": df["datetime"].max().strftime("%Y-%m-%d %H:%M:%S"),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
