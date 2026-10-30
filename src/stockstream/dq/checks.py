"""Daily data-quality checks. Results land in gold/dq_results for Athena and the dashboard.

Each check returns rows of {check, status, detail}; status is PASS, WARN, or FAIL.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from stockstream.common.config import load_universe
from stockstream.common.markets import NY, is_trading_day
from stockstream.common.storage import (
    delete_prefix,
    list_keys,
    put_parquet,
    read_jsonl_gz,
    read_parquet_prefix,
)


def bronze_vs_silver(bronze: pd.DataFrame, silver: pd.DataFrame, quarantined: int, tolerance: float = 0.01) -> dict:
    """Every bronze record should end up in silver or quarantine (after dedupe)."""
    b = bronze["event_id"].nunique() if not bronze.empty else 0
    s = silver["event_id"].nunique() if not silver.empty else 0
    accounted = s + quarantined
    gap = abs(b - accounted) / b if b else 0.0
    status = "PASS" if gap <= tolerance else "FAIL"
    return {"check": "bronze_vs_silver", "status": status,
            "detail": f"bronze={b} silver={s} quarantined={quarantined} gap={gap:.4f}"}


def missing_equity_hours(silver: pd.DataFrame, day: date) -> dict:
    """Every equity symbol should trade in every full regular-session hour (10:00 to 15:00 ET)."""
    if not is_trading_day(day):
        return {"check": "missing_equity_hours", "status": "PASS", "detail": "not a trading day"}
    universe = load_universe()
    expected_hours = [datetime(day.year, day.month, day.day, h, tzinfo=NY) for h in range(10, 16)]
    eq = silver[silver["asset_class"] == "equity"] if not silver.empty else silver
    have = set()
    if not eq.empty:
        hours = pd.to_datetime(eq["event_ts"], unit="ms", utc=True).dt.tz_convert(NY).dt.floor("h")
        have = set(zip(eq["symbol"], hours))
    missing = [f"{s}@{h.strftime('%H:%M')}" for s in universe.equities for h in expected_hours
               if (s, pd.Timestamp(h)) not in have]
    status = "PASS" if not missing else "WARN"
    return {"check": "missing_equity_hours", "status": status,
            "detail": f"{len(missing)} missing" + (": " + ", ".join(missing[:20]) if missing else "")}


def ohlc_sanity(hourly: pd.DataFrame) -> dict:
    if hourly.empty:
        return {"check": "ohlc_sanity", "status": "WARN", "detail": "no hourly rows"}
    bad = hourly[(hourly["low"] > hourly[["open", "close"]].min(axis=1)) |
                 (hourly["high"] < hourly[["open", "close"]].max(axis=1)) |
                 (hourly["volume"] <= 0)]
    return {"check": "ohlc_sanity", "status": "PASS" if bad.empty else "FAIL",
            "detail": f"{len(bad)} bad rows of {len(hourly)}"}


def run_dq(s3, bucket: str, day: date) -> pd.DataFrame:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    bronze_rows, quarantined = [], 0
    for h in range(0, 30):
        ts = start + timedelta(hours=h)
        dt, hour = ts.strftime("%Y-%m-%d"), ts.strftime("%H")
        for key in list_keys(s3, bucket, f"bronze/dt={dt}/hour={hour}/"):
            bronze_rows.extend(read_jsonl_gz(s3, bucket, key))
        for key in list_keys(s3, bucket, f"quarantine/dt={dt}/hour={hour}/"):
            quarantined += len(read_jsonl_gz(s3, bucket, key))
    bronze = pd.DataFrame(bronze_rows)
    silver = pd.concat([read_parquet_prefix(s3, bucket, f"silver/trades/dt={day.isoformat()}/")], ignore_index=True)
    hourly = read_parquet_prefix(s3, bucket, f"gold/hourly_stats/dt={day.isoformat()}/")

    results = pd.DataFrame([
        bronze_vs_silver(bronze, silver, quarantined),
        missing_equity_hours(silver, day),
        ohlc_sanity(hourly),
    ])
    results["run_date"] = day.isoformat()
    prefix = f"gold/dq_results/dt={day.isoformat()}/"
    delete_prefix(s3, bucket, prefix)
    put_parquet(s3, bucket, prefix + "part-0.parquet", results)
    return results
