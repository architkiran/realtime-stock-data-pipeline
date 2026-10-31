"""Daily data-quality checks. Results land in gold/dq_results for Athena and the dashboard.

Each check returns rows of {check, status, detail}; status is PASS, WARN, or FAIL.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd

from stockstream.common.config import load_universe
from stockstream.common.markets import NY, is_trading_day, regular_close
from stockstream.common.storage import (
    delete_prefix,
    list_keys,
    put_parquet,
    read_jsonl_gz,
    read_parquet_prefix,
)

UTC = timezone.utc  # datetime.UTC needs Python 3.11; Glue Python shell runs 3.9


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
    """Every equity symbol should trade in every full regular-session hour (10:00 up to the close)."""
    if not is_trading_day(day):
        return {"check": "missing_equity_hours", "status": "PASS", "detail": "not a trading day"}
    universe = load_universe()
    last_full_hour = regular_close(day).hour  # 16 normally, 13 on half days
    expected_hours = [datetime(day.year, day.month, day.day, h, tzinfo=NY) for h in range(10, last_full_hour)]
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


def _utc_day_mask(ts: pd.Series, day: date) -> pd.Series:
    start = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)
    return (ts >= start) & (ts < start + 86_400_000)


def run_dq(s3, bucket: str, day: date, arrival_slack_hours: int = 6) -> pd.DataFrame:
    """Reconcile the same set of trades on both sides: everything whose EVENT time falls on `day` (UTC).

    Bronze is partitioned by arrival time, so trades from `day` can sit in the first hours of the
    next day's bronze; read `arrival_slack_hours` extra, then filter by event time. Silver is
    partitioned by event time already. Backfill rows come from bronze/backfill/, not the stream,
    so they are excluded from this stream reconciliation.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    bronze_rows, quarantine_rows = [], []
    for h in range(0, 24 + arrival_slack_hours):
        ts = start + timedelta(hours=h)
        dt, hour = ts.strftime("%Y-%m-%d"), ts.strftime("%H")
        for key in list_keys(s3, bucket, f"bronze/dt={dt}/hour={hour}/"):
            bronze_rows.extend(read_jsonl_gz(s3, bucket, key))
        for key in list_keys(s3, bucket, f"quarantine/dt={dt}/hour={hour}/"):
            quarantine_rows.extend(read_jsonl_gz(s3, bucket, key))

    bronze = pd.DataFrame(bronze_rows)
    if not bronze.empty and "event_ts" in bronze:
        bronze = bronze[pd.to_numeric(bronze["event_ts"], errors="coerce").pipe(_utc_day_mask, day)]
    quarantined = sum(1 for q in quarantine_rows if isinstance(q.get("payload"), dict)
                      and isinstance(q["payload"].get("event_ts"), (int, float))
                      and _utc_day_mask(pd.Series([q["payload"]["event_ts"]]), day).iloc[0])
    silver = read_parquet_prefix(s3, bucket, f"silver/trades/dt={day.isoformat()}/")
    if not silver.empty:
        silver = silver[~silver["is_backfill"].astype(bool)]
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
