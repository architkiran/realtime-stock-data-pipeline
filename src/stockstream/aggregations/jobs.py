"""Batch jobs: read silver, compute stats, overwrite gold partitions.

Gold partitions are rewritten whole on every run (delete then write), which
makes reruns idempotent: running the hourly job twice gives the same files.
The hourly job recomputes a lookback window so late trades are absorbed.

These functions are what the Glue Python shell jobs in glue_jobs/ call.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from stockstream.aggregations.stats import add_rolling, daily_bars, hourly_stats
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.common.storage import delete_prefix, put_parquet, read_parquet_prefix

log = get_logger(__name__)


def _hour_prefixes(start: datetime, hours: int) -> list[tuple[str, str]]:
    return [((start + timedelta(hours=i)).strftime("%Y-%m-%d"), (start + timedelta(hours=i)).strftime("%H"))
            for i in range(hours)]


def read_silver_hours(s3, bucket: str, start: datetime, hours: int) -> pd.DataFrame:
    frames = [read_parquet_prefix(s3, bucket, f"silver/trades/dt={dt}/hour={h}/") for dt, h in
              _hour_prefixes(start, hours)]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_hourly(s3, bucket: str, now: datetime, lookback_hours: int = 3) -> int:
    """Recompute the last `lookback_hours` closed hours and overwrite their gold partitions."""
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=lookback_hours)
    trades = read_silver_hours(s3, bucket, start, lookback_hours)
    stats = hourly_stats(trades)
    written = 0
    for dt, h in _hour_prefixes(start, lookback_hours):
        prefix = f"gold/hourly_stats/dt={dt}/hour={h}/"
        delete_prefix(s3, bucket, prefix)
        hour_start = pd.Timestamp(f"{dt} {h}:00", tz="UTC")
        part = stats[stats["hour_start"] == hour_start]
        if not part.empty:
            put_parquet(s3, bucket, prefix + "part-0.parquet", part)
            written += len(part)
    log_fields(log, "hourly job done", window_start=start.isoformat(), rows=written, trades=len(trades))
    return written


def run_daily(s3, bucket: str, day: date, history_days: int = 70) -> int:
    """Build daily bars for `day` and rolling metrics using stored history."""
    # A New York trading day spans two UTC dates, so read the UTC day plus the next one.
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    trades = read_silver_hours(s3, bucket, start, 30)
    bars = daily_bars(trades)
    bars = bars[bars["trade_date"] == day]

    history = []
    for i in range(1, history_days + 1):
        d = day - timedelta(days=i)
        part = read_parquet_prefix(s3, bucket, f"gold/daily_stats/dt={d.isoformat()}/")
        if not part.empty:
            history.append(part[["symbol", "asset_class", "trade_date", "open", "high", "low", "close",
                                 "volume", "trade_count", "vwap"]])
    combined = pd.concat([*history, bars], ignore_index=True) if history else bars
    if combined.empty:
        return 0
    combined["trade_date"] = pd.to_datetime(combined["trade_date"]).dt.date
    full = add_rolling(combined)
    today = full[full["trade_date"] == day]

    prefix = f"gold/daily_stats/dt={day.isoformat()}/"
    delete_prefix(s3, bucket, prefix)
    if not today.empty:
        put_parquet(s3, bucket, prefix + "part-0.parquet", today)
    log_fields(log, "daily job done", day=day.isoformat(), rows=len(today), history_days=len(history))
    return len(today)
