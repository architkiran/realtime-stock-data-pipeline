"""Pure pandas statistics. No I/O here, so every formula is unit-testable.

Definitions (documented so the numbers are reproducible):
- VWAP = sum(price * size) / sum(size)
- Hourly realized volatility = std dev of 1-minute log returns inside the hour
  (last trade price per minute), not annualized.
- Daily volatility_20d = std dev of the last 20 daily log returns x sqrt(252)
  for equities, x sqrt(365) for crypto.
- volume_z = (volume - mean of previous 20 days) / std of previous 20 days
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from stockstream.common.markets import NY

HOURLY_COLUMNS = [
    "symbol", "asset_class", "hour_start", "open", "high", "low", "close", "volume",
    "trade_count", "vwap", "realized_vol_1m", "return_vs_prev_close",
]
DAILY_COLUMNS = [
    "symbol", "asset_class", "trade_date", "open", "high", "low", "close", "volume", "trade_count",
    "vwap", "return_1d", "ma_20", "ma_50", "volatility_20d", "volume_z",
]


def dedupe(trades: pd.DataFrame) -> pd.DataFrame:
    """Kinesis is at-least-once: the same event_id can appear in several silver files."""
    return trades.sort_values("processed_ts").drop_duplicates("event_id", keep="first")


def _ohlcv(group: pd.DataFrame) -> pd.Series:
    g = group.sort_values("event_ts")
    notional = (g["price"] * g["size"]).sum()
    volume = g["size"].sum()
    return pd.Series({
        "asset_class": g["asset_class"].iloc[0],
        "open": g["price"].iloc[0],
        "high": g["price"].max(),
        "low": g["price"].min(),
        "close": g["price"].iloc[-1],
        "volume": volume,
        "trade_count": len(g),
        "vwap": notional / volume,
    })


def _realized_vol(group: pd.DataFrame) -> float:
    g = group.sort_values("event_ts")
    minute_close = g.groupby(g["event_ts"] // 60_000)["price"].last()
    if len(minute_close) < 3:
        return float("nan")
    return float(np.log(minute_close).diff().dropna().std())


def hourly_stats(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=HOURLY_COLUMNS)
    t = dedupe(trades).copy()
    t["hour_start"] = pd.to_datetime(t["event_ts"] // 3_600_000 * 3_600_000, unit="ms", utc=True)
    keys = ["symbol", "hour_start"]
    base = t.groupby(keys).apply(_ohlcv, include_groups=False).reset_index()
    vol = t.groupby(keys).apply(_realized_vol, include_groups=False).rename("realized_vol_1m").reset_index()
    out = base.merge(vol, on=keys).sort_values(keys)
    out["return_vs_prev_close"] = out.groupby("symbol")["close"].pct_change()
    return out[HOURLY_COLUMNS].reset_index(drop=True)


def trade_date(row_ts: pd.Series, asset_class: pd.Series) -> pd.Series:
    """Equities use the New York calendar date; crypto uses the UTC date."""
    utc = pd.to_datetime(row_ts, unit="ms", utc=True)
    ny_date = utc.dt.tz_convert(NY).dt.date
    utc_date = utc.dt.date
    return ny_date.where(asset_class == "equity", utc_date)


def daily_bars(trades: pd.DataFrame) -> pd.DataFrame:
    """One OHLCV row per symbol and trade date. Equities: regular session only."""
    if trades.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS[:10])
    t = dedupe(trades)
    t = t[(t["asset_class"] != "equity") | (t["session"] == "regular")].copy()
    t["trade_date"] = trade_date(t["event_ts"], t["asset_class"])
    keys = ["symbol", "trade_date"]
    return t.groupby(keys).apply(_ohlcv, include_groups=False).reset_index()


def add_rolling(daily: pd.DataFrame) -> pd.DataFrame:
    """Rolling metrics need history: pass previous days' bars together with today's."""
    d = daily.sort_values(["symbol", "trade_date"]).copy()
    g = d.groupby("symbol")
    d["return_1d"] = g["close"].pct_change()
    d["ma_20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    d["ma_50"] = g["close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    log_ret = g["close"].transform(lambda s: np.log(s).diff())
    ann = np.where(d["asset_class"] == "crypto", np.sqrt(365), np.sqrt(252))
    d["volatility_20d"] = log_ret.groupby(d["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).std()) * ann
    prev_mean = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).mean())
    prev_std = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).std())
    d["volume_z"] = (d["volume"] - prev_mean) / prev_std
    return d[DAILY_COLUMNS].reset_index(drop=True)
