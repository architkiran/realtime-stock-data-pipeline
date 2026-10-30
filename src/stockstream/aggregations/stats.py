"""Pure pandas statistics. No I/O here, so every formula is unit-testable.

Definitions (documented so the numbers are reproducible):
- VWAP = sum(price * size) / sum(size)
- Hourly realized volatility = std dev of 1-minute log returns inside the hour
  (last trade price per minute), not annualized.
- return_vs_prev_close = close / previous hour's close - 1 (previous close can be
  passed in from gold so a recompute window doesn't lose its first hour).
- Daily volatility_20d = std dev of the last 20 daily log returns x sqrt(252)
  for equities, x sqrt(365) for crypto.
- volume_z = (volume - mean of previous 20 days) / std of previous 20 days

Source selection: the same equity can arrive from several feeds that are not the
same market (Alpaca's free feed is IEX only). Summing them double counts, and they
can't be deduplicated trade by trade because Finnhub sends no trade id. So for each
symbol and bucket (hour or day) only the highest-priority source present is used.

Only plain groupby aggregations are used here (no groupby.apply(include_groups=...)),
so this module runs on the pandas pinned for the Glue Python 3.9 runtime.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from stockstream.common.markets import NY

HOURLY_COLUMNS = [
    "symbol", "asset_class", "source", "hour_start", "open", "high", "low", "close", "volume",
    "trade_count", "vwap", "realized_vol_1m", "return_vs_prev_close",
]
DAILY_COLUMNS = [
    "symbol", "asset_class", "source", "trade_date", "open", "high", "low", "close", "volume", "trade_count",
    "vwap", "return_1d", "ma_20", "ma_50", "volatility_20d", "volume_z",
]
BAR_COLUMNS = DAILY_COLUMNS[:11]

# Lower number wins. Finnhub's free feed is consolidated US trades; Alpaca's free feed is IEX only.
SOURCE_PRIORITY = {"finnhub": 0, "alpaca": 1, "coinbase": 0}


def dedupe(trades: pd.DataFrame) -> pd.DataFrame:
    """Kinesis is at-least-once: the same event_id can appear in several silver files."""
    return trades.sort_values("processed_ts").drop_duplicates("event_id", keep="first")


def select_primary_source(trades: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    """Keep only the best-ranked source per (symbol, bucket)."""
    rank = trades["source"].map(SOURCE_PRIORITY).fillna(99)
    best = rank.groupby([trades["symbol"], trades[bucket_col]]).transform("min")
    return trades[rank == best]


def _bars(t: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """OHLCV + VWAP per group. `t` must already be sorted by event_ts."""
    t = t.assign(notional=t["price"] * t["size"])
    g = t.groupby(keys, sort=True)
    out = g.agg(asset_class=("asset_class", "first"), source=("source", "first"), open=("price", "first"),
                high=("price", "max"), low=("price", "min"), close=("price", "last"),
                volume=("size", "sum"), trade_count=("price", "size"), notional=("notional", "sum"))
    out["vwap"] = out["notional"] / out["volume"]
    return out.drop(columns="notional").reset_index()


def hourly_stats(trades: pd.DataFrame, prev_close: dict[str, float] | None = None) -> pd.DataFrame:
    """Hourly bars. `prev_close` maps symbol -> close of the hour before the earliest hour in `trades`."""
    if trades.empty:
        return pd.DataFrame(columns=HOURLY_COLUMNS)
    t = dedupe(trades)
    t = t.assign(hour_ms=t["event_ts"] // 3_600_000 * 3_600_000)
    t = select_primary_source(t, "hour_ms").sort_values("event_ts", kind="stable")
    keys = ["symbol", "hour_ms"]
    out = _bars(t, keys)

    # realized volatility from last price per minute
    minute = t.assign(minute=t["event_ts"] // 60_000).groupby(keys + ["minute"])["price"].last()
    log_ret = np.log(minute).groupby(level=[0, 1]).diff()
    vol = log_ret.groupby(level=[0, 1]).agg(["std", "count"])
    vol["realized_vol_1m"] = vol["std"].where(vol["count"] >= 2)
    out = out.merge(vol[["realized_vol_1m"]].reset_index(), on=keys, how="left")

    out = out.sort_values(keys)
    prev = out.groupby("symbol")["close"].shift(1)
    if prev_close:
        first = out.groupby("symbol").cumcount() == 0
        prev = prev.where(~first, out["symbol"].map(prev_close))
    out["return_vs_prev_close"] = out["close"] / prev - 1
    out["hour_start"] = pd.to_datetime(out["hour_ms"], unit="ms", utc=True)
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
        return pd.DataFrame(columns=BAR_COLUMNS)
    t = dedupe(trades)
    t = t[(t["asset_class"] != "equity") | (t["session"] == "regular")]
    if t.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    t = t.assign(trade_date=trade_date(t["event_ts"], t["asset_class"]))
    t = select_primary_source(t, "trade_date").sort_values("event_ts", kind="stable")
    return _bars(t, ["symbol", "trade_date"])[BAR_COLUMNS]


def add_rolling(daily: pd.DataFrame) -> pd.DataFrame:
    """Rolling metrics need history: pass previous days' bars together with today's."""
    d = daily.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    g = d.groupby("symbol")
    d["return_1d"] = g["close"].pct_change()
    d["ma_20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    d["ma_50"] = g["close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    log_ret = np.log(d["close"]).groupby(d["symbol"]).diff()
    ann = np.where(d["asset_class"] == "crypto", np.sqrt(365), np.sqrt(252))
    d["volatility_20d"] = log_ret.groupby(d["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).std()) * ann
    prev_mean = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).mean())
    prev_std = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).std())
    d["volume_z"] = (d["volume"] - prev_mean) / prev_std
    return d[DAILY_COLUMNS]
