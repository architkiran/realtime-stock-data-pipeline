import math

import pandas as pd
import pytest

from stockstream.aggregations.stats import add_rolling, daily_bars, hourly_stats

H = 3_600_000
T0 = 1793282400000  # 2026-10-29 14:00 UTC = 10:00 New York


def trades(rows):
    df = pd.DataFrame(rows, columns=["event_id", "symbol", "asset_class", "price", "size", "event_ts"])
    df["session"] = "regular"
    df["source"] = "finnhub"
    df["processed_ts"] = df["event_ts"]
    return df


def test_hourly_ohlc_and_vwap():
    df = trades([
        ("a", "AAPL", "equity", 10.0, 100, T0 + 1000),
        ("b", "AAPL", "equity", 12.0, 100, T0 + 2000),
        ("c", "AAPL", "equity", 9.0, 200, T0 + 3000),
        ("d", "AAPL", "equity", 11.0, 100, T0 + 4000),
    ])
    row = hourly_stats(df).iloc[0]
    assert (row.open, row.high, row.low, row.close) == (10.0, 12.0, 9.0, 11.0)
    assert row.volume == 500 and row.trade_count == 4
    assert row.vwap == pytest.approx((1000 + 1200 + 1800 + 1100) / 500)


def test_duplicates_are_counted_once():
    df = trades([("a", "AAPL", "equity", 10.0, 100, T0), ("a", "AAPL", "equity", 10.0, 100, T0)])
    assert hourly_stats(df).iloc[0].volume == 100


def test_return_vs_previous_hour():
    df = trades([("a", "AAPL", "equity", 10.0, 1, T0), ("b", "AAPL", "equity", 11.0, 1, T0 + H)])
    out = hourly_stats(df)
    assert math.isnan(out.iloc[0].return_vs_prev_close)
    assert out.iloc[1].return_vs_prev_close == pytest.approx(0.10)


def test_daily_rolling_needs_20_days():
    days = pd.date_range("2026-09-01", periods=25, freq="D").date
    daily = pd.DataFrame({"symbol": "X", "asset_class": "equity", "source": "finnhub", "trade_date": days, "open": 1.0,
                          "high": 1.0, "low": 1.0, "close": [100 + i for i in range(25)],
                          "volume": [1000.0] * 24 + [5000.0], "trade_count": 1, "vwap": 1.0})
    out = add_rolling(daily)
    assert out["ma_20"].iloc[18] != out["ma_20"].iloc[18]  # NaN before 20 days
    assert out["ma_20"].iloc[19] == pytest.approx(sum(100 + i for i in range(20)) / 20)


def test_equity_daily_bar_uses_new_york_date_and_regular_session():
    late = T0 + 9 * H  # 23:00 UTC = 19:00 New York, same NY date, extended hours
    df = trades([("a", "AAPL", "equity", 10.0, 1, T0), ("b", "AAPL", "equity", 99.0, 1, late)])
    df.loc[1, "session"] = "extended"
    bars = daily_bars(df)
    assert len(bars) == 1 and bars.iloc[0].close == 10.0
    assert str(bars.iloc[0].trade_date) == "2026-10-29"
