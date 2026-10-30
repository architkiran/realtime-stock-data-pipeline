"""Market session logic. Store UTC everywhere; use New York time only for sessions."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc  # datetime.UTC needs Python 3.11; Glue Python shell runs 3.9

NY = ZoneInfo("America/New_York")

PRE_OPEN = time(4, 0)
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
POST_CLOSE = time(20, 0)

# NYSE full-day closures for the years this project covers. In production swap this
# for `exchange_calendars`; a small explicit list keeps the Lambda zip tiny.
NYSE_HOLIDAYS = {
    date(2025, 1, 1), date(2025, 1, 9), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
    date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1), date(2025, 11, 27),
    date(2025, 12, 25),
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3), date(2026, 5, 25),
    date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
}


# 13:00 New York early closes (post-market ends 17:00 on these days).
NYSE_EARLY_CLOSES = {date(2025, 7, 3), date(2025, 11, 28), date(2025, 12, 24), date(2026, 11, 27), date(2026, 12, 24)}
EARLY_CLOSE = time(13, 0)
EARLY_POST_CLOSE = time(17, 0)


def regular_close(d: date) -> time:
    return EARLY_CLOSE if d in NYSE_EARLY_CLOSES else REGULAR_CLOSE


def to_ny(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(NY)


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def equity_session(ts_ms: int) -> str:
    """Classify an equity trade: 'regular', 'extended', or 'off_hours'."""
    local = to_ny(ts_ms)
    if not is_trading_day(local.date()):
        return "off_hours"
    t, d = local.time(), local.date()
    if REGULAR_OPEN <= t < regular_close(d):
        return "regular"
    if PRE_OPEN <= t < (EARLY_POST_CLOSE if d in NYSE_EARLY_CLOSES else POST_CLOSE):
        return "extended"
    return "off_hours"


def utc_partition(ts_ms: int) -> tuple[str, str]:
    """(dt, hour) partition values for an event time, in UTC."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")
