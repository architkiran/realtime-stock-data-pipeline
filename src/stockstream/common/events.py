"""The trade_event contract: building, hashing, and validating events.

Every source is normalized into this one shape before it enters Kinesis, so the
consumer only ever has to understand one schema.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

UTC = timezone.utc  # datetime.UTC needs Python 3.11; Glue Python shell runs 3.9

SCHEMA_VERSION = 1
SOURCES = ("finnhub", "alpaca", "coinbase")


def _contract_path() -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parent.parent / "contracts" / "trade_event.v1.json",  # packaged copy (Lambda zip)
        here.parents[3] / "contracts" / "trade_event.v1.json",  # repo checkout
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("trade_event.v1.json not found in: " + ", ".join(map(str, candidates)))


@lru_cache(maxsize=1)
def validator() -> Draft202012Validator:
    schema = json.loads(_contract_path().read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def make_event_id(source: str, symbol: str, trade_id: str | None, event_ts: int, price: float, size: float,
                  occurrence: int = 0) -> str:
    """Deterministic dedupe key. Same trade from a replay produces the same id.

    Without a trade id, two genuinely different trades can share symbol, millisecond, price and
    size (common for algorithmic order slicing). `occurrence` numbers such look-alikes in arrival
    order (0, 1, 2...) so they keep distinct ids; 0 leaves the original id format unchanged.
    """
    if trade_id:
        raw = f"{source}|{symbol}|{trade_id}"
    else:
        raw = f"{source}|{symbol}|{event_ts}|{price}|{size}"
        if occurrence:
            raw += f"|{occurrence}"
    return hashlib.sha256(raw.encode()).hexdigest()


def build_event(
    *,
    source: str,
    asset_class: str,
    symbol: str,
    price: float,
    size: float,
    event_ts: int,
    ingest_ts: int,
    trade_id: str | None = None,
    venue: str | None = None,
    conditions: list[str] | None = None,
    is_backfill: bool = False,
    occurrence: int = 0,
) -> dict[str, Any]:
    return {
        "event_id": make_event_id(source, symbol, trade_id, event_ts, price, size, occurrence),
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "asset_class": asset_class,
        "symbol": symbol,
        "price": price,
        "size": size,
        "event_ts": event_ts,
        "ingest_ts": ingest_ts,
        "trade_id": trade_id,
        "venue": venue,
        "conditions": conditions,
        "is_backfill": is_backfill,
    }


def schema_errors(event: Any) -> list[str]:
    """Return human-readable contract violations (empty list = valid).

    JSON Schema can't reject NaN or Infinity: Python's json module accepts both, and
    `NaN <= 0` is False, so `exclusiveMinimum: 0` lets NaN through. Checked explicitly.
    """
    errors = [
        f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
        for e in sorted(validator().iter_errors(event), key=lambda e: list(e.absolute_path))
    ]
    if isinstance(event, dict):
        for name in ("price", "size", "event_ts", "ingest_ts"):
            value = event.get(name)
            if isinstance(value, float) and not math.isfinite(value):
                errors.append(f"{name}: {value!r} is not a finite number")
    return errors


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def iso_to_ms(value: str) -> int:
    """Parse an RFC 3339 timestamp (with 'Z' or offset) to UTC epoch ms."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)
