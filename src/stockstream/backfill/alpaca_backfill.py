"""Historical backfill from Alpaca's free Market Data API (IEX feed).

Pages GET /v2/stocks/{symbol}/trades for one symbol and one day at a time,
normalizes each trade into trade_event (is_backfill=true), and writes bronze
files. Progress is checkpointed per symbol-day so a crash or Ctrl-C resumes
where it stopped, and a finished symbol-day is never fetched twice.

Usage:
    python -m stockstream.backfill.alpaca_backfill --start 2026-09-01 --end 2026-09-30
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import requests

from stockstream.common.config import load_universe, settings
from stockstream.common.events import build_event, iso_to_ms, now_ms
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.common.markets import is_trading_day
from stockstream.common.storage import put_jsonl_gz

log = get_logger(__name__)

BASE_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/trades"
PAGE_LIMIT = 10_000  # the endpoint's maximum


@dataclass
class RateLimiter:
    """Token bucket: at most `per_minute` calls in any 60 s window."""
    per_minute: int = 180  # stay under the free plan's 200/min
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _calls: list[float] = field(default_factory=list)

    def wait(self) -> None:
        now = self.clock()
        self._calls = [t for t in self._calls if now - t < 60]
        if len(self._calls) >= self.per_minute:
            self.sleep(60 - (now - self._calls[0]))
        self._calls.append(self.clock())


@dataclass
class Checkpoint:
    path: Path

    def done(self) -> set[str]:
        if not self.path.exists():
            return set()
        return set(json.loads(self.path.read_text()))

    def mark(self, key: str) -> None:
        keys = self.done() | {key}
        self.path.write_text(json.dumps(sorted(keys)))


def fetch_day(session: requests.Session, limiter: RateLimiter, symbol: str, day: date,
              max_retries: int = 5) -> list[dict]:
    url = BASE_URL.format(symbol=symbol)
    params = {"start": f"{day}T00:00:00Z", "end": f"{day + timedelta(days=1)}T00:00:00Z",
              "limit": PAGE_LIMIT, "feed": "iex"}
    trades, page_token, retries = [], None, 0
    while True:
        if page_token:
            params["page_token"] = page_token
        limiter.wait()
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            retries += 1
            if retries > max_retries:
                resp.raise_for_status()
            limiter.sleep(min(60, 2 ** retries))
            continue
        resp.raise_for_status()
        retries = 0  # the retry budget is per request, not per day
        body = resp.json()
        trades.extend(body.get("trades") or [])
        page_token = body.get("next_page_token")
        if not page_token:
            return trades


def to_events(symbol: str, raw: list[dict]) -> list[dict]:
    ingest = now_ms()
    return [
        build_event(source="alpaca", asset_class="equity", symbol=symbol, price=float(t["p"]),
                    size=float(t["s"]), event_ts=iso_to_ms(t["t"]), ingest_ts=ingest, trade_id=str(t["i"]),
                    venue=t.get("x"), conditions=list(t["c"]) if t.get("c") else None, is_backfill=True)
        for t in raw
    ]


def run(s3, bucket: str, session: requests.Session, symbols: list[str], start: date, end: date,
        checkpoint: Checkpoint, limiter: RateLimiter | None = None) -> int:
    limiter = limiter or RateLimiter()
    done, total = checkpoint.done(), 0
    day = start
    while day <= end:
        if is_trading_day(day):
            for symbol in symbols:
                key = f"{symbol}:{day}"
                if key in done:
                    continue
                events = to_events(symbol, fetch_day(session, limiter, symbol, day))
                if events:
                    put_jsonl_gz(s3, bucket, f"bronze/backfill/dt={day}/symbol={symbol}/part-0.json.gz", events)
                checkpoint.mark(key)
                total += len(events)
                log_fields(log, "backfilled", symbol=symbol, day=str(day), trades=len(events))
        day += timedelta(days=1)
    return total


def main() -> None:
    import boto3

    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--symbols", default="")
    p.add_argument("--checkpoint", default=".backfill_checkpoint.json")
    args = p.parse_args()

    session = requests.Session()
    session.headers.update({"APCA-API-KEY-ID": os.environ["ALPACA_KEY"],
                            "APCA-API-SECRET-KEY": os.environ["ALPACA_SECRET"]})
    symbols = args.symbols.split(",") if args.symbols else list(load_universe().equities)
    cfg = settings()
    total = run(boto3.client("s3", region_name=cfg.region), cfg.bucket, session, symbols, args.start, args.end,
                Checkpoint(Path(args.checkpoint)))
    print(f"backfilled {total} trades")


if __name__ == "__main__":
    main()
