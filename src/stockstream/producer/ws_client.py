"""Reconnecting websocket readers, one per source.

A websocket will drop: network blips, server restarts, idle timeouts. The
reader reconnects with exponential backoff, resubscribes, and records a gap
marker (the time it was disconnected) so the daily data-quality job can ask
the REST API to backfill that window.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import websockets

from stockstream.common.events import now_ms
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.producer.parsers import PARSERS

log = get_logger(__name__)


def subscribe_messages(source: str, symbols: tuple[str, ...], api_key: str = "", api_secret: str = "") -> list[str]:
    if source == "finnhub":
        return [json.dumps({"type": "subscribe", "symbol": s}) for s in symbols]
    if source == "alpaca":
        return [
            json.dumps({"action": "auth", "key": api_key, "secret": api_secret}),
            json.dumps({"action": "subscribe", "trades": list(symbols)}),
        ]
    if source == "coinbase":
        return [
            json.dumps({"type": "subscribe", "channel": "market_trades", "product_ids": list(symbols)}),
            json.dumps({"type": "subscribe", "channel": "heartbeats"}),
        ]
    raise ValueError(f"unknown source {source}")


@dataclass
class Gap:
    source: str
    start_ms: int
    end_ms: int | None = None


@dataclass
class SourceReader:
    source: str
    url: str
    symbols: tuple[str, ...]
    on_events: Callable[[list[dict]], Awaitable[None] | None]
    api_key: str = ""
    api_secret: str = ""
    max_backoff_s: float = 60.0
    gaps: list[Gap] = field(default_factory=list)
    messages_seen: int = 0
    parse_failures: int = 0
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

    def stop(self) -> None:
        self._stop.set()

    async def run(self, max_connections: int | None = None) -> None:
        parser = PARSERS[self.source]
        backoff, connections = 1.0, 0
        while not self._stop.is_set():
            connections += 1
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    for msg in subscribe_messages(self.source, self.symbols, self.api_key, self.api_secret):
                        await ws.send(msg)
                    if self.gaps and self.gaps[-1].end_ms is None:
                        self.gaps[-1].end_ms = now_ms()
                    backoff = 1.0
                    log_fields(log, "connected", source=self.source, symbols=len(self.symbols))
                    async for raw in ws:
                        self.messages_seen += 1
                        try:
                            events = parser(json.loads(raw), now_ms())
                        except Exception as exc:  # a bad frame must never stop the stream
                            self.parse_failures += 1
                            if self.parse_failures <= 20 or self.parse_failures % 1000 == 0:
                                log_fields(log, "unparseable frame", source=self.source, error=repr(exc),
                                           frame=str(raw)[:300], total=self.parse_failures)
                            continue
                        if events:
                            result = self.on_events(events)
                            if asyncio.iscoroutine(result):
                                await result
                        if self._stop.is_set():
                            return
            except (OSError, websockets.ConnectionClosed) as exc:
                log_fields(log, "disconnected", source=self.source, error=str(exc))
            self.gaps.append(Gap(self.source, now_ms()))
            if max_connections is not None and connections >= max_connections:
                return
            delay = min(self.max_backoff_s, backoff) * (0.5 + random.random())
            await asyncio.sleep(delay)
            backoff *= 2
