"""Synthetic market feed that speaks each source's native wire format.

Prices follow geometric Brownian motion, trade arrivals are Poisson, and the
messages are shaped exactly like Finnhub, Alpaca, and Coinbase messages, so
the real parsers are exercised end to end without network access.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

UTC = timezone.utc  # datetime.UTC needs Python 3.11; Glue Python shell runs 3.9

START_PRICES = {
    "AAPL": 230.0, "MSFT": 420.0, "NVDA": 180.0, "AMZN": 220.0, "GOOGL": 170.0, "META": 590.0,
    "TSLA": 260.0, "JPM": 240.0, "SPY": 580.0, "QQQ": 500.0,
    "BTC-USD": 68000.0, "ETH-USD": 2600.0, "SOL-USD": 160.0,
}


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class SymbolSim:
    symbol: str
    source: str
    price: float
    trades_per_min: float
    annual_vol: float
    next_id: int = 1

    def step(self, rng: np.random.Generator, start_ms: int, minutes: int) -> list[tuple[int, float, float, int]]:
        n = rng.poisson(self.trades_per_min * minutes)
        times = np.sort(rng.integers(start_ms, start_ms + minutes * 60_000, size=n))
        # per-trade GBM increment, scaled to the time between trades
        dt_years = np.diff(np.concatenate([[start_ms], times])) / (365 * 24 * 3600 * 1000)
        shocks = rng.standard_normal(n) * self.annual_vol * np.sqrt(np.maximum(dt_years, 1e-12))
        out = []
        for t, z in zip(times, shocks):
            self.price *= math.exp(z)
            size = float(max(1, int(rng.lognormal(3, 1))))
            if self.source == "coinbase":
                size = round(size / 1000, 6)
            out.append((int(t), round(self.price, 2), size, self.next_id))
            self.next_id += 1
        return out


def to_wire(source: str, symbol: str, trades: list[tuple[int, float, float, int]]) -> list:
    """Wrap trades in the source's message format (one message per ~50 trades)."""
    msgs = []
    for i in range(0, len(trades), 50):
        chunk = trades[i : i + 50]
        if source == "finnhub":
            msgs.append({"type": "trade", "data": [{"s": symbol, "p": p, "v": s, "t": t, "c": ["1"]}
                                                   for t, p, s, _ in chunk]})
        elif source == "alpaca":
            msgs.append([{"T": "t", "S": symbol, "i": tid, "x": "V", "p": p, "s": s, "t": iso(t),
                          "c": ["@"], "z": "C"} for t, p, s, tid in chunk])
        else:
            msgs.append({"channel": "market_trades", "timestamp": iso(chunk[-1][0]), "sequence_num": i,
                         "events": [{"type": "update", "trades": [
                             {"trade_id": str(tid), "product_id": symbol, "price": f"{p}", "size": f"{s}",
                              "side": "BUY", "time": iso(t)} for t, p, s, tid in chunk]}]})
    return msgs


def default_sims(scale: float = 1.0) -> list[SymbolSim]:
    """One simulated feed per symbol in config/symbols.yaml (equities split across Finnhub and Alpaca)."""
    from stockstream.common.config import load_universe

    universe = load_universe()
    sims = []
    for i, sym in enumerate(universe.equities):
        source = "finnhub" if i % 2 == 0 else "alpaca"
        sims.append(SymbolSim(sym, source, START_PRICES.get(sym, 100.0 + 10 * i), 20 * scale, 0.3))
    for sym in universe.crypto:
        sims.append(SymbolSim(sym, "coinbase", START_PRICES.get(sym, 100.0), 6 * scale, 0.6))
    return sims
