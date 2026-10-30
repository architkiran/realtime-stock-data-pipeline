"""Turn raw websocket messages from each source into trade_event dicts.

Each parser takes the decoded JSON message and the time it was received and
returns zero or more events. Non-trade messages (pings, acks, subscriptions)
return an empty list. Parsers never raise on unexpected input; they return what
they could parse and count the rest, so one weird message can't kill the producer.
"""

from __future__ import annotations

from typing import Any

from stockstream.common.events import build_event, iso_to_ms


def parse_finnhub(msg: dict[str, Any], ingest_ts: int) -> list[dict]:
    # {"type":"trade","data":[{"s":"AAPL","p":187.3,"v":100,"t":1730300000123,"c":["1","12"]}]}
    if msg.get("type") != "trade":
        return []
    events = []
    for t in msg.get("data") or []:
        events.append(
            build_event(
                source="finnhub",
                asset_class="equity",
                symbol=t["s"],
                price=float(t["p"]),
                size=float(t["v"]),
                event_ts=int(t["t"]),
                ingest_ts=ingest_ts,
                conditions=[str(c) for c in t["c"]] if t.get("c") else None,
            )
        )
    return events


def parse_alpaca(msg: list[dict[str, Any]] | dict[str, Any], ingest_ts: int) -> list[dict]:
    # Alpaca sends arrays: [{"T":"t","S":"AAPL","i":96921,"x":"V","p":126.55,"s":1,
    #                        "t":"2021-02-22T15:51:44.208Z","c":["@","I"],"z":"C"}]
    items = msg if isinstance(msg, list) else [msg]
    events = []
    for t in items:
        if t.get("T") != "t":
            continue
        events.append(
            build_event(
                source="alpaca",
                asset_class="equity",
                symbol=t["S"],
                price=float(t["p"]),
                size=float(t["s"]),
                event_ts=iso_to_ms(t["t"]),
                ingest_ts=ingest_ts,
                trade_id=str(t["i"]),
                venue=t.get("x"),
                conditions=list(t["c"]) if t.get("c") else None,
            )
        )
    return events


def parse_coinbase(msg: dict[str, Any], ingest_ts: int) -> list[dict]:
    # {"channel":"market_trades","events":[{"type":"update","trades":[{"trade_id":"123",
    #   "product_id":"BTC-USD","price":"67000.1","size":"0.01","side":"BUY","time":"...Z"}]}]}
    if msg.get("channel") != "market_trades":
        return []
    events = []
    for ev in msg.get("events") or []:
        for t in ev.get("trades") or []:
            events.append(
                build_event(
                    source="coinbase",
                    asset_class="crypto",
                    symbol=t["product_id"],
                    price=float(t["price"]),
                    size=float(t["size"]),
                    event_ts=iso_to_ms(t["time"]),
                    ingest_ts=ingest_ts,
                    trade_id=str(t["trade_id"]),
                    venue="coinbase",
                )
            )
    return events


PARSERS = {"finnhub": parse_finnhub, "alpaca": parse_alpaca, "coinbase": parse_coinbase}
