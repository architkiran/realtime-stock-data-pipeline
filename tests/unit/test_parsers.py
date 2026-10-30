from stockstream.common.events import schema_errors
from stockstream.producer.parsers import parse_alpaca, parse_coinbase, parse_finnhub
from tests.conftest import fixture

INGEST = 1793289601000


def test_finnhub_parses_all_trades_and_matches_contract():
    events = parse_finnhub(fixture("finnhub_trade.json"), INGEST)
    assert [e["price"] for e in events] == [227.31, 227.32]
    assert events[0]["conditions"] == ["1", "12"] and events[1]["conditions"] is None
    assert all(schema_errors(e) == [] for e in events)


def test_finnhub_ignores_pings():
    assert parse_finnhub({"type": "ping"}, INGEST) == []


def test_alpaca_keeps_only_trades():
    events = parse_alpaca(fixture("alpaca_trades.json"), INGEST)
    assert len(events) == 1
    e = events[0]
    assert (e["symbol"], e["price"], e["size"], e["trade_id"], e["venue"]) == ("MSFT", 421.17, 10.0, "52983525028", "V")
    assert e["event_ts"] == 1793289104208
    assert schema_errors(e) == []


def test_coinbase_parses_string_numbers():
    events = parse_coinbase(fixture("coinbase_market_trades.json"), INGEST)
    assert [e["size"] for e in events] == [0.0125, 0.3]
    assert all(e["asset_class"] == "crypto" and schema_errors(e) == [] for e in events)


def test_event_id_is_deterministic():
    a = parse_coinbase(fixture("coinbase_market_trades.json"), INGEST)
    b = parse_coinbase(fixture("coinbase_market_trades.json"), INGEST + 5000)
    assert [e["event_id"] for e in a] == [e["event_id"] for e in b]
