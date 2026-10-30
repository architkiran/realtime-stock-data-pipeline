from stockstream.common.storage import list_keys, read_jsonl_gz, read_parquet_prefix
from stockstream.producer.parsers import parse_finnhub
from stockstream.stream_processor.handler import lambda_handler
from tests.conftest import fixture, kinesis_event

NOW = 1793289605000


def test_valid_records_land_in_silver(aws):
    events = parse_finnhub(fixture("finnhub_trade.json"), NOW - 1000)
    resp = lambda_handler(kinesis_event(events), now=NOW)
    assert resp == {"batchItemFailures": []}
    silver = read_parquet_prefix(aws["s3"], aws["bucket"], "silver/trades/dt=2026-10-29/hour=16/")
    assert len(silver) == 2 and set(silver["session"]) == {"regular"}


def test_bad_records_are_quarantined_with_reason(aws):
    good = parse_finnhub(fixture("finnhub_trade.json"), NOW - 1000)[0]
    bad_price = {**good, "price": 0}
    unknown = {**good, "symbol": "ZZZZ"}
    lambda_handler(kinesis_event([good, bad_price, unknown, b"not json"]), now=NOW)
    keys = list_keys(aws["s3"], aws["bucket"], "quarantine/")
    rows = [r for k in keys for r in read_jsonl_gz(aws["s3"], aws["bucket"], k)]
    assert sorted(r["reason"] for r in rows) == ["schema", "undecodable", "unknown_symbol"]


def test_kinesis_round_trip(aws):
    from stockstream.producer.kinesis_sink import KinesisSink

    kin = aws["kinesis"]
    kin.create_stream(StreamName="s", ShardCount=1)
    sink = KinesisSink(kin, "s", sleep=lambda s: None)
    sink.add(parse_finnhub(fixture("finnhub_trade.json"), NOW))
    sink.flush()
    shard = kin.describe_stream(StreamName="s")["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(StreamName="s", ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    assert len(kin.get_records(ShardIterator=it)["Records"]) == 2
