"""Break-it suite. Each test states how the pipeline SHOULD behave under abuse.

When this file was first run against the v0.1 code, the failures here became
the entries in docs/bug-log.md. Every test is kept as a regression test.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from stockstream.common.events import build_event
from stockstream.common.storage import list_keys, put_jsonl_gz, put_parquet, read_parquet_prefix
from stockstream.producer.kinesis_sink import KinesisSink
from stockstream.producer.parsers import parse_finnhub
from stockstream.stream_processor.handler import classify, lambda_handler
from tests.conftest import kinesis_event

pytestmark = pytest.mark.chaos
UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[2]
H = 3_600_000
T0 = 1793282400000  # 2026-10-29 14:00 UTC, 10:00 New York, a Thursday


def ev(symbol="AAPL", price=100.0, size=10.0, ts=T0, source="finnhub", asset="equity", trade_id=None, **kw):
    return build_event(source=source, asset_class=asset, symbol=symbol, price=price, size=size, event_ts=ts,
                       ingest_ts=ts + 500, trade_id=trade_id, **kw)


def silver_rows(events, now):
    valid, _, _ = classify([(str(i), e) for i, e in enumerate(events)], now)
    df = pd.DataFrame(valid)
    return df


# ---------------------------------------------------------------- producer


def test_01_parser_survives_a_malformed_trade():
    """One bad trade inside a message must not throw away the good ones or crash the reader."""
    msg = {"type": "trade", "data": [{"s": "AAPL", "p": 1.0, "v": 1, "t": T0},
                                     {"s": "AAPL", "v": 1, "t": T0},           # missing price
                                     {"s": "AAPL", "p": "n/a", "v": 1, "t": T0}]}
    events = parse_finnhub(msg, T0)
    assert len(events) == 1


def test_02_reader_survives_garbage_frames_and_reconnects():
    """Malformed JSON should be counted and skipped; a dropped socket should reconnect and close the gap."""
    import websockets

    from stockstream.producer.ws_client import SourceReader

    received: list[dict] = []
    good = json.dumps({"type": "trade", "data": [{"s": "AAPL", "p": 1.0, "v": 1, "t": T0}]})

    async def server(ws):
        await ws.recv()  # wait for the subscribe message, like the real servers
        await ws.send("{this is not json")
        await ws.send(good)
        await ws.close()  # drop the connection after two frames

    async def scenario():
        async with websockets.serve(server, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]
            reader = SourceReader("finnhub", f"ws://127.0.0.1:{port}", ("AAPL",), received.extend,
                                  max_backoff_s=0.05)
            await asyncio.wait_for(reader.run(max_connections=2), timeout=10)
            return reader

    reader = asyncio.run(scenario())
    assert reader.parse_failures == 2       # one garbage frame per connection
    assert len(received) == 2               # the good frame, once per connection
    assert reader.gaps[0].end_ms is not None  # the first gap was closed by the reconnect


class ThrottlingClient:
    """Fails the whole PutRecords call (as Kinesis does under heavy throttling) N times, then accepts."""

    def __init__(self, fail_calls: int):
        self.fail_calls, self.calls, self.accepted = fail_calls, 0, 0

    def put_records(self, StreamName, Records):  # noqa: N803 (boto3 casing)
        self.calls += 1
        if self.calls <= self.fail_calls:
            raise ClientError({"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow"}},
                              "PutRecords")
        self.accepted += len(Records)
        return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"} for _ in Records]}


def test_03_sink_retries_a_fully_throttled_call():
    client = ThrottlingClient(fail_calls=2)
    sink = KinesisSink(client, "s", sleep=lambda s: None)
    sink.add([ev(ts=T0 + i) for i in range(10)])
    sink.flush()
    assert client.accepted == 10 and sink.stats.dropped == 0


def test_04_sink_handles_a_burst_quickly():
    """A reconnect can deliver a burst; buffering 50k events must not be quadratic."""
    client = ThrottlingClient(fail_calls=0)
    sink = KinesisSink(client, "s", sleep=lambda s: None)
    events = [ev(ts=T0 + i) for i in range(50_000)]
    sink._buffer.extend({"Data": json.dumps(e).encode(), "PartitionKey": e["symbol"]} for e in events)
    start = time.perf_counter()
    sink.flush()
    assert client.accepted == 50_000
    assert time.perf_counter() - start < 1.0


def test_04b_flushing_does_not_block_the_event_loop():
    """boto3 is blocking; a slow PutRecords inside the event loop starves websocket pings."""
    from stockstream.producer.kinesis_sink import flush_async

    class SlowClient(ThrottlingClient):
        def put_records(self, StreamName, Records):  # noqa: N803
            time.sleep(0.4)
            return super().put_records(StreamName, Records)

    sink = KinesisSink(SlowClient(0), "s", sleep=lambda s: None)
    sink.add([ev(ts=T0 + i) for i in range(10)])
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(6):
            await asyncio.sleep(0.05)
            ticks += 1

    async def scenario():
        await asyncio.gather(flush_async(sink), ticker())

    asyncio.run(scenario())
    assert ticks == 6 and sink.client.accepted == 10


# ---------------------------------------------------------------- stream processor


def test_05_nan_and_infinite_prices_are_rejected(aws):
    payloads = [b'{"price": NaN}', ]
    good = ev()
    bad_nan = json.dumps({**good, "price": float("nan")}).encode()
    bad_inf = json.dumps({**good, "size": float("inf")}).encode()
    lambda_handler(kinesis_event([bad_nan, bad_inf, *payloads]), now=T0 + 1000)
    silver = read_parquet_prefix(aws["s3"], aws["bucket"], "silver/")
    assert silver.empty


def test_06_backfilled_history_is_not_flagged_late():
    """Backfill is old by definition; 'late' is for live records that arrived after the watermark."""
    rows = silver_rows([ev(ts=T0 - 30 * 24 * H, trade_id="1", source="alpaca", is_backfill=True)], now=T0)
    assert not rows.iloc[0]["late"]


def test_07_same_trade_from_two_live_sources_is_not_double_counted():
    """symbols.yaml streams equities from Finnhub AND Alpaca; stats must not sum both feeds."""
    from stockstream.aggregations.stats import hourly_stats

    rows = silver_rows([ev(source="finnhub"), ev(source="alpaca", trade_id="77")], now=T0 + 1000)
    stats = hourly_stats(rows)
    assert stats.iloc[0]["volume"] == 10.0


# ---------------------------------------------------------------- batch jobs


def _write_silver(s3, bucket, events, now, name):
    rows = silver_rows(events, now)
    for (dt, hour), part in rows.groupby(rows["event_ts"].map(
            lambda t: (datetime.fromtimestamp(t / 1000, UTC).strftime("%Y-%m-%d"),
                       datetime.fromtimestamp(t / 1000, UTC).strftime("%H")))):
        put_parquet(s3, bucket, f"silver/trades/dt={dt}/hour={hour}/{name}.parquet", part)


def test_08_rerunning_the_hourly_job_keeps_the_hourly_return(aws):
    """Each run recomputes a 3-hour window; the oldest hour in the window must keep its return."""
    from stockstream.aggregations.jobs import run_hourly

    s3, bucket = aws["s3"], aws["bucket"]
    events = [ev(price=100 + h, ts=T0 + h * H + 1000) for h in range(6)]
    _write_silver(s3, bucket, events, T0 + 7 * H, "a")
    base = datetime.fromtimestamp(T0 / 1000, UTC)
    for h in range(1, 7):
        run_hourly(s3, bucket, base + timedelta(hours=h, minutes=10))
    gold = read_parquet_prefix(s3, bucket, "gold/hourly_stats/").sort_values("hour_start")
    assert gold["return_vs_prev_close"].iloc[1:].notna().all()


def test_09_very_late_trade_reaches_hourly_gold(aws):
    """A trade arriving 5 hours late (past the 3-hour lookback) must still be reflected eventually."""
    from stockstream.aggregations.jobs import run_daily, run_hourly

    s3, bucket = aws["s3"], aws["bucket"]
    _write_silver(s3, bucket, [ev(size=10, ts=T0 + 1000)], T0 + 2000, "on_time")
    base = datetime.fromtimestamp(T0 / 1000, UTC)
    run_hourly(s3, bucket, base + timedelta(hours=1, minutes=10))
    _write_silver(s3, bucket, [ev(size=5, ts=T0 + 2000, trade_id="late")], T0 + 5 * H, "late")
    run_hourly(s3, bucket, base + timedelta(hours=5, minutes=10))
    run_daily(s3, bucket, base.date())  # the nightly job: the brief promises it rebuilds yesterday
    gold = read_parquet_prefix(s3, bucket, "gold/hourly_stats/dt=2026-10-29/hour=14/")
    assert gold.iloc[0]["volume"] == 15


def test_10_dq_bronze_vs_silver_has_no_false_alarm_across_midnight(aws):
    """Crypto trades every hour of every day; the reconciliation must compare the same set of trades."""
    from stockstream.dq.checks import run_dq

    s3, bucket = aws["s3"], aws["bucket"]
    day0 = datetime(2026, 10, 29, tzinfo=UTC)
    for d in range(2):
        for h in range(24):
            ts = int((day0 + timedelta(days=d, hours=h, minutes=1)).timestamp() * 1000)
            events = [ev("BTC-USD", 68000, 0.1, ts, "coinbase", "crypto", trade_id=f"{d}-{h}")]
            arrival = datetime.fromtimestamp(ts / 1000, UTC)
            put_jsonl_gz(s3, bucket, f"bronze/dt={arrival:%Y-%m-%d}/hour={arrival:%H}/f.json.gz", events)
            _write_silver(s3, bucket, events, ts + 1000, f"b{d}{h}")
    result = run_dq(s3, bucket, date(2026, 10, 29)).set_index("check")
    assert result.loc["bronze_vs_silver", "status"] == "PASS", result.loc["bronze_vs_silver", "detail"]


def test_11_dq_knows_about_half_days():
    """The day after Thanksgiving closes at 13:00 New York; no missing-hour warnings after the close."""
    from stockstream.common.config import load_universe
    from stockstream.dq.checks import missing_equity_hours

    day = date(2026, 11, 27)
    open_ms = int(datetime(2026, 11, 27, 14, 31, tzinfo=UTC).timestamp() * 1000)  # 9:31 New York
    events = [ev(s, ts=open_ms + h * H, trade_id=f"{s}{h}") for s in load_universe().equities for h in range(4)]
    result = missing_equity_hours(silver_rows(events, open_ms + 5 * H), day)
    assert result["status"] == "PASS", result["detail"]


def test_12b_bulk_deletes_respect_the_s3_limit_of_1000_keys():
    """Real S3 rejects DeleteObjects with more than 1000 keys (moto does not), so check the calls directly."""
    import io
    import runpy

    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(silver_rows([ev()], T0 + 1000), preserve_index=False), buf)
    payload = buf.getvalue()
    calls = []

    class FakeS3:
        def get_paginator(self, _):
            class P:
                def paginate(self, Bucket, Prefix):  # noqa: N803
                    if Prefix.endswith("hour=14/"):
                        yield {"Contents": [{"Key": f"{Prefix}b-{i}.parquet"} for i in range(1500)]}
            return P()

        def get_object(self, Bucket, Key):  # noqa: N803
            return {"Body": io.BytesIO(payload)}

        def put_object(self, **kw):
            pass

        def delete_objects(self, Bucket, Delete):  # noqa: N803
            calls.append(len(Delete["Objects"]))

    import boto3

    real = boto3.client
    boto3.client = lambda *a, **k: FakeS3()
    old_argv = sys.argv
    sys.argv = ["compaction", "--DATA_BUCKET", "b", "--RUN_DATE", "2026-10-29"]
    sys.path.insert(0, str(ROOT / "glue_jobs"))
    try:
        runpy.run_path(str(ROOT / "glue_jobs" / "compaction.py"), run_name="__main__")
    finally:
        boto3.client, sys.argv = real, old_argv
        sys.path.remove(str(ROOT / "glue_jobs"))
    assert calls and max(calls) <= 1000, calls


def test_12_compaction_handles_more_than_1000_files(aws, monkeypatch):
    s3, bucket = aws["s3"], aws["bucket"]
    df = silver_rows([ev()], T0 + 1000)
    for i in range(1005):
        put_parquet(s3, bucket, f"silver/trades/dt=2026-10-29/hour=14/batch-{i}.parquet", df)
    monkeypatch.setattr(sys, "argv", ["compaction", "--DATA_BUCKET", bucket, "--RUN_DATE", "2026-10-29"])
    monkeypatch.syspath_prepend(str(ROOT / "glue_jobs"))
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: s3)
    import runpy
    runpy.run_path(str(ROOT / "glue_jobs" / "compaction.py"), run_name="__main__")
    keys = list_keys(s3, bucket, "silver/trades/dt=2026-10-29/hour=14/")
    assert len(keys) == 1


def test_13_backfill_survives_sporadic_429s_across_many_pages():
    """Six 429s spread over a long day must not abort it: the retry budget is per request."""
    from stockstream.backfill.alpaca_backfill import RateLimiter, fetch_day

    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._body = code, body or {}

        def json(self):
            return self._body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    class Session:
        def __init__(self):
            self.page = 0

        def get(self, url, params, timeout):
            self.page += 1
            if self.page % 2 == 1 and self.page < 14:
                return Resp(429)
            n = self.page // 2
            return Resp(200, {"trades": [{"t": "2026-10-29T14:00:00Z", "p": 1, "s": 1, "i": n}],
                              "next_page_token": f"tok{n}" if n < 7 else None})

    limiter = RateLimiter(per_minute=10_000, sleep=lambda s: None)
    trades = fetch_day(Session(), limiter, "AAPL", date(2026, 10, 29))
    assert len(trades) == 7


# ---------------------------------------------------------------- deployment


def test_14_glue_code_runs_on_python_39():
    """Glue Python shell jobs run Python 3.9; everything the jobs import must be 3.9-compatible."""
    pytest.importorskip("vermin")
    targets = ["glue_jobs", "src/stockstream/aggregations", "src/stockstream/common", "src/stockstream/dq"]
    import shutil

    exe = shutil.which("vermin")
    if not exe:
        pytest.skip("vermin CLI not installed")
    out = subprocess.run([exe, "--no-tips", "--violations", "-t=3.9-",
                          "--eval-annotations", *targets],
                         cwd=ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stdout[-2000:]


def test_15_dst_switch_classifies_sessions_correctly():
    """US clocks fall back on Nov 1, 2026: 14:00 UTC is 10:00 before and 09:00 after."""
    from stockstream.common.markets import equity_session

    before = int(datetime(2026, 10, 30, 14, 0, tzinfo=UTC).timestamp() * 1000)
    after = int(datetime(2026, 11, 2, 14, 0, tzinfo=UTC).timestamp() * 1000)
    assert equity_session(before) == "regular"
    assert equity_session(after) == "extended"


def test_16_write_failure_then_retry_does_not_double_count(aws):
    """If S3 fails halfway and Lambda retries, gold must still count each trade once."""
    from stockstream.aggregations.stats import hourly_stats
    from stockstream.stream_processor import handler

    events = [ev(ts=T0 + i, trade_id=str(i)) for i in range(4)] + [ev(ts=T0 + H + i, trade_id=f"h{i}")
                                                                   for i in range(4)]
    real_put = handler.put_parquet
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:
            raise ClientError({"Error": {"Code": "SlowDown", "Message": "x"}}, "PutObject")
        return real_put(*a, **k)

    handler.put_parquet = flaky
    try:
        first = lambda_handler(kinesis_event(events), now=T0 + 2 * H)
        second = lambda_handler(kinesis_event(events), now=T0 + 2 * H)
    finally:
        handler.put_parquet = real_put
    assert first["batchItemFailures"] and not second["batchItemFailures"]
    silver = read_parquet_prefix(aws["s3"], aws["bucket"], "silver/")
    assert hourly_stats(silver)["volume"].sum() == 80


# ---------------------------------------------------------------- found in the 42-day full run


def test_17_identical_looking_trades_are_both_kept():
    """Two real Finnhub prints can share symbol, millisecond, price and size; both are volume."""
    from stockstream.aggregations.stats import hourly_stats
    from stockstream.producer import parsers
    from stockstream.producer.parsers import LookalikeCounter, parse_finnhub

    parsers.FINNHUB_LOOKALIKES = LookalikeCounter()
    msg = {"type": "trade", "data": [{"s": "AAPL", "p": 230.0, "v": 100, "t": T0},
                                     {"s": "AAPL", "p": 230.0, "v": 100, "t": T0}]}
    events = parse_finnhub(msg, T0 + 10)
    assert len({e["event_id"] for e in events}) == 2
    assert hourly_stats(silver_rows(events, T0 + 1000)).iloc[0]["volume"] == 200
    # ...and the first print keeps the original id format, so existing data still dedupes
    from stockstream.common.events import make_event_id
    assert events[0]["event_id"] == make_event_id("finnhub", "AAPL", None, T0, 230.0, 100.0)


def test_18_top_movers_includes_stocks_on_a_weekend(tmp_path):
    """On a Sunday only crypto has a bar for 'today'; stocks must still appear with Friday's move."""
    import duckdb

    rows = pd.DataFrame({"symbol": ["AAPL", "BTC-USD"], "asset_class": ["equity", "crypto"],
                         "trade_date": [date(2026, 10, 23), date(2026, 10, 25)],
                         "close": [230.0, 68000.0], "return_1d": [0.05, 0.01]})
    con = duckdb.connect()
    con.execute("CREATE SCHEMA stockstream")
    con.register("daily_df", rows)
    con.execute("CREATE VIEW stockstream.daily_stats AS SELECT * FROM daily_df")
    out = con.sql((ROOT / "sql/analytics/01_top_movers.sql").read_text()).df()
    assert set(out["symbol"]) == {"AAPL", "BTC-USD"}


def test_19_mondays_first_hour_has_a_return(aws):
    """Found by counting NaN returns after the 42-day run: 183 = 33 first-ever hours + 30 stocks x 5 Mondays.
    Monday 04:00 New York must compare with Friday's last close, 56 hours earlier."""
    from stockstream.aggregations.jobs import run_hourly

    s3, bucket = aws["s3"], aws["bucket"]
    fri = int(datetime(2026, 10, 23, 23, 30, tzinfo=UTC).timestamp() * 1000)   # 19:30 New York
    mon = int(datetime(2026, 10, 26, 8, 30, tzinfo=UTC).timestamp() * 1000)    # 04:30 New York
    sat = [ev("BTC-USD", 68000, 0.1, fri + h * H, "coinbase", "crypto", trade_id=f"c{h}") for h in range(1, 56)]
    _write_silver(s3, bucket, [ev(price=100.0, ts=fri, trade_id="f"), *sat], mon, "a")  # now=mon: no future_ts
    for h in range(0, 58):
        run_hourly(s3, bucket, datetime.fromtimestamp(fri / 1000, UTC) + timedelta(hours=h + 1, minutes=10), 1)
    _write_silver(s3, bucket, [ev(price=110.0, ts=mon, trade_id="m")], mon + 1000, "b")
    run_hourly(s3, bucket, datetime.fromtimestamp(mon / 1000, UTC) + timedelta(hours=1, minutes=10), 1)
    gold = read_parquet_prefix(s3, bucket, "gold/hourly_stats/dt=2026-10-26/hour=08/")
    assert gold.iloc[0]["return_vs_prev_close"] == pytest.approx(0.10)


# ---------------------------------------------------------------- found in the final packaging review


def test_20_glue_wheel_carries_its_config_and_contract(tmp_path):
    """The Glue jobs import stockstream from a wheel, outside the repo: the wheel must ship its data files."""
    import zipfile

    subprocess.run([sys.executable, "-m", "pip", "wheel", "-q", "--no-deps", "-w", str(tmp_path), str(ROOT)],
                   check=True, capture_output=True)
    names = zipfile.ZipFile(next(tmp_path.glob("*.whl"))).namelist()
    assert "stockstream/config/symbols.yaml" in names
    assert "stockstream/contracts/trade_event.v1.json" in names


def test_21_sns_topic_policy_still_lets_alarms_and_budgets_publish():
    """A custom SNS topic policy replaces the default one; it must allow every service that publishes."""
    hcl2 = pytest.importorskip("hcl2")
    with open(ROOT / "infra" / "monitoring.tf") as fh:
        tf = hcl2.load(fh)
    docs = [blk['"aws_iam_policy_document"'] for blk in tf["data"] if '"aws_iam_policy_document"' in blk]
    policy = next(d['"sns_events"'] for d in docs if '"sns_events"' in d)
    principals = {i.strip('"') for st in policy["statement"] for pr in st["principals"] for i in pr["identifiers"]}
    assert {"events.amazonaws.com", "cloudwatch.amazonaws.com"} <= principals, principals


def test_22_glue_job_arguments_are_never_empty_strings():
    """Glue rejects empty default argument values, so optional arguments need a real sentinel value."""
    text = (ROOT / "infra" / "glue.tf").read_text()
    assert '= ""' not in text.split("locals {")[1].split("}\n}")[0]
