"""Run the whole pipeline on a laptop against in-memory AWS fakes. No AWS account, no bill.

Kinesis and S3 are small in-memory fakes (fake_kinesis.py, fake_s3.py) because moto slows down
quadratically on month-long simulations; pass --moto to use moto's S3 instead.

    python -m stockstream.local.run_local --start 2026-09-21 --days 30 --scale 1.0 --out local_output

Flow per simulated 5-minute step:
  simulator -> native wire messages -> parsers -> KinesisSink -> (fake) Kinesis
  Kinesis -> "Firehose" copy into bronze   (arrival-time partitions, untouched)
  Kinesis -> lambda_handler               (silver + quarantine)
Then: the hourly job every hour, the daily job and DQ checks every day, and
DuckDB over the gold Parquet files stands in for Athena (see query_local.py).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

UTC = timezone.utc  # datetime.UTC needs Python 3.11; Glue Python shell runs 3.9

STEP_MIN = 5
STREAM = "stockstream-trades"


def _setup_env() -> None:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("DATA_BUCKET", "stockstream-local")


@dataclass
class LocalPipeline:
    s3: object
    kin: object
    bucket: str
    sims: list
    rng: np.random.Generator
    iterator: str = ""
    counts: dict = field(default_factory=lambda: {"produced": 0, "lambda_batches": 0})

    def __post_init__(self) -> None:
        from stockstream.producer.kinesis_sink import KinesisSink
        from stockstream.stream_processor import handler

        self.s3.create_bucket(Bucket=self.bucket)
        self.kin.create_stream(StreamName=STREAM, ShardCount=1)
        handler._s3 = self.s3
        shard = self.kin.describe_stream(StreamName=STREAM)["StreamDescription"]["Shards"][0]["ShardId"]
        self.iterator = self.kin.get_shard_iterator(
            StreamName=STREAM, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        self.sink = KinesisSink(self.kin, STREAM, sleep=lambda s: None)

    def produce(self, step_ms: int) -> None:
        from stockstream.common.markets import equity_session
        from stockstream.local.simulator import to_wire
        from stockstream.producer.parsers import PARSERS

        ingest = step_ms + STEP_MIN * 60_000
        for sim in self.sims:
            if sim.source != "coinbase":
                session = equity_session(step_ms)
                if session == "off_hours":
                    continue
                trades = sim.step(self.rng, step_ms, STEP_MIN)
                if session == "extended":
                    trades = trades[::10]
            else:
                trades = sim.step(self.rng, step_ms, STEP_MIN)
            for msg in to_wire(sim.source, sim.symbol, trades):
                events = PARSERS[sim.source](json.loads(json.dumps(msg)), ingest)
                self.sink.add(events)
                self.counts["produced"] += len(events)
        self.sink.flush()

    def consume(self, now: int, step_label: str) -> None:
        """Firehose copy to bronze + Lambda invocation for everything new on the stream."""
        from stockstream.common.storage import put_jsonl_gz
        from stockstream.stream_processor import handler

        while True:
            resp = self.kin.get_records(ShardIterator=self.iterator, Limit=10_000)
            self.iterator = resp["NextShardIterator"]
            recs = resp["Records"]
            if not recs:
                return
            arrival = datetime.fromtimestamp(now / 1000, tz=UTC)
            seq_tail = recs[0]["SequenceNumber"][-10:]
            key = f"bronze/dt={arrival:%Y-%m-%d}/hour={arrival:%H}/fh-{step_label}-{seq_tail}.json.gz"
            put_jsonl_gz(self.s3, self.bucket, key, [json.loads(r["Data"]) for r in recs])
            event = {"Records": [{"kinesis": {"sequenceNumber": r["SequenceNumber"],
                                              "data": base64.b64encode(r["Data"]).decode()}} for r in recs]}
            handler.lambda_handler(event, None, now=now)
            self.counts["lambda_batches"] += 1

    def simulate_day(self, day: date) -> None:
        from stockstream.aggregations.jobs import run_hourly

        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        for i in range(24 * 60 // STEP_MIN):
            step_start = start + timedelta(minutes=i * STEP_MIN)
            step_ms = int(step_start.timestamp() * 1000)
            self.produce(step_ms)
            self.consume(step_ms + STEP_MIN * 60_000 + 2_000, f"{day:%Y%m%d}-{i:03d}")
            if (i + 1) % (60 // STEP_MIN) == 0:  # the hourly job fires at :10 past each hour
                run_hourly(self.s3, self.bucket, step_start + timedelta(minutes=STEP_MIN + 10))


def run(first_day: date, days: int, scale: float, out: Path, seed: int = 7, use_moto: bool = False) -> dict:
    """use_moto=True runs against moto's S3 (closer to AWS, but slow once the lake is large)."""
    _setup_env()
    import contextlib

    import boto3
    from moto import mock_aws

    from stockstream.aggregations.jobs import run_daily, run_hourly
    from stockstream.common.storage import list_keys
    from stockstream.dq.checks import run_dq
    from stockstream.local.fake_kinesis import FakeKinesis
    from stockstream.local.simulator import default_sims

    bucket = os.environ["DATA_BUCKET"]
    t0 = time.time()
    daily_rows, dq = 0, None
    from stockstream.local.fake_s3 import FakeS3

    with mock_aws() if use_moto else contextlib.nullcontext():
        s3 = boto3.client("s3", region_name="us-east-1") if use_moto else FakeS3()
        pipe = LocalPipeline(s3, FakeKinesis(), bucket,
                             default_sims(scale), np.random.default_rng(seed))
        for d in range(days):
            day = first_day + timedelta(days=d)
            pipe.simulate_day(day)
            # the daily job runs the next morning, after the hourly job has covered the last hours
            for h in range(1, 4):
                run_hourly(s3, bucket, datetime(day.year, day.month, day.day, tzinfo=UTC)
                           + timedelta(days=1, hours=h, minutes=10))
            daily_rows += run_daily(s3, bucket, day)
            dq = run_dq(s3, bucket, day)
            print(f"simulated {day}: trades so far {pipe.counts['produced']:,}", flush=True)

        out.mkdir(parents=True, exist_ok=True)
        for key in list_keys(s3, bucket, ""):
            if key.startswith(("silver/", "gold/", "quarantine/")):
                dest = out / key
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    return {**pipe.counts, "daily_rows": daily_rows, "sink": pipe.sink.stats.__dict__,
            "seconds": round(time.time() - t0, 1),
            "last_day_dq": dq[["check", "status", "detail"]].to_dict("records") if dq is not None else []}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 26))
    p.add_argument("--days", type=int, default=1)
    p.add_argument("--scale", type=float, default=1.0)
    p.add_argument("--out", type=Path, default=Path("local_output"))
    p.add_argument("--moto", action="store_true", help="use moto S3 instead of the in-memory fake")
    args = p.parse_args()
    result = run(args.start, args.days, args.scale, args.out, use_moto=args.moto)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
