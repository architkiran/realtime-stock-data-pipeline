"""Lambda consumer for the Kinesis stream.

Per batch: decode, validate against the contract, apply quality rules, drop
in-batch duplicates, then write valid rows to silver (Parquet, partitioned by
event-time dt/hour) and rejects to quarantine (JSON lines with the reason and
the original payload). Returns batchItemFailures so Lambda retries only the
records that could not be written.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from collections import defaultdict
from typing import Any

import boto3
import pandas as pd

from stockstream.common.config import load_universe, settings
from stockstream.common.events import now_ms, schema_errors
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.common.markets import equity_session, utc_partition
from stockstream.common.storage import put_jsonl_gz, put_parquet

log = get_logger(__name__)

SILVER_COLUMNS = [
    "event_id", "source", "asset_class", "symbol", "price", "size", "event_ts", "ingest_ts",
    "trade_id", "venue", "is_backfill", "session", "late", "suspect_price", "processed_ts",
]

_s3 = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=settings().region)
    return _s3


def decode_record(record: dict) -> tuple[str, Any]:
    seq = record["kinesis"]["sequenceNumber"]
    raw = base64.b64decode(record["kinesis"]["data"])
    return seq, json.loads(raw)


def classify(events: list[tuple[str, dict]], now: int) -> tuple[list[dict], list[dict], int]:
    """Split events into (valid rows, quarantined rows, duplicates dropped)."""
    cfg, universe = settings(), load_universe()
    watermark = now - cfg.late_watermark_minutes * 60_000
    valid, quarantined, seen = [], [], set()
    last_price: dict[str, float] = {}
    duplicates = 0

    for seq, event in events:
        errors = schema_errors(event)
        reason = "schema" if errors else None
        if not reason and event["symbol"] not in universe.all_symbols:
            reason, errors = "unknown_symbol", [event["symbol"]]
        if not reason and event["event_ts"] > now + cfg.future_tolerance_ms:
            reason, errors = "future_ts", [str(event["event_ts"])]
        if reason:
            quarantined.append({"seq": seq, "reason": reason, "errors": errors, "payload": event})
            continue

        if event["event_id"] in seen:
            duplicates += 1
            continue
        seen.add(event["event_id"])

        prev = last_price.get(event["symbol"])
        suspect = prev is not None and abs(event["price"] - prev) / prev > cfg.suspect_jump_pct
        last_price[event["symbol"]] = event["price"]

        row = {k: event.get(k) for k in SILVER_COLUMNS if k in event}
        row["session"] = equity_session(event["event_ts"]) if event["asset_class"] == "equity" else "continuous"
        row["late"] = event["event_ts"] < watermark
        row["suspect_price"] = suspect
        row["processed_ts"] = now
        valid.append(row)
    return valid, quarantined, duplicates


def write_outputs(valid: list[dict], quarantined: list[dict], bucket: str, batch_id: str) -> None:
    s3 = s3_client()
    by_partition: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in valid:
        by_partition[utc_partition(row["event_ts"])].append(row)
    for (dt, hour), rows in by_partition.items():
        df = pd.DataFrame(rows, columns=SILVER_COLUMNS)
        put_parquet(s3, bucket, f"silver/trades/dt={dt}/hour={hour}/{batch_id}.parquet", df)
    if quarantined:
        dt, hour = utc_partition(now_ms())
        put_jsonl_gz(s3, bucket, f"quarantine/dt={dt}/hour={hour}/{batch_id}.json.gz", quarantined)


def lambda_handler(event: dict, context: Any = None, *, now: int | None = None) -> dict:
    """`now` is only passed by the local simulator and tests; Lambda uses the wall clock."""
    bucket = os.environ.get("DATA_BUCKET", settings().bucket)
    records = event.get("Records", [])
    decoded, undecodable = [], []
    for rec in records:
        try:
            decoded.append(decode_record(rec))
        except (ValueError, KeyError) as exc:
            undecodable.append({"seq": rec.get("kinesis", {}).get("sequenceNumber"), "reason": "undecodable",
                                "errors": [str(exc)], "payload": rec.get("kinesis", {}).get("data")})

    now = now if now is not None else now_ms()
    valid, quarantined, duplicates = classify(decoded, now)
    quarantined.extend(undecodable)

    batch_id = f"{now}-{uuid.uuid4().hex[:8]}"
    try:
        write_outputs(valid, quarantined, bucket, batch_id)
    except Exception:
        log.exception("write failed; asking Lambda to retry the batch")
        return {"batchItemFailures": [{"itemIdentifier": r["kinesis"]["sequenceNumber"]} for r in records]}

    log_fields(log, "batch processed", records=len(records), valid=len(valid),
               quarantined=len(quarantined), duplicates=duplicates,
               late=sum(r["late"] for r in valid), batch_id=batch_id)
    return {"batchItemFailures": []}
