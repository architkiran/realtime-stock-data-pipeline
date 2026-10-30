"""Batch path into silver: process bronze files with the same rules the Lambda uses.

Used for (1) loading backfill files into silver and (2) `make replay DATE=...`
to rebuild silver after a bug fix, since bronze is the untouched source of truth.
"""

from __future__ import annotations

import argparse
import uuid

from stockstream.common.events import now_ms
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.common.storage import list_keys, read_jsonl_gz
from stockstream.stream_processor.handler import classify, write_outputs

log = get_logger(__name__)

CHUNK = 50_000


def replay_prefix(s3, bucket: str, prefix: str) -> dict:
    totals = {"files": 0, "valid": 0, "quarantined": 0, "duplicates": 0}
    for key in list_keys(s3, bucket, prefix):
        if not key.endswith(".json.gz"):
            continue
        records = read_jsonl_gz(s3, bucket, key)
        for i in range(0, len(records), CHUNK):
            chunk = [(f"{key}#{i + j}", r) for j, r in enumerate(records[i : i + CHUNK])]
            valid, quarantined, dupes = classify(chunk, now_ms())
            write_outputs(valid, quarantined, bucket, f"replay-{uuid.uuid4().hex[:12]}")
            totals["valid"] += len(valid)
            totals["quarantined"] += len(quarantined)
            totals["duplicates"] += dupes
        totals["files"] += 1
    log_fields(log, "replay done", prefix=prefix, **totals)
    return totals


def main() -> None:
    import boto3

    from stockstream.common.config import settings

    p = argparse.ArgumentParser()
    p.add_argument("--prefix", required=True, help="e.g. bronze/backfill/dt=2026-09-15/")
    args = p.parse_args()
    cfg = settings()
    print(replay_prefix(boto3.client("s3", region_name=cfg.region), cfg.bucket, args.prefix))


if __name__ == "__main__":
    main()
