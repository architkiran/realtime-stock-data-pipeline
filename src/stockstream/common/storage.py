"""Small S3 helpers for the lake layout (bronze JSON lines, silver/gold Parquet)."""

from __future__ import annotations

import gzip
import io
import json
from collections.abc import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def put_jsonl_gz(s3, bucket: str, key: str, records: Iterable[dict]) -> int:
    buf = io.BytesIO()
    n = 0
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            gz.write((json.dumps(rec) + "\n").encode())
            n += 1
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return n


def read_jsonl_gz(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return [json.loads(line) for line in gzip.decompress(body).splitlines() if line.strip()]


def put_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def read_parquet(s3, bucket: str, key: str) -> pd.DataFrame:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body)).to_pandas()


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def read_parquet_prefix(s3, bucket: str, prefix: str) -> pd.DataFrame:
    frames = [read_parquet(s3, bucket, k) for k in list_keys(s3, bucket, prefix) if k.endswith(".parquet")]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    keys = list_keys(s3, bucket, prefix)
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]})
    return len(keys)
