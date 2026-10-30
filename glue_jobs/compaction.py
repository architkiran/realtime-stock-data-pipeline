"""Glue job: merge yesterday's small silver files into one file per hour partition.

Lambda writes one Parquet file per batch, so an hour can hold hundreds of tiny
files. Athena pays a per-file overhead, so compacting cuts both latency and cost.
The rewrite is safe to rerun: it writes the merged file first, then deletes the
originals it read.
"""

from datetime import datetime, timedelta

import boto3
import pandas as pd
from _common import args

from stockstream.common.markets import NY
from stockstream.common.storage import list_keys, put_parquet, read_parquet

a = args("DATA_BUCKET", "RUN_DATE")
bucket, s3 = a["DATA_BUCKET"], boto3.client("s3")
day = a.get("RUN_DATE") or (datetime.now(NY).date() - timedelta(days=1)).isoformat()
for hour in range(24):
    prefix = f"silver/trades/dt={day}/hour={hour:02d}/"
    keys = [k for k in list_keys(s3, bucket, prefix) if k.endswith(".parquet") and "compacted" not in k]
    if len(keys) < 2:
        continue
    df = pd.concat([read_parquet(s3, bucket, k) for k in keys], ignore_index=True)
    df = df.sort_values("processed_ts").drop_duplicates("event_id")
    put_parquet(s3, bucket, prefix + f"compacted-{datetime.now():%Y%m%d%H%M%S}.parquet", df)
    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]})
    print(f"{prefix}: {len(keys)} files -> 1 ({len(df)} rows)")
