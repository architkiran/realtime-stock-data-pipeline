"""Glue job: build gold.daily_stats for yesterday (New York date), then run the DQ checks."""

from datetime import date, datetime, timedelta

import boto3
from _common import args

from stockstream.aggregations.jobs import run_daily
from stockstream.common.markets import NY
from stockstream.dq.checks import run_dq

a = args("DATA_BUCKET", "RUN_DATE")
day = date.fromisoformat(a["RUN_DATE"]) if a.get("RUN_DATE") else datetime.now(NY).date() - timedelta(days=1)
s3 = boto3.client("s3")
run_daily(s3, a["DATA_BUCKET"], day)
print(run_dq(s3, a["DATA_BUCKET"], day).to_string())
