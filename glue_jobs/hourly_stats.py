"""Glue job: recompute the last 3 closed hours of gold.hourly_stats (runs at :10 every hour)."""

from datetime import UTC, datetime

import boto3
from _common import args

from stockstream.aggregations.jobs import run_hourly

a = args("DATA_BUCKET", "LOOKBACK_HOURS")
run_hourly(boto3.client("s3"), a["DATA_BUCKET"], datetime.now(UTC), int(a["LOOKBACK_HOURS"]))
