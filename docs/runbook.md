# Runbook

What to check first when an alarm emails you. All alarms come from `infra/monitoring.tf`.

## iterator-age (Lambda is behind the stream)
1. CloudWatch dashboard: did `IncomingRecords` spike (market open) or did Lambda `Duration` jump?
2. Lambda logs: look for repeated `write failed` lines (S3 throttling) or timeouts.
3. If it is sustained volume: raise `parallelization_factor` on the event source mapping, or add a shard.

## lambda-errors
1. Logs Insights: `fields @timestamp, msg, exc | filter level = "ERROR" | sort @timestamp desc`.
2. A code bug: fix, deploy, then `make replay DATE=<day>` to rebuild silver from bronze.

## dlq-depth (a batch gave up after 3 retries)
1. Read one message from the SQS DLQ: it holds the shard and sequence range, not the data.
2. The data is still in bronze (Firehose is independent). Fix the cause, then `make replay DATE=<day>`.

## crypto-freshness (no Coinbase trade for 5 minutes)
1. ECS console: is the producer task running? `make start` if desired count is 0.
2. Producer logs: `disconnected` lines in a loop mean the websocket is refusing; check the subscribe message.
3. `BadFrames` rising means the source changed its message format: compare a logged frame with the parser.

## write-throttled (producer hitting the shard limit)
1. Check `IncomingRecords` against 1,000 records/s per shard.
2. Add a shard (`shard_count` in `infra/kinesis.tf`); the producer already retries and requeues meanwhile.

## Glue job failed (EventBridge rule)
1. CloudWatch log group `/aws-glue/python-jobs/error`.
2. Every job is safe to rerun: Glue console, Run job, with `--RUN_DATE` set for daily and compaction.

## DQ result FAIL or WARN (`gold.dq_results`, dashboard panel)
- `bronze_vs_silver` FAIL: count quarantine reasons for the day; a new reason usually means a source format change.
- `missing_equity_hours` WARN: check for an unlisted holiday or early close in `common/markets.py`, then for a websocket gap.
- `ohlc_sanity` FAIL: a stats bug; rerun `daily-stats` for the day after fixing.
