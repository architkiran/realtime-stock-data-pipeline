# Real-time stock and crypto trade pipeline on AWS

A serverless streaming pipeline that collects live trades from three free market-data APIs, validates them against a written data contract in near real time, lands them in an S3 data lake (bronze, silver, gold), computes hourly and daily price statistics, and serves everything through Athena and a Streamlit dashboard.

The whole pipeline also runs on a laptop with no AWS account (`make local`), which is how it is tested. After the first complete build it went through a deliberate break-it phase: **14 bugs found, documented, fixed, and pinned by regression tests** ([bug log](docs/bug-log.md), [fix log](docs/fix-log.md)).

**Stack:** Python, Kinesis Data Streams, Data Firehose, Lambda, S3, Glue, Athena, ECS Fargate, EventBridge Scheduler, CloudWatch, Secrets Manager, Terraform, GitHub Actions, pytest, moto, DuckDB, Streamlit.

---

## Architecture

```text
 Finnhub ws ─┐                                         ┌─> Data Firehose ──> S3 bronze  (raw JSON, by arrival time)
 Coinbase ws ┼─> Producer (ECS Fargate) ──> Kinesis ───┤
 Alpaca ws ──┘   normalize, event_id,       1 shard    └─> Lambda ──┬──> S3 silver    (validated Parquet, by event time)
                 PutRecords + retries                   validate,  └──> S3 quarantine (rejects + reason + payload)
 Alpaca REST ──> Backfill loader ──> bronze/backfill ──> replay ──> silver           dedupe, flag

 S3 silver ──> Glue: hourly stats (:10 every hour, 3 h lookback)  ──> S3 gold ──> Athena ──> Streamlit
               Glue: daily stats + hourly rebuild + DQ (00:30 ET)
               Glue: compaction (02:00 ET)
```

Design choices worth reading: [ADR 001](docs/decisions/001-medallion-and-partition-overwrite.md) (why gold is rebuilt by partition instead of Iceberg MERGE), [ADR 002](docs/decisions/002-one-source-per-bucket.md) (why stats use one feed per symbol), [ADR 003](docs/decisions/003-glue-python-shell.md) (Glue Python shell and its expiry date).

## Data sources

| Source | Used for | Free tier |
|---|---|---|
| [Finnhub](https://finnhub.io/docs/api) websocket | Live US stock trades (primary equity feed) | 50 websocket symbols, 60 REST calls/min |
| [Alpaca Market Data](https://docs.alpaca.markets/us/docs/about-market-data-api), Basic plan | Historical backfill (trades since 2016), optional live IEX feed | 30 websocket symbols, IEX only |
| [Coinbase Advanced Trade](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints.md) `market_trades` | 24/7 crypto trades (keeps the pipeline busy nights and weekends) | Public, no key needed |

Universe: 30 equities and ETFs across sectors plus BTC-USD, ETH-USD, SOL-USD (`config/symbols.yaml`).

Every source is normalized into one contract, `trade_event` v1 ([contracts/trade_event.v1.json](contracts/trade_event.v1.json)). `event_id` is a SHA-256 of source, symbol, and trade id (or timestamp, price, size when a source has no trade id), so a replayed record gets the same key and can be deduplicated.

## Data quality

| Check | Where | On failure |
|---|---|---|
| Contract (types, enums, required fields, finite positive numbers) | Lambda | quarantine `schema` |
| Symbol in the configured universe | Lambda | quarantine `unknown_symbol` |
| Event time more than 5 s in the future | Lambda | quarantine `future_ts` |
| Live record older than the 2 h watermark | Lambda | keep, `late=true` |
| Price jump over 20% within a batch | Lambda | keep, `suspect_price=true` |
| Duplicate `event_id` | Lambda (batch) and every stats job | dropped |
| Bronze vs silver + quarantine, same event-time day, within 1% | nightly DQ | FAIL |
| Every equity trades in every full session hour (half-day aware) | nightly DQ | WARN |
| Hourly OHLC sanity (`low <= open, close <= high`, volume > 0) | nightly DQ | FAIL |
| Freshness: no crypto trade for 5 minutes | CloudWatch alarm | email |

Rejected rows are never deleted: quarantine keeps the original payload so it can be replayed after a fix.

## Statistics

Hourly (`gold/hourly_stats`): OHLC, volume, trade count, VWAP, realized volatility from 1-minute log returns, and return vs the previous hour's close. Daily (`gold/daily_stats`): OHLCV and VWAP (regular session only for equities, New York date; UTC date for crypto), 1-day return, 20- and 50-day moving averages, annualized 20-day volatility, and a volume z-score against the prior 20 days. All formulas are documented at the top of [`aggregations/stats.py`](src/stockstream/aggregations/stats.py).

## Repository layout

```text
config/symbols.yaml              symbol universe and which source streams what
contracts/trade_event.v1.json    the data contract
src/stockstream/
  producer/                      websocket readers, parsers, buffered Kinesis sink, Fargate entry point
  stream_processor/              Lambda handler; replay from bronze (backfill, rebuilds)
  aggregations/                  pure-pandas stats + the hourly/daily jobs
  dq/                            nightly data-quality checks
  backfill/                      resumable, rate-limited Alpaca history loader
  local/                         simulator, in-memory AWS fakes, DuckDB query runner
  dashboard/                     Streamlit app (Athena or local lake)
glue_jobs/                       thin Glue wrappers around the job functions
sql/                             Athena DDL (partition projection), views, analytics queries
infra/                           Terraform for everything above, alarms, budget, dashboard
tests/                           unit, integration (moto), end-to-end, chaos (break-it)
docs/                            bug log, fix log, ADRs, runbook
```

## Run it locally (no AWS account)

```bash
python -m venv .venv && source .venv/bin/activate
make install            # pip install -e ".[dev]"
make test               # 32 tests: unit, integration, end-to-end, chaos
make local DAYS=5 SCALE=0.5    # simulate 5 days of trades through the whole pipeline
make query              # run sql/analytics/*.sql with DuckDB over the local lake
pip install ".[dashboard]" && LAKE_PATH=local_output streamlit run src/stockstream/dashboard/app.py
```

The simulator speaks each API's real wire format (Finnhub, Alpaca, and Coinbase message shapes), so the real parsers run end to end. Prices follow geometric Brownian motion; trade arrivals are Poisson; equities trade only in session hours.

## Deploy to AWS

Prerequisites: an AWS account (new accounts get up to $200 in [Free Tier credits](https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/); Kinesis is not in the always-free tier), Terraform 1.6+, Docker, and free API keys from Finnhub and Alpaca.

```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars   # set alert_email
make deploy                                    # builds the Lambda zip and Glue wheel, then terraform apply
aws secretsmanager put-secret-value --secret-id stockstream/api-keys \
  --secret-string '{"finnhub_key":"...","alpaca_key":"...","alpaca_secret":"..."}'
make push-producer                             # build and push the producer image to ECR
make start                                     # 1 Fargate task: trades start flowing
make backfill START=2026-09-01 END=2026-09-30  # optional: a month of Alpaca history
# run sql/ddl/*.sql and sql/views/views.sql in the Athena console (workgroup "stockstream"), replacing ${bucket}
make stop                                      # producer off when you are not using it
make destroy                                   # everything except the data bucket
```

Confirm the SNS subscription email, or you won't get alarms. Before deploying, check the current version of the AWS SDK for pandas Lambda layer (`pandas_layer_arn` in `infra/variables.tf`).

**Cost guardrails:** a $20/month AWS Budget with email alerts, 1 provisioned shard (about $11/month), the producer at 0 tasks unless started, Athena's 1 GB per-query scan limit, lifecycle rules on bronze and quarantine, and 1/16-DPU Glue jobs.

**What has and hasn't been verified:** everything in this repo was built and tested without access to AWS or the live APIs. The pipeline logic is tested end to end against moto and in-memory fakes, and the batch jobs are tested on Python 3.9 with the exact library versions Glue installs. The Terraform has not yet been applied to a real account (CI runs `terraform validate`), and the websocket URLs and subscribe messages follow each provider's docs but have not been exercised live. Expect a round of fixes on the first real deploy; record them in the bug log.

## Sample results (local simulation)

RESULTS_PLACEHOLDER

## Break it, then fix it

After the first complete build, an adversarial suite (`tests/chaos/test_break_it.py`) attacked every stage: malformed frames, dropped sockets, full-call throttling, NaN prices, late and duplicate data, two feeds for one symbol, midnight boundaries, half days, S3 API limits, rate-limit storms, and the Glue runtime. 14 of 18 tests failed. Highlights:

- **Critical:** equities streamed from two feeds were counted twice in every volume and VWAP (BUG-07).
- **Critical:** every Glue job would have failed on its first AWS run, because Glue's Python shell is 3.9 and the code used 3.11-only APIs; ruff's auto-upgrade rule had introduced them (BUG-14).
- **High:** a throttled `PutRecords` call lost the whole batch (BUG-03); one bad websocket frame stopped a source (BUG-01, BUG-02); NaN prices passed the contract because `NaN <= 0` is false (BUG-05).

Each bug's symptom, root cause, impact, and how it could have been spotted are in the [bug log](docs/bug-log.md); each fix and the alternatives considered are in the [fix log](docs/fix-log.md).

## What I learned

- At-least-once delivery shapes everything: deterministic ids, idempotent writes, and dedupe at every read.
- Event time and processing time must never be mixed in one comparison (BUG-10 was exactly that).
- Mocks are not the service: moto skipped the 1,000-key S3 limit and has quadratic Kinesis puts.
- Test on the runtime you deploy to; a linter's auto-fix broke the Glue runtime.

## Future work

- Move the batch jobs off Glue Python shell (boto3 ends Python 3.9 support in April 2026) to Glue 5.0 Spark or a scheduled container Lambda.
- Apache Iceberg for gold when there are concurrent writers or time-travel needs.
- Managed Service for Apache Flink for true real-time windows instead of hourly batches.
- Replace the hand-maintained holiday list with `exchange_calendars`.
- A news-sentiment stream joined to price moves.
