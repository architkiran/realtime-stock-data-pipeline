# Fix log

BUG-01 to BUG-14 are fixed in commit `0d674d6`, and BUG-15 to BUG-21 (found in the 42-day run and the final review) in the commit after it, all on branch `fix/break-it-bugs`. Each fix is pinned by the chaos test that first exposed the bug, so a regression fails CI.

**After the fixes:** 38 of 38 tests pass on Python 3.13, and the batch-job tests (stats, jobs, DQ, compaction) also pass on Python 3.9 with the exact library versions the Glue jobs install.

| ID | Fix in one line | Files |
|---|---|---|
| FIX-01 | Parsers wrap each trade, skip and count bad ones | `producer/parsers.py` |
| FIX-02 | Reader catches any per-frame error, counts it, keeps reading | `producer/ws_client.py` |
| FIX-03 | Sink retries thrown throttling errors and requeues instead of dropping | `producer/kinesis_sink.py` |
| FIX-04 | Producer flushes in a worker thread (`flush_async`) with locks | `producer/kinesis_sink.py`, `producer/main.py` |
| FIX-05 | Contract check rejects non-finite numbers explicitly | `common/events.py` |
| FIX-06 | `late` is only set for live records | `stream_processor/handler.py` |
| FIX-07 | Stats use one source per symbol and bucket, by priority | `aggregations/stats.py` |
| FIX-08 | Hourly job seeds the window's first hour with the previous gold close | `aggregations/jobs.py`, `aggregations/stats.py` |
| FIX-09 | Nightly job rebuilds all 24 hourly partitions of the day | `aggregations/jobs.py` |
| FIX-10 | Reconciliation filters both sides to the same event-time day | `dq/checks.py` |
| FIX-11 | Early-close calendar drives sessions and expected hours | `common/markets.py`, `dq/checks.py` |
| FIX-12 | All bulk deletes go through a helper that chunks at 1000 keys | `common/storage.py`, `glue_jobs/compaction.py` |
| FIX-13 | Retry counter resets after every successful page | `backfill/alpaca_backfill.py` |
| FIX-14 | 3.9-compatible code, pinned Glue libraries, a Python 3.9 CI job | many, `infra/glue.tf`, `.github/workflows/ci.yml` |
| FIX-15 | Look-alike trades get an occurrence number in their id | `common/events.py`, `producer/parsers.py` |
| FIX-16 | Movers use each symbol's latest trading day | `sql/analytics/01_top_movers.sql`, `sql/views/views.sql`, dashboard |
| FIX-17 | `git rm --cached` the `.pyc` files | repo |
| FIX-18 | Baseline search continues until every symbol in the window is found | `aggregations/jobs.py` |
| FIX-19 | Config and contract live inside the package; repo-root paths are symlinks | `src/stockstream/config/`, `src/stockstream/contracts/`, `pyproject.toml` |
| FIX-20 | Topic policy allows CloudWatch and EventBridge, scoped to this account | `infra/monitoring.tf` |
| FIX-21 | Optional Glue arguments default to `auto` | `infra/glue.tf`, `glue_jobs/` |

---

## FIX-01: parsers skip and count malformed trades

Each source's per-trade logic moved into its own function (`_finnhub_trade`, `_alpaca_trade`, `_coinbase_trade`). The loop calls it inside `try/except (KeyError, TypeError, ValueError, AttributeError)`. A bad trade increments `BAD_TRADES[source]` and is logged (the first 20, then every 1,000th, so a broken feed can't flood CloudWatch). The good trades in the same message are kept. The producer publishes the count as the `BadFrames` metric.

**Alternative considered:** quarantining bad trades through Kinesis. Rejected because a record that can't even be parsed has no symbol to partition by and no schema to validate; the raw frame is logged instead.

## FIX-02: reader survives garbage frames

`json.loads` and the parser now run inside a `try/except Exception` in the receive loop. Failures increment `parse_failures` (the field already existed but was never used), get rate-limited logging, and the loop moves to the next frame. Catching broad `Exception` here is deliberate: this is the boundary where untrusted network input enters, and nothing a frame contains should stop the stream.

## FIX-03: thrown throttling errors are retried, and nothing is silently dropped

`_send_with_retry` now catches `ClientError` and `EndpointConnectionError`. Retryable codes (`ProvisionedThroughputExceededException`, `ThrottlingException`, `KMSThrottlingException`, service errors) back off with jitter and retry the same records. A non-retryable error (for example an IAM `AccessDenied`) puts the records back and re-raises, because retrying can't help and the operator must see it.

When the retry budget runs out, the records go back to the **front** of the buffer (`deque.extendleft`) instead of being counted as dropped, and `flush()` returns `False` so the caller stops hammering Kinesis until the next tick. The only path that drops data is now buffer overflow, which is counted and logged. The buffer also became a `deque`, so taking batches is O(1) instead of `list.pop(0)`.

## FIX-04: the event loop stays free during flushes

`flush_async(sink)` runs `sink.flush` through `asyncio.to_thread`. Because the websocket readers now append while a worker thread drains, the buffer is guarded by a lock and flushes are serialized by a second lock. The async producer creates the sink with `auto_flush=False`, so `add()` never makes a blocking network call from the event loop. The test proves a 50 ms ticker gets all six turns while `put_records` takes 400 ms.

## FIX-05: NaN and Infinity are rejected

`schema_errors` keeps the JSON Schema check and adds an explicit `math.isfinite` check on `price`, `size`, `event_ts`, and `ingest_ts`. The check lives in `schema_errors` rather than in the Lambda so every path that validates (Lambda, replay, backfill) gets it. Such records go to quarantine with a readable reason, for example `price: nan is not a finite number`.

## FIX-06: backfill is never "late"

`late = (not is_backfill) and event_ts < watermark`. The flag now means what the brief says: a live record that arrived after the watermark.

## FIX-07: one source per symbol and bucket

`select_primary_source` keeps, for each symbol and hour (hourly stats) or trade date (daily stats), only rows from the highest-priority source present: Finnhub, then Alpaca. Choosing per bucket rather than per row matters: mixing feeds inside one hour would give an OHLC built from two different markets. Gold rows now carry a `source` column so you can see which feed produced them, and the Athena DDL gained that column.

Why not deduplicate trade by trade? Finnhub sends no trade id, and the two feeds don't cover the same trades (Alpaca's free feed is IEX only), so there is nothing reliable to match on.

Also changed by default: `SOURCES=finnhub,coinbase` (Alpaca live is optional; Alpaca stays the backfill source).

## FIX-08: hourly reruns keep good returns

`run_hourly` calls `previous_closes`, which reads the gold rows just before the window (walking back up to a day, further only if nothing at all was found) and passes them to `hourly_stats(prev_close=...)`. The first hour of each symbol in the window uses that close as its baseline instead of NaN. Because gold is the input, a rerun reproduces exactly what the previous run wrote.

The stats code was also rewritten with plain `groupby().agg()` instead of `groupby().apply(include_groups=False)`, which is both faster and compatible with older pandas (needed for FIX-14).

## FIX-09: late trades reach hourly gold by the next morning

New `rebuild_hourly_day(day)` recomputes all 24 UTC hours of a day. `run_daily` (the 00:30 nightly job) calls it first, so the hourly and daily tables agree every morning. The 3-hour lookback still handles ordinary lateness within the day.

## FIX-10: reconciliation compares like with like

`run_dq` still reads 6 extra hours of bronze by arrival time (to catch late arrivals) but now filters bronze and quarantine rows to the **event-time** day before counting, exactly like silver's partitioning. Silver also excludes `is_backfill` rows, because those enter through `bronze/backfill/` and the replay job, not through the stream being reconciled.

## FIX-11: half days

`NYSE_EARLY_CLOSES` (13:00 close, 17:00 end of post-market) sits next to the holiday list, and `regular_close(day)` drives both `equity_session` and the expected hours in `missing_equity_hours`. On Nov 27, 2026 the check now expects 10:00 to 12:00 only. Production note: both lists should be replaced by `exchange_calendars` once the Lambda layer budget allows; they are explicit here to keep the zip small, and the list must be extended each December.

## FIX-12: bulk deletes are chunked

New `delete_keys` in `common/storage.py` sends at most 1,000 keys per `DeleteObjects` call, and both `delete_prefix` and the compaction job use it. The new test uses a fake S3 client that records request sizes, because moto doesn't enforce the limit.

## FIX-13: retry budget per request

`retries = 0` after every successful response, so the 5-retry budget applies to each page request, not to the whole day.

## FIX-14: the Glue jobs run on the runtime they deploy to

Three changes:

1. **Code:** every `datetime.UTC` became `timezone.utc` (aliased as `UTC`). Ruff's `target-version` was lowered to `py39`; ruff's pyupgrade rule had been "fixing" the code back to `datetime.UTC` during the first attempt at this fix, which is how it slipped in originally.
2. **Libraries:** [Glue's Python shell 3.9 analytics set](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.md) ships pandas 1.4.2. The jobs now install `pandas==2.2.3, pyarrow==17.0.0, numpy==1.26.4, pyyaml==6.0.2, jsonschema==4.23.0` through `--additional-python-modules` (pandas 2.2 is the last line that supports Python 3.9).
3. **Proof:** a `glue-runtime` CI job installs Python 3.9 with exactly those pins, runs `vermin -t=3.9`, and runs the stats, jobs, DQ, and compaction tests. Locally the same suite passed under Python 3.9.25 (21 passed, 1 skipped after the later fixes).

**Remaining risk, documented rather than fixed:** boto3 drops Python 3.9 support from April 29, 2026 (it prints a deprecation warning under 3.9), and AWS recommends migrating off Python shell jobs. The next step is moving the three jobs to a Glue Spark job (Glue 5.0, Python 3.11) or to a container Lambda on a schedule; the job logic is already plain functions in `aggregations/jobs.py`, so only the wrappers change.

## FIX-15: look-alike trades keep distinct ids

`LookalikeCounter` (in `producer/parsers.py`) numbers Finnhub trades that share symbol, millisecond, price, and size within a 60-second window of event time: the first gets occurrence 0, the next 1, and so on. `make_event_id` appends the occurrence only when it is above 0, so every id that existed before the fix is unchanged and existing silver data still deduplicates correctly.

**Tradeoff, stated plainly:** if Finnhub ever re-sent the exact same print, it would now be counted twice. Finnhub has no replay on reconnect, so a re-send is far less likely than two real identical prints; losing real volume was the worse error. Kinesis retries are unaffected, because the id is computed once in the producer and the retried record carries it.

## FIX-16: movers by each symbol's latest day

The query, the `v_top_movers` view, and the dashboard now take each symbol's own latest `trade_date` (the view restricts that to the last 7 days so Athena still prunes partitions). On a weekend the list shows Friday's stock moves next to today's crypto moves, with the date in each row.

## FIX-17: untracked compiled files

`git rm -r --cached` on every tracked `__pycache__` directory. The `.gitignore` entries already existed, so they stay ignored from now on.

## FIX-18: baselines across weekends

`previous_closes` now takes the set of symbols in the window and walks back hour by hour until each one is found, up to 100 hours (Friday 20:00 to Tuesday 04:00 New York after a Monday holiday is 80 hours). The extra reads happen only in the first hourly run after a weekend; on ordinary hours every symbol is found within one or two partitions.

## FIX-19: data files ship inside the package

`symbols.yaml` and `trade_event.v1.json` moved to `src/stockstream/config/` and `src/stockstream/contracts/`, and `package-data` includes both. `config/` and `contracts/` at the repo root are now symlinks, so the paths in the brief and README still work. The Makefile's and Dockerfile's manual copy steps were removed, since every build now gets the files from the package. The new test builds the real wheel and checks its file list.

## FIX-20: alarms can reach the topic

The topic policy now lists `events.amazonaws.com` and `cloudwatch.amazonaws.com`, with an `aws:SourceAccount` condition so no other account's alarms or rules can publish to it. Budgets is not listed because the budget emails its subscribers directly.

## FIX-21: no empty Glue arguments

`--RUN_DATE` defaults to `auto`, which the scripts read as "yesterday, New York time"; a manual rerun passes a real date such as `2026-10-29`.

---

## Found while fixing (not bugs in the pipeline, but worth knowing)

- **moto's Kinesis is O(n) per put.** It recomputes the max sequence number on every record, so a simulated day of 148k trades spent most of its 313 seconds inside moto. The local simulator now uses a small in-memory fake (`local/fake_kinesis.py`); one simulated day dropped to 44 seconds. Unit and integration tests still use moto.
- **JSON Schema validation costs about 0.3 ms per record** (about 150 ms per 500-record Lambda batch). Fine at this volume; if it ever matters, compile the schema with `fastjsonschema`.
- **A flaky test hid BUG-02.** The first test server sent frames before the client subscribed, so the client died on its own send and never saw the bad frame. Tests that fake a server must follow the real protocol order.
