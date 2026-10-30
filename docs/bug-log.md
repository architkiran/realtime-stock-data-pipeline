# Bug log: break-it phase

**Version under test:** v0.1 (commit `641043b`), the first complete build.
**Method:** an adversarial test suite (`tests/chaos/test_break_it.py`) written after the build, plus a code review pass for things moto cannot simulate. Each test states the behavior the pipeline *should* have. A failing test is a bug.
**Result:** 18 chaos tests, 14 failed, 4 passed. Every failure is listed below with its symptom, root cause, impact, and the test that reproduces it. Fixes are in [fix-log.md](fix-log.md).

| ID | Area | Severity | Bug | Test |
|---|---|---|---|---|
| BUG-01 | Producer | High | One malformed trade crashes the parser and kills the websocket reader | test_01 |
| BUG-02 | Producer | High | A non-JSON frame crashes the reader; `parse_failures` is never counted | test_02 |
| BUG-03 | Producer | High | A fully throttled `PutRecords` call raises and the buffer is lost | test_03 |
| BUG-04 | Producer | Medium | Blocking boto3 flush freezes the asyncio loop (websocket pings starve) | test_04b |
| BUG-05 | Validation | High | `NaN` and `Infinity` prices and sizes pass the data contract | test_05 |
| BUG-06 | Validation | Medium | Every backfilled trade is flagged `late=true` | test_06 |
| BUG-07 | Aggregation | Critical | Equities streamed from Finnhub and Alpaca are counted twice | test_07 |
| BUG-08 | Aggregation | High | Each hourly rerun wipes the return of the oldest hour in its window | test_08 |
| BUG-09 | Aggregation | High | Trades later than the 3-hour lookback never reach hourly gold | test_09 |
| BUG-10 | Data quality | Medium | Bronze vs silver reconciliation fails falsely across midnight | test_10 |
| BUG-11 | Data quality | Low | Missing-hours check ignores NYSE half days | test_11 |
| BUG-12 | Glue job | Medium | Compaction sends more than 1000 keys to `DeleteObjects` | test_12b |
| BUG-13 | Backfill | Medium | The 429 retry budget is shared by every page of a day | test_13 |
| BUG-14 | Deployment | Critical | Glue Python shell runs Python 3.9; the code needs 3.11 | test_14 |

What held up (tests that passed on the first run): a 50,000-event burst flushes in well under a second; DST fall-back on Nov 1, 2026 is classified correctly; an S3 failure halfway through a Lambda batch followed by a retry does not double count in gold (dedupe on `event_id` works); compaction of 1,005 files works on moto (but see BUG-12).

---

## BUG-01: One malformed trade crashes the parser

- **Symptom:** `parse_finnhub` raises `KeyError: 'p'` when one trade in a message has no price, and `ValueError` when the price is a string like `"n/a"`.
- **Root cause:** the parsers index fields directly (`t["p"]`) even though the module docstring promises they never raise. Nothing above them catches the error.
- **Impact:** the exception escapes `SourceReader.run`, which only catches network errors, so one bad trade stops that source until the container restarts. The good trades in the same message are lost too.
- **How it could have been spotted:** the docstring made a promise that no test checked.

## BUG-02: A non-JSON frame crashes the reader

- **Symptom:** `json.decoder.JSONDecodeError` escapes `SourceReader.run`. `parse_failures` exists on the class but is never incremented.
- **Root cause:** `json.loads(raw)` sits inside the receive loop with no error handling; only `OSError` and `ConnectionClosed` are caught.
- **Impact:** same as BUG-01. Servers do send odd frames (error strings, HTML from a proxy), so this would happen in production.
- **Also found while writing the test:** the first version of the test server sent data before the client subscribed, which hid the bug behind a race. Real servers wait for the subscribe message; the test now does too.

## BUG-03: A fully throttled PutRecords call loses the buffer

- **Symptom:** `ClientError (ProvisionedThroughputExceededException)` propagates out of `KinesisSink.flush`.
- **Root cause:** the retry loop only handles *partial* failures (`FailedRecordCount > 0`). When Kinesis throttles hard it rejects the whole call with an exception, which the loop never sees. The batch was already popped from the buffer, so those records are gone.
- **Impact:** data loss plus a producer crash at exactly the moment traffic spikes (market open).

## BUG-04: Blocking flush freezes the event loop

- **Symptom:** while `put_records` takes 400 ms, an asyncio ticker scheduled every 50 ms gets no turns.
- **Root cause:** boto3 is synchronous, and `flusher()` calls `sink.flush()` directly inside the event loop.
- **Impact:** during slow Kinesis calls (throttling with backoff can take seconds) no websocket frames are read and keepalive pings go unanswered, so servers drop the connection, which causes gaps and reconnect storms.
- **Found by:** code review; the test was written to prove it.

## BUG-05: NaN and Infinity pass the data contract

- **Symptom:** a record with `"price": NaN` or `"size": Infinity` is written to silver.
- **Root cause:** Python's `json` accepts `NaN` and `Infinity`. JSON Schema's `exclusiveMinimum: 0` is checked as `instance <= 0`, and `NaN <= 0` is `False`, so NaN passes. Infinity is greater than 0, so it passes too.
- **Impact:** one NaN trade turns the hour's VWAP and volume into NaN for that symbol, and Infinity makes volume infinite. Bad data looks valid.

## BUG-06: Backfilled trades are flagged late

- **Symptom:** a 30-day-old backfill trade has `late=true`.
- **Root cause:** the late check compares `event_ts` with the processing-time watermark and ignores `is_backfill`.
- **Impact:** the late-rate metric becomes meaningless after any backfill, and "late" filters in Athena hide all history.

## BUG-07: Equities are double counted

- **Symptom:** one trade seen by both Finnhub and Alpaca produces hourly volume 20 instead of 10.
- **Root cause:** `symbols.yaml` streams the same equity list from both sources. Finnhub gives no trade id and Alpaca does, so the two copies get different `event_id`s and dedupe can't merge them. The stats group by symbol across all sources.
- **Impact:** every volume, VWAP, and trade count for equities is wrong. The two feeds are not even the same market (Alpaca free is IEX only), so summing them is meaningless.
- **How it could have been spotted:** the brief's data contract says nothing about which source is authoritative for a symbol.

## BUG-08: Hourly reruns wipe the oldest hour's return

- **Symptom:** after six hourly runs, hours 1 to 3 have `return_vs_prev_close = NaN`.
- **Root cause:** each run reads only its 3-hour window. The first hour in the window has no previous hour in the data it read, so `pct_change` gives NaN, and the job then overwrites the correct value written by the previous run.
- **Impact:** most hourly returns end up NaN in gold, which breaks the correlation query (`05_equity_crypto_correlation.sql`).

## BUG-09: Very late trades never reach hourly gold

- **Symptom:** a trade that arrives 5 hours late lands in silver but hourly gold for its hour still shows the old volume (10, not 15), even after the nightly job.
- **Root cause:** the hourly job only recomputes the last 3 hours, and the "nightly rebuild of yesterday" described in the brief was never implemented.
- **Impact:** hourly and daily gold disagree for that hour; the daily DQ check cannot explain the difference.

## BUG-10: Reconciliation fails falsely across midnight

- **Symptom:** with two days of crypto data, the check reports `bronze=30 silver=24 gap=0.2000` and FAILs, although nothing is missing.
- **Root cause:** bronze is read for 30 hours of *arrival time* (to catch late arrivals), but silver is read for 24 hours of *event time*. The extra 6 hours of bronze have no counterpart.
- **Impact:** a FAIL every day the pipeline runs past midnight, so people learn to ignore the check.

## BUG-11: Missing-hours check ignores half days

- **Symptom:** on Nov 27, 2026 (the market closes at 13:00) the check warns about 90 missing symbol-hours.
- **Root cause:** the expected hours are always 10:00 to 15:00 New York; there is no early-close calendar.
- **Impact:** false warnings three days a year. Low severity, but alert fatigue starts this way.

## BUG-12: Compaction sends more than 1000 keys to DeleteObjects

- **Symptom:** one call with 1,500 keys.
- **Root cause:** compaction deletes all source files in one `delete_objects` call. Real S3 caps it at 1,000 keys and returns `MalformedXML`; moto does not enforce the cap, which is why the moto-based test passed.
- **Impact:** an hour with more than 1,000 Lambda batch files (very plausible at market open) fails to compact *after* the merged file was written, leaving duplicates until the next run. Dedupe hides it in gold, but Athena scans grow.
- **Lesson:** mocks are not the real service. Limits must be tested against a fake that enforces them.

## BUG-13: Shared 429 retry budget aborts long backfills

- **Symptom:** a 7-page day with a 429 before each of its first pages fails with `HTTP 429` after the sixth 429.
- **Root cause:** `retries` counts across the whole day instead of resetting after each successful page.
- **Impact:** backfills of busy symbols (many pages) abort under normal rate limiting, and the long days are exactly the ones you want.

## BUG-14: Glue Python shell runs Python 3.9

- **Symptom:** `vermin -t=3.9` reports `datetime.UTC` (Python 3.11+) in the Glue jobs and in every module they import. `groupby(...).apply(include_groups=False)` also needs pandas 2.2, newer than Glue's bundled pandas.
- **Root cause:** the code was written and tested on Python 3.13; the Terraform deploys the jobs as `pythonshell` with `python_version = "3.9"`.
- **Impact:** every Glue job fails on its first run in AWS with `ImportError`. Nothing local or in CI would have caught it.
- **Lesson:** test against the runtime you deploy to, not the one on your laptop.
