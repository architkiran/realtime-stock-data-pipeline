# ADR 003: Glue Python shell jobs for the batch layer

**Status:** accepted with a known expiry, 2026-10-30 (see BUG-14)

**Context.** The batch jobs process megabytes to low gigabytes per day. Spark would be
overkill; a 1/16-DPU Python shell job is the cheapest Glue option.

**Decision.** Python shell 3.9 with pinned pandas 2.2.3 and pyarrow 17, tested in CI on Python 3.9.

**Consequences.** boto3 ends Python 3.9 support on April 29, 2026 and AWS recommends migrating
off Python shell. The job logic is plain functions in `aggregations/jobs.py`, so moving to a
Glue 5.0 Spark job or a scheduled container Lambda only changes the thin wrappers in `glue_jobs/`.
