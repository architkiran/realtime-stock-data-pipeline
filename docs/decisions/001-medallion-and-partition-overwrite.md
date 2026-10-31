# ADR 001: Medallion layout with partition-overwrite gold (not Iceberg MERGE, for now)

**Status:** accepted, 2026-10-30

**Context.** Kinesis and Lambda deliver at least once, trades can arrive late, and the hourly and
daily stats must be correct after retries and reruns. The brief proposed Apache Iceberg with
`MERGE INTO` for gold.

**Decision.** Bronze (untouched JSON), silver (validated Parquet, may hold duplicates), gold
(stats Parquet). Gold partitions are rebuilt whole on every run (delete the partition, write it
again), computed from deduplicated silver. A rerun therefore produces the same files.

**Why not Iceberg yet.** Partition overwrite gives the same idempotency for a single writer with
no extra moving parts, runs identically on moto and DuckDB in tests, and needs no Spark. Iceberg
earns its place when there are concurrent writers, row-level updates, or a need for time travel.

**Consequences.** A reader querying during the few milliseconds between delete and write can see
an empty partition. Acceptable for a dashboard refreshed every minute; not acceptable for a
system of record, which is when to switch to Iceberg (listed as a stretch goal).
