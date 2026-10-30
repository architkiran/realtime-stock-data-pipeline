"""Run the sql/analytics queries with DuckDB over the local lake (stands in for Athena).

    python -m stockstream.local.query_local --lake local_output
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

TABLES = {
    "silver_trades": "silver/trades/**/*.parquet",
    "hourly_stats": "gold/hourly_stats/**/*.parquet",
    "daily_stats": "gold/daily_stats/**/*.parquet",
    "dq_results": "gold/dq_results/**/*.parquet",
}


def connect(lake: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA stockstream")
    for name, pattern in TABLES.items():
        if list(lake.glob(pattern)):
            con.execute(f"CREATE VIEW stockstream.{name} AS SELECT * FROM "
                        f"read_parquet('{lake / pattern}', hive_partitioning = true, union_by_name = true)")
    return con


def run_all(lake: Path, sql_dir: Path) -> dict[str, str]:
    con = connect(lake)
    results = {}
    for path in sorted(sql_dir.glob("*.sql")):
        results[path.name] = con.sql(path.read_text()).df().to_string(index=False, max_rows=15)
    return results


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lake", type=Path, default=Path("local_output"))
    p.add_argument("--sql", type=Path, default=Path(__file__).resolve().parents[3] / "sql" / "analytics")
    args = p.parse_args()
    for name, table in run_all(args.lake, args.sql).items():
        print(f"\n=== {name} ===\n{table}")


if __name__ == "__main__":
    main()
