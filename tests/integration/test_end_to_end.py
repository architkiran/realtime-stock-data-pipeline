"""Whole pipeline on one simulated day at small scale: simulator -> parsers -> Kinesis -> Lambda ->
silver -> hourly/daily gold -> DQ, then the analytics SQL through DuckDB."""

from datetime import date
from pathlib import Path

from stockstream.local.query_local import run_all
from stockstream.local.run_local import run

ROOT = Path(__file__).resolve().parents[2]


def test_one_day_end_to_end(tmp_path):
    result = run(date(2026, 10, 29), days=1, scale=0.05, out=tmp_path)
    assert result["produced"] > 5_000
    assert result["sink"]["dropped"] == 0
    assert {r["check"]: r["status"] for r in result["last_day_dq"]} == {
        "bronze_vs_silver": "PASS", "missing_equity_hours": "PASS", "ohlc_sanity": "PASS"}
    assert result["daily_rows"] == 33  # 30 equities + 3 crypto pairs

    outputs = run_all(tmp_path, ROOT / "sql" / "analytics")
    assert set(outputs) == {p.name for p in (ROOT / "sql" / "analytics").glob("*.sql")}
    assert "PASS" in outputs["06_pipeline_health.sql"]
