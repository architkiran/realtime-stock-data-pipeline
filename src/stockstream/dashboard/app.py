"""Streamlit dashboard.

    # against the local lake built by `make local`
    LAKE_PATH=local_output streamlit run src/stockstream/dashboard/app.py
    # against Athena in your account
    ATHENA_WORKGROUP=stockstream AWS_REGION=us-east-1 streamlit run src/stockstream/dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st


@st.cache_resource
def connection():
    lake = os.environ.get("LAKE_PATH")
    if lake:
        from stockstream.local.query_local import connect

        return "duckdb", connect(Path(lake))
    from pyathena import connect as athena_connect

    return "athena", athena_connect(work_group=os.environ.get("ATHENA_WORKGROUP", "stockstream"),
                                    region_name=os.environ.get("AWS_REGION", "us-east-1"))


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    kind, con = connection()
    if kind == "duckdb":
        return con.sql(sql).df()
    return pd.read_sql(sql, con)


st.set_page_config(page_title="Stock stream", layout="wide")
st.title("Stock and crypto trade pipeline")

latest_day = query("SELECT max(trade_date) AS d FROM stockstream.daily_stats")["d"].iloc[0]
st.caption(f"Latest stored trading day: {latest_day}")

left, right = st.columns(2)
with left:
    st.subheader("Top movers")
    st.dataframe(query(f"""
        SELECT symbol, round(close, 2) AS close, round(100 * return_1d, 2) AS return_pct
        FROM stockstream.daily_stats WHERE trade_date = DATE '{latest_day}' AND return_1d IS NOT NULL
        ORDER BY abs(return_1d) DESC LIMIT 10"""), hide_index=True)
with right:
    st.subheader("20-day volatility (annualized)")
    st.dataframe(query(f"""
        SELECT symbol, round(100 * volatility_20d, 1) AS vol_pct, round(volume_z, 2) AS volume_z
        FROM stockstream.daily_stats WHERE trade_date = DATE '{latest_day}' AND volatility_20d IS NOT NULL
        ORDER BY volatility_20d DESC LIMIT 10"""), hide_index=True)

symbols = query("SELECT DISTINCT symbol FROM stockstream.hourly_stats ORDER BY 1")["symbol"].tolist()
symbol = st.selectbox("Symbol", symbols, index=symbols.index("SPY") if "SPY" in symbols else 0)
hourly = query(f"""
    SELECT hour_start, close, vwap, volume FROM stockstream.hourly_stats
    WHERE symbol = '{symbol}' ORDER BY hour_start""")
st.subheader(f"{symbol}: hourly close and VWAP")
st.line_chart(hourly.set_index("hour_start")[["close", "vwap"]])
st.bar_chart(hourly.set_index("hour_start")["volume"])

st.subheader("Data quality")
st.dataframe(query("""SELECT run_date, "check", status, detail FROM stockstream.dq_results
                      ORDER BY run_date DESC, "check" LIMIT 30"""), hide_index=True)
