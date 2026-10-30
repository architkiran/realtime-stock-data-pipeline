-- Did each symbol close above or below its VWAP? (Closing above VWAP is often read as buyers in control.)
SELECT symbol, trade_date, round(vwap, 2) AS vwap, round(close, 2) AS close,
       round(100 * (close - vwap) / vwap, 3) AS close_vs_vwap_pct
FROM stockstream.daily_stats
ORDER BY trade_date DESC, close_vs_vwap_pct DESC;
