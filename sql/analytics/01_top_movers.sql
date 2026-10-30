-- Top 5 movers by daily return on the latest day.
SELECT symbol, trade_date, round(close, 2) AS close, round(100 * return_1d, 2) AS return_pct
FROM stockstream.daily_stats
WHERE return_1d IS NOT NULL
  AND trade_date = (SELECT max(trade_date) FROM stockstream.daily_stats)
ORDER BY abs(return_1d) DESC
LIMIT 5;
