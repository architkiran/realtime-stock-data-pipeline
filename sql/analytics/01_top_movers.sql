-- Top 5 movers by daily return on each symbol's latest trading day.
-- (Equities have no bar on weekends and holidays, so "the latest date overall" would only show crypto.)
WITH latest AS (
  SELECT symbol, max(trade_date) AS trade_date FROM stockstream.daily_stats GROUP BY symbol
)
SELECT d.symbol, d.asset_class, d.trade_date, round(d.close, 2) AS close, round(100 * d.return_1d, 2) AS return_pct
FROM stockstream.daily_stats d
JOIN latest l ON d.symbol = l.symbol AND d.trade_date = l.trade_date
WHERE d.return_1d IS NOT NULL
ORDER BY abs(d.return_1d) DESC
LIMIT 5;
