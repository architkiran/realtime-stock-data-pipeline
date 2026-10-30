-- Correlation of hourly returns: SPY vs each crypto pair, during hours both traded.
SELECT c.symbol AS crypto, count(*) AS hours,
       round(corr(s.return_vs_prev_close, c.return_vs_prev_close), 3) AS corr_with_spy
FROM stockstream.hourly_stats s
JOIN stockstream.hourly_stats c ON s.hour_start = c.hour_start
WHERE s.symbol = 'SPY' AND c.asset_class = 'crypto'
  AND s.return_vs_prev_close IS NOT NULL AND c.return_vs_prev_close IS NOT NULL
GROUP BY c.symbol
ORDER BY corr_with_spy DESC;
