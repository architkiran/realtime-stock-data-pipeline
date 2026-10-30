-- Days where volume was more than 3 standard deviations above the prior 20-day mean.
SELECT symbol, trade_date, volume, round(volume_z, 2) AS volume_z
FROM stockstream.daily_stats
WHERE volume_z > 3
ORDER BY volume_z DESC;
