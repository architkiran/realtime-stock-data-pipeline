-- Which hour of the New York trading day is most volatile for equities?
SELECT hour(hour_start AT TIME ZONE 'America/New_York') AS ny_hour,
       count(*) AS symbol_hours,
       round(avg(realized_vol_1m) * 10000, 2) AS avg_1m_vol_bps,
       round(avg((high - low) / vwap) * 10000, 2) AS avg_range_bps
FROM stockstream.hourly_stats
WHERE asset_class = 'equity' AND realized_vol_1m IS NOT NULL
GROUP BY 1
ORDER BY avg_1m_vol_bps DESC;
