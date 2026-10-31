-- Deduplicated trades for today and yesterday (partition filter keeps scans small).
CREATE OR REPLACE VIEW stockstream.v_recent_trades AS
SELECT * FROM (
  SELECT t.*, row_number() OVER (PARTITION BY event_id ORDER BY processed_ts) AS rn
  FROM stockstream.silver_trades t
  WHERE dt >= date_format(current_date - interval '1' day, '%Y-%m-%d')
) WHERE rn = 1;

-- Latest trade per symbol and how stale it is.
CREATE OR REPLACE VIEW stockstream.v_latest_price AS
SELECT symbol, asset_class, price, from_unixtime(event_ts / 1000) AS trade_time,
       date_diff('second', from_unixtime(event_ts / 1000), current_timestamp) AS age_seconds
FROM (
  SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY event_ts DESC) AS rk
  FROM stockstream.v_recent_trades
) WHERE rk = 1;

-- Running intraday VWAP by symbol (UTC day).
CREATE OR REPLACE VIEW stockstream.v_intraday_vwap AS
SELECT symbol, dt, sum(price * size) / sum(size) AS vwap, sum(size) AS volume, count(*) AS trades
FROM stockstream.v_recent_trades
GROUP BY symbol, dt;

-- Biggest daily movers on each symbol's latest trading day (equities have no weekend bars).
CREATE OR REPLACE VIEW stockstream.v_top_movers AS
SELECT d.symbol, d.asset_class, d.trade_date, d.close, d.return_1d
FROM stockstream.daily_stats d
JOIN (SELECT symbol, max(trade_date) AS trade_date FROM stockstream.daily_stats
      WHERE dt >= date_format(current_date - interval '7' day, '%Y-%m-%d') GROUP BY symbol) l
  ON d.symbol = l.symbol AND d.trade_date = l.trade_date
WHERE d.dt >= date_format(current_date - interval '7' day, '%Y-%m-%d')
ORDER BY abs(d.return_1d) DESC
LIMIT 10;
