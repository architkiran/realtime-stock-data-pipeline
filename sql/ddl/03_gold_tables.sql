CREATE EXTERNAL TABLE IF NOT EXISTS stockstream.hourly_stats (
  symbol string, asset_class string, source string, hour_start timestamp,
  open double, high double, low double, close double, volume double, trade_count bigint,
  vwap double, realized_vol_1m double, return_vs_prev_close double
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${bucket}/gold/hourly_stats/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date', 'projection.dt.format' = 'yyyy-MM-dd', 'projection.dt.range' = '2016-01-01,NOW',
  'projection.hour.type' = 'integer', 'projection.hour.range' = '0,23', 'projection.hour.digits' = '2',
  'storage.location.template' = 's3://${bucket}/gold/hourly_stats/dt=${dt}/hour=${hour}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS stockstream.daily_stats (
  symbol string, asset_class string, source string, trade_date date,
  open double, high double, low double, close double, volume double, trade_count bigint, vwap double,
  return_1d double, ma_20 double, ma_50 double, volatility_20d double, volume_z double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://${bucket}/gold/daily_stats/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date', 'projection.dt.format' = 'yyyy-MM-dd', 'projection.dt.range' = '2016-01-01,NOW',
  'storage.location.template' = 's3://${bucket}/gold/daily_stats/dt=${dt}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS stockstream.dq_results (
  `check` string, status string, detail string, run_date string
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://${bucket}/gold/dq_results/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date', 'projection.dt.format' = 'yyyy-MM-dd', 'projection.dt.range' = '2016-01-01,NOW',
  'storage.location.template' = 's3://${bucket}/gold/dq_results/dt=${dt}/'
);
