-- Silver: validated trades, one row per trade (may contain duplicates across files;
-- dedupe on event_id, as the views below do). Partition projection means no crawler
-- and no MSCK REPAIR: Athena computes the partitions from the path template.
CREATE EXTERNAL TABLE IF NOT EXISTS stockstream.silver_trades (
  event_id      string,
  source        string,
  asset_class   string,
  symbol        string,
  price         double,
  size          double,
  event_ts      bigint,
  ingest_ts     bigint,
  trade_id      string,
  venue         string,
  is_backfill   boolean,
  session       string,
  late          boolean,
  suspect_price boolean,
  processed_ts  bigint
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${bucket}/silver/trades/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.range'         = '2016-01-01,NOW',
  'projection.hour.type'        = 'integer',
  'projection.hour.range'       = '0,23',
  'projection.hour.digits'      = '2',
  'storage.location.template'   = 's3://${bucket}/silver/trades/dt=${dt}/hour=${hour}/'
);
