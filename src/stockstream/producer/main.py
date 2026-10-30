"""Producer entry point (runs as the Fargate task).

Starts one reader per enabled source and funnels every event into one
KinesisSink, flushing at least once a second so latency stays low when
volume is light.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal

import boto3

from stockstream.common.config import load_universe, settings
from stockstream.common.events import now_ms
from stockstream.common.logging_setup import get_logger, log_fields
from stockstream.producer.kinesis_sink import KinesisSink, flush_async
from stockstream.producer.parsers import BAD_TRADES
from stockstream.producer.ws_client import SourceReader

log = get_logger(__name__)

URLS = {
    "finnhub": "wss://ws.finnhub.io?token={key}",
    "alpaca": "wss://stream.data.alpaca.markets/v2/iex",
    "coinbase": "wss://advanced-trade-ws.coinbase.com",
}


def load_secrets() -> dict:
    """API keys come from Secrets Manager (JSON with finnhub_key, alpaca_key, alpaca_secret)."""
    secret_id = os.environ.get("API_SECRET_ID")
    if not secret_id:
        return {}
    sm = boto3.client("secretsmanager", region_name=settings().region)
    return json.loads(sm.get_secret_value(SecretId=secret_id)["SecretString"])


def publish_metrics(cloudwatch, readers, last_trade: dict[str, int], sink: KinesisSink) -> None:
    """Freshness and throughput metrics; the freshness alarm watches LastTradeAgeSeconds."""
    now = now_ms()
    data = [{"MetricName": "RecordsSent", "Value": sink.stats.sent, "Unit": "Count"},
            {"MetricName": "RecordsDropped", "Value": sink.stats.dropped, "Unit": "Count"}]
    for r in readers:
        age = (now - last_trade[r.source]) / 1000 if r.source in last_trade else 86_400
        dims = [{"Name": "Source", "Value": r.source}]
        data.append({"MetricName": "LastTradeAgeSeconds", "Value": age, "Unit": "Seconds", "Dimensions": dims})
        data.append({"MetricName": "BadFrames", "Value": r.parse_failures + BAD_TRADES[r.source],
                     "Unit": "Count", "Dimensions": dims})
    try:
        cloudwatch.put_metric_data(Namespace="StockStream", MetricData=data)
    except Exception:  # metrics must never take the producer down
        log.exception("put_metric_data failed")
    log_fields(log, "sink stats", **sink.stats.__dict__)


async def main() -> None:
    cfg, universe = settings(), load_universe()
    enabled = [s.strip() for s in os.environ.get("SOURCES", "finnhub,coinbase").split(",") if s.strip()]
    secrets = load_secrets()
    sink = KinesisSink(boto3.client("kinesis", region_name=cfg.region), cfg.stream_name, auto_flush=False)

    readers = []
    for source in enabled:
        url = URLS[source].format(key=secrets.get("finnhub_key", ""))
        readers.append(
            SourceReader(
                source=source,
                url=url,
                symbols=universe.symbols_for(source),
                on_events=sink.add,
                api_key=secrets.get("alpaca_key", ""),
                api_secret=secrets.get("alpaca_secret", ""),
            )
        )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: [r.stop() for r in readers])

    cloudwatch = boto3.client("cloudwatch", region_name=cfg.region)
    last_trade: dict[str, int] = {}

    def track(events: list[dict]) -> None:
        for e in events:
            last_trade[e["source"]] = max(last_trade.get(e["source"], 0), e["ingest_ts"])
        sink.add(events)

    for r in readers:
        r.on_events = track

    async def flusher() -> None:
        ticks = 0
        while any(not r._stop.is_set() for r in readers):
            await asyncio.sleep(1)
            await flush_async(sink)  # boto3 blocks; keep the event loop free for websocket frames
            ticks += 1
            if ticks % 60 == 0:
                publish_metrics(cloudwatch, readers, last_trade, sink)

    await asyncio.gather(flusher(), *(r.run() for r in readers))
    await flush_async(sink)


if __name__ == "__main__":
    asyncio.run(main())
