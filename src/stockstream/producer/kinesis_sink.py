"""Buffered Kinesis writer.

PutRecords takes at most 500 records / 5 MiB per call and can fail two ways:
- partially: the response says which records were rejected (usually throttling),
  and only those must be retried (retrying the whole batch duplicates the rest);
- completely: under heavy throttling the call itself raises
  ProvisionedThroughputExceededException.

Records that still fail after the retry budget go back to the front of the
buffer, so nothing is lost while Kinesis recovers; only buffer overflow drops
data, and that is counted.

boto3 is blocking, so the async producer flushes through `flush_async`, which
runs the flush in a worker thread and keeps the websocket event loop free.
"""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from botocore.exceptions import ClientError, EndpointConnectionError

from stockstream.common.logging_setup import get_logger, log_fields

log = get_logger(__name__)

MAX_RECORDS_PER_CALL = 500
MAX_BYTES_PER_CALL = 5 * 1024 * 1024
RETRYABLE_CODES = {"ProvisionedThroughputExceededException", "ThrottlingException", "LimitExceededException",
                   "ServiceUnavailable", "InternalFailure", "KMSThrottlingException"}


@dataclass
class SinkStats:
    sent: int = 0
    retried: int = 0
    dropped: int = 0
    calls: int = 0
    requeued: int = 0


@dataclass
class KinesisSink:
    client: object
    stream_name: str
    max_buffer: int = 50_000
    max_attempts: int = 5
    base_backoff_s: float = 0.1
    auto_flush: bool = True  # sync callers flush on every 500 records; the async producer sets False
    sleep: Callable[[float], None] = time.sleep
    stats: SinkStats = field(default_factory=SinkStats)
    _buffer: deque = field(default_factory=deque)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _flush_lock: threading.Lock = field(default_factory=threading.Lock)

    def __len__(self) -> int:
        return len(self._buffer)

    def add(self, events: Iterable[dict]) -> None:
        records = [{"Data": json.dumps(e).encode(), "PartitionKey": e["symbol"]} for e in events]
        with self._lock:
            self._buffer.extend(records)
            self._trim()
        if self.auto_flush and len(self._buffer) >= MAX_RECORDS_PER_CALL:
            self.flush()

    def _trim(self) -> None:
        overflow = len(self._buffer) - self.max_buffer
        if overflow > 0:
            for _ in range(overflow):
                self._buffer.popleft()
            self.stats.dropped += overflow
            log_fields(log, "buffer overflow, dropped oldest", dropped=overflow)

    def flush(self) -> bool:
        """Send everything buffered. Returns False if Kinesis kept failing (records stay buffered)."""
        with self._flush_lock:
            while True:
                batch = self._take_batch()
                if not batch:
                    return True
                if not self._send_with_retry(batch):
                    return False

    def _take_batch(self) -> list[dict]:
        batch, size = [], 0
        with self._lock:
            while self._buffer and len(batch) < MAX_RECORDS_PER_CALL:
                rec = self._buffer[0]
                rec_size = len(rec["Data"]) + len(rec["PartitionKey"])
                if batch and size + rec_size > MAX_BYTES_PER_CALL:
                    break
                batch.append(self._buffer.popleft())
                size += rec_size
        return batch

    def _backoff(self, attempt: int) -> None:
        self.sleep(self.base_backoff_s * (2 ** (attempt - 1)) * (0.5 + random.random()))

    def _send_with_retry(self, batch: list[dict]) -> bool:
        pending = batch
        for attempt in range(1, self.max_attempts + 1):
            self.stats.calls += 1
            try:
                resp = self.client.put_records(StreamName=self.stream_name, Records=pending)
            except (ClientError, EndpointConnectionError) as exc:
                code = exc.response["Error"]["Code"] if isinstance(exc, ClientError) else "EndpointConnectionError"
                if code not in RETRYABLE_CODES and code != "EndpointConnectionError":
                    self._requeue(pending)
                    raise
                log_fields(log, "put_records call failed", code=code, attempt=attempt, records=len(pending))
                self.stats.retried += len(pending)
                self._backoff(attempt)
                continue
            failed = resp.get("FailedRecordCount", 0)
            self.stats.sent += len(pending) - failed
            if failed == 0:
                return True
            pending = [rec for rec, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            self.stats.retried += len(pending)
            log_fields(log, "put_records partial failure", failed=failed, attempt=attempt)
            self._backoff(attempt)
        self._requeue(pending)
        log_fields(log, "kinesis still failing after retries; records kept in buffer", records=len(pending))
        return False

    def _requeue(self, records: list[dict]) -> None:
        with self._lock:
            self._buffer.extendleft(reversed(records))
            self.stats.requeued += len(records)
            self._trim()


async def flush_async(sink: KinesisSink) -> bool:
    """Flush in a worker thread so the event loop keeps reading websockets meanwhile."""
    return await asyncio.to_thread(sink.flush)
