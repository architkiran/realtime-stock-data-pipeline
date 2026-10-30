"""Buffered Kinesis writer.

PutRecords takes at most 500 records / 5 MiB per call and can partially fail:
the response says which records were rejected (usually throttling), and only
those must be retried. Retrying the whole batch would duplicate the successes.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from stockstream.common.logging_setup import get_logger, log_fields

log = get_logger(__name__)

MAX_RECORDS_PER_CALL = 500
MAX_BYTES_PER_CALL = 5 * 1024 * 1024


@dataclass
class SinkStats:
    sent: int = 0
    retried: int = 0
    dropped: int = 0
    calls: int = 0


@dataclass
class KinesisSink:
    client: object
    stream_name: str
    max_buffer: int = 50_000
    max_attempts: int = 5
    base_backoff_s: float = 0.1
    sleep: Callable[[float], None] = time.sleep
    stats: SinkStats = field(default_factory=SinkStats)
    _buffer: list[dict] = field(default_factory=list)

    def add(self, events: Iterable[dict]) -> None:
        for event in events:
            self._buffer.append(
                {"Data": json.dumps(event).encode(), "PartitionKey": event["symbol"]}
            )
        if len(self._buffer) > self.max_buffer:
            overflow = len(self._buffer) - self.max_buffer
            self._buffer = self._buffer[overflow:]
            self.stats.dropped += overflow
            log_fields(log, "buffer overflow, dropped oldest", dropped=overflow)
        if len(self._buffer) >= MAX_RECORDS_PER_CALL:
            self.flush()

    def flush(self) -> None:
        while self._buffer:
            batch = self._take_batch()
            self._send_with_retry(batch)

    def _take_batch(self) -> list[dict]:
        batch, size = [], 0
        while self._buffer and len(batch) < MAX_RECORDS_PER_CALL:
            rec = self._buffer[0]
            rec_size = len(rec["Data"]) + len(rec["PartitionKey"])
            if batch and size + rec_size > MAX_BYTES_PER_CALL:
                break
            batch.append(self._buffer.pop(0))
            size += rec_size
        return batch

    def _send_with_retry(self, batch: list[dict]) -> None:
        pending = batch
        for attempt in range(1, self.max_attempts + 1):
            self.stats.calls += 1
            resp = self.client.put_records(StreamName=self.stream_name, Records=pending)
            failed = resp.get("FailedRecordCount", 0)
            self.stats.sent += len(pending) - failed
            if failed == 0:
                return
            pending = [rec for rec, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            self.stats.retried += len(pending)
            delay = self.base_backoff_s * (2 ** (attempt - 1)) * (0.5 + random.random())
            log_fields(log, "put_records partial failure", failed=failed, attempt=attempt, sleep_s=round(delay, 3))
            self.sleep(delay)
        self.stats.dropped += len(pending)
        log_fields(log, "giving up on records after retries", dropped=len(pending))
