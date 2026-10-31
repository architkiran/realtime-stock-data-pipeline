"""A tiny in-memory Kinesis for the local simulator.

moto's Kinesis recomputes the max sequence number on every put (O(n) per record),
so a simulated day of ~150k trades took minutes in moto alone. This fake keeps the
same API surface the pipeline uses (create_stream, describe_stream, get_shard_iterator,
put_records, get_records) and the same 500-records-per-call limit, in O(1) per record.
Tests that need real AWS behaviour still use moto.
"""

from __future__ import annotations


class FakeKinesis:
    def __init__(self) -> None:
        self._streams: dict[str, list[dict]] = {}

    def create_stream(self, StreamName: str, ShardCount: int = 1) -> None:  # noqa: N803
        self._streams[StreamName] = []

    def describe_stream(self, StreamName: str) -> dict:  # noqa: N803
        return {"StreamDescription": {"Shards": [{"ShardId": "shardId-000000000000"}]}}

    def get_shard_iterator(self, StreamName: str, ShardId: str, ShardIteratorType: str) -> dict:  # noqa: N803
        return {"ShardIterator": f"{StreamName}|0"}

    def put_records(self, StreamName: str, Records: list[dict]) -> dict:  # noqa: N803
        if len(Records) > 500:
            raise ValueError("PutRecords accepts at most 500 records")
        log = self._streams[StreamName]
        out = []
        for rec in Records:
            seq = f"{len(log):056d}"
            log.append({"SequenceNumber": seq, "Data": rec["Data"], "PartitionKey": rec["PartitionKey"]})
            out.append({"SequenceNumber": seq, "ShardId": "shardId-000000000000"})
        return {"FailedRecordCount": 0, "Records": out}

    def get_records(self, ShardIterator: str, Limit: int = 10_000) -> dict:  # noqa: N803
        name, pos = ShardIterator.rsplit("|", 1)
        start = int(pos)
        recs = self._streams[name][start : start + Limit]
        return {"Records": recs, "NextShardIterator": f"{name}|{start + len(recs)}"}
