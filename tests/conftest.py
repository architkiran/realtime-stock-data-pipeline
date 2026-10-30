import base64
import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES = Path(__file__).parent / "fixtures"

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = "test-bucket"
os.environ["DATA_BUCKET"] = BUCKET


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        kin = boto3.client("kinesis", region_name="us-east-1")
        from stockstream.stream_processor import handler

        handler._s3 = s3
        yield {"s3": s3, "kinesis": kin, "bucket": BUCKET}
        handler._s3 = None


def kinesis_event(payloads: list, start_seq: int = 1) -> dict:
    """Build a Lambda Kinesis event from dicts (or raw bytes for malformed cases)."""
    records = []
    for i, p in enumerate(payloads):
        data = p if isinstance(p, bytes) else json.dumps(p).encode()
        records.append({"kinesis": {"sequenceNumber": f"{start_seq + i:056d}",
                                    "data": base64.b64encode(data).decode()}})
    return {"Records": records}
