"""In-memory S3 for long local simulations.

moto's S3 lists objects by scanning every key, so listing gets slower as the lake grows
(a month of simulated data meant tens of thousands of keys scanned per hourly job).
This fake keeps keys in a sorted list and answers prefix listings with bisect. It
implements only the calls the pipeline uses, and it DOES enforce the 1000-key
DeleteObjects limit that moto skips (see BUG-12).
"""

from __future__ import annotations

import bisect
import io


class _Paginator:
    def __init__(self, s3: FakeS3) -> None:
        self.s3 = s3

    def paginate(self, Bucket: str, Prefix: str = ""):  # noqa: N803
        keys = self.s3._keys
        i = bisect.bisect_left(keys, Prefix)
        page = []
        while i < len(keys) and keys[i].startswith(Prefix):
            page.append({"Key": keys[i]})
            if len(page) == 1000:
                yield {"Contents": page}
                page = []
            i += 1
        yield {"Contents": page} if page else {}


class FakeS3:
    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}
        self._keys: list[str] = []

    def create_bucket(self, Bucket: str, **_) -> None:  # noqa: N803
        pass

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        if Key not in self._objects:
            bisect.insort(self._keys, Key)
        self._objects[Key] = Body

    def get_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803
        return {"Body": io.BytesIO(self._objects[Key])}

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self)

    def delete_objects(self, Bucket: str, Delete: dict) -> None:  # noqa: N803
        if len(Delete["Objects"]) > 1000:
            raise ValueError("MalformedXML: DeleteObjects accepts at most 1000 keys")
        for obj in Delete["Objects"]:
            if self._objects.pop(obj["Key"], None) is not None:
                self._keys.pop(bisect.bisect_left(self._keys, obj["Key"]))
