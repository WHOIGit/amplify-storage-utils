import io
import pytest
import botocore
from unittest.mock import Mock, AsyncMock

from storage.s3 import BucketStore, AsyncBucketStore


class _AsyncBody:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self):
        return self._data


class _AsyncPageIterator:
    def __init__(self, pages):
        self._pages = list(pages)
        self._i = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._i >= len(self._pages):
            raise StopAsyncIteration
        page = self._pages[self._i]
        self._i += 1
        return page


def _client_error(code: str):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "boom"}},
        "HeadObject",
    )


def test_bucketstore_put_injects_client():
    client = Mock()
    store = BucketStore("bucket", client)

    assert store.put("k", b"data") is True
    client.put_object.assert_called_once_with(Bucket="bucket", Key="k", Body=b"data")


def test_bucketstore_get_reads_body():
    client = Mock()
    client.get_object.return_value = {"Body": io.BytesIO(b"abc")}
    store = BucketStore("bucket", client)

    assert store.get("k") == b"abc"
    client.get_object.assert_called_once_with(Bucket="bucket", Key="k")


def test_bucketstore_exists_true():
    client = Mock()
    store = BucketStore("bucket", client)

    assert store.exists("k") is True
    client.head_object.assert_called_once_with(Bucket="bucket", Key="k")


def test_bucketstore_exists_false_on_404():
    client = Mock()
    client.head_object.side_effect = _client_error("404")
    store = BucketStore("bucket", client)

    assert store.exists("k") is False
    client.head_object.assert_called_once_with(Bucket="bucket", Key="k")


def test_bucketstore_delete():
    client = Mock()
    store = BucketStore("bucket", client)

    assert store.delete("k") is True
    client.delete_object.assert_called_once_with(Bucket="bucket", Key="k")


def test_bucketstore_keys_uses_paginator():
    client = Mock()
    paginator = Mock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "a"}, {"Key": "b"}]},
        {"Contents": [{"Key": "c"}]},
    ]
    client.get_paginator.return_value = paginator
    store = BucketStore("bucket", client)

    assert list(store.keys(prefix="p/")) == ["a", "b", "c"]
    client.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(Bucket="bucket", Prefix="p/")


def test_bucketstore_presigned_get_delegates():
    client = Mock()
    client.generate_presigned_url.return_value = "url"
    store = BucketStore("bucket", client)

    assert store.presigned_get("k", expiry=12) == "url"
    client.generate_presigned_url.assert_called_once_with(
        "get_object", Params={"Key": "k", "Bucket": "bucket"}, ExpiresIn=12
    )


@pytest.mark.asyncio
async def test_asyncbucketstore_put_injects_client():
    client = Mock()
    client.put_object = AsyncMock()
    store = AsyncBucketStore("bucket", client)

    assert await store.put("k", b"data") is True
    client.put_object.assert_awaited_once_with(Bucket="bucket", Key="k", Body=b"data")


@pytest.mark.asyncio
async def test_asyncbucketstore_get_reads_body():
    client = Mock()
    client.get_object = AsyncMock(return_value={"Body": _AsyncBody(b"abc")})
    store = AsyncBucketStore("bucket", client)

    assert await store.get("k") == b"abc"
    client.get_object.assert_awaited_once_with(Bucket="bucket", Key="k")


@pytest.mark.asyncio
async def test_asyncbucketstore_exists_false_on_nosuchkey():
    client = Mock()
    client.head_object = AsyncMock(side_effect=_client_error("NoSuchKey"))
    store = AsyncBucketStore("bucket", client)

    assert await store.exists("k") is False
    client.head_object.assert_awaited_once_with(Bucket="bucket", Key="k")


@pytest.mark.asyncio
async def test_asyncbucketstore_keys_uses_async_paginator():
    client = Mock()
    paginator = Mock()
    paginator.paginate.return_value = _AsyncPageIterator(
        [{"Contents": [{"Key": "a"}]}, {"Contents": [{"Key": "b"}, {"Key": "c"}]}]
    )
    client.get_paginator.return_value = paginator
    store = AsyncBucketStore("bucket", client)

    got = []
    async for k in store.keys(prefix="p/"):
        got.append(k)

    assert got == ["a", "b", "c"]
    client.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(Bucket="bucket", Prefix="p/")


@pytest.mark.asyncio
async def test_asyncbucketstore_presigned_get_delegates_without_awaiting_client_method():
    client = Mock()
    client.generate_presigned_url.return_value = "url"
    store = AsyncBucketStore("bucket", client)

    assert store.presigned_get("k", expiry=12) == "url"
    client.generate_presigned_url.assert_called_once_with(
        "get_object", Params={"Key": "k", "Bucket": "bucket"}, ExpiresIn=12
    )
