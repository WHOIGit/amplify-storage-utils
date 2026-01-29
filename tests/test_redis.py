import pytest
from unittest.mock import Mock, AsyncMock

from storage.redis import RedisStore, AsyncRedisStore


class _AsyncScanIter:
    def __init__(self, keys):
        self._keys = list(keys)
        self._i = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._i >= len(self._keys):
            raise StopAsyncIteration
        key = self._keys[self._i]
        self._i += 1
        return key


def test_redisstore_put():
    client = Mock()
    store = RedisStore(client)
    assert store.put("k", b"data") is True
    client.set.assert_called_once_with("k", b"data")


def test_redisstore_get():
    client = Mock()
    client.get.return_value = b"abc"
    store = RedisStore(client)
    assert store.get("k") == b"abc"


def test_redisstore_get_missing_raises():
    client = Mock()
    client.get.return_value = None
    store = RedisStore(client)
    with pytest.raises(KeyError):
        store.get("missing")


def test_redisstore_exists():
    client = Mock()
    client.exists.return_value = 1
    store = RedisStore(client)
    assert store.exists("k") is True


def test_redisstore_delete():
    client = Mock()
    store = RedisStore(client)
    assert store.delete("k") is True
    client.delete.assert_called_once_with("k")


def test_redisstore_keys():
    client = Mock()
    client.scan_iter.return_value = iter([b"a", b"b", b"c"])
    store = RedisStore(client)
    assert list(store.keys(pattern="test:*")) == [b"a", b"b", b"c"]
    client.scan_iter.assert_called_once_with(match="test:*")


def test_redisstore_set_client():
    store = RedisStore()
    client = Mock()
    store.set_client(client)
    assert store.client is client


@pytest.mark.asyncio
async def test_asyncredisstore_put():
    client = Mock()
    client.set = AsyncMock()
    store = AsyncRedisStore(client)
    assert await store.put("k", b"data") is True
    client.set.assert_awaited_once_with("k", b"data")


@pytest.mark.asyncio
async def test_asyncredisstore_get():
    client = Mock()
    client.get = AsyncMock(return_value=b"abc")
    store = AsyncRedisStore(client)
    assert await store.get("k") == b"abc"


@pytest.mark.asyncio
async def test_asyncredisstore_get_missing_raises():
    client = Mock()
    client.get = AsyncMock(return_value=None)
    store = AsyncRedisStore(client)
    with pytest.raises(KeyError):
        await store.get("missing")


@pytest.mark.asyncio
async def test_asyncredisstore_exists():
    client = Mock()
    client.exists = AsyncMock(return_value=1)
    store = AsyncRedisStore(client)
    assert await store.exists("k") is True


@pytest.mark.asyncio
async def test_asyncredisstore_keys():
    client = Mock()
    client.scan_iter.return_value = _AsyncScanIter([b"a", b"b"])
    store = AsyncRedisStore(client)
    got = [k async for k in store.keys(pattern="*")]
    assert got == [b"a", b"b"]
