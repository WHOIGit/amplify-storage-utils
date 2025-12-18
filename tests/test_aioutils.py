import pytest
from io import BytesIO
import base64

from storage.utils import DataTransformer, KeyTransformer
from storage.aioutils import (
    AsyncFanoutStore,
    AsyncCachingStore,
    AsyncTransformingStore,
    AsyncTextEncodingStore,
    AsyncGzipStore,
    AsyncBufferStore,
    AsyncBase64Store,
    AsyncJsonStore,
    AsyncKeyTransformingStore,
    AsyncKeyValidatingStore,
    AsyncUrlValidatingStore,
    AsyncRegexValidatingStore,
    AsyncPrefixStore,
    AsyncHashPrefixStore,
    AsyncUrlEncodingStore,
    async_copy_store,
    async_clear_store,
    async_sync_stores,
)

pytestmark = pytest.mark.asyncio


class AsyncDictStore:
    """Simple in-memory async store used for tests."""

    def __init__(self):
        self._data = {}

    async def put(self, key, data):
        self._data[key] = data

    async def get(self, key):
        if key not in self._data:
            raise KeyError(key)
        return self._data[key]

    async def exists(self, key):
        return key in self._data

    async def delete(self, key):
        self._data.pop(key, None)

    async def keys(self):
        # yield a snapshot so mutations during iteration are safe
        for key in list(self._data.keys()):
            yield key


class AsyncContextTrackingStore(AsyncDictStore):
    """Async store that tracks context manager calls."""

    def __init__(self):
        super().__init__()
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True


@pytest.fixture
def store():
    return AsyncDictStore()


@pytest.fixture
def test_data():
    return b"test data"


@pytest.fixture
def test_key():
    return "test_key"


class TestAsyncFanoutStore:
    @pytest.fixture
    def fanout_stores(self):
        return [AsyncDictStore(), AsyncDictStore(), AsyncDictStore()]

    async def test_fanout_operations(self, fanout_stores, test_key, test_data):
        fanout_store = AsyncFanoutStore(fanout_stores)

        # put to all children
        await fanout_store.put(test_key, test_data)
        for s in fanout_stores:
            assert await s.get(test_key) == test_data

        # get from first that has the key
        assert await fanout_store.get(test_key) == test_data

        # exists across children
        assert await fanout_store.exists(test_key)

        # delete from all
        await fanout_store.delete(test_key)
        for s in fanout_stores:
            assert not await s.exists(test_key)

        # keys = union of all children
        keys = ["key1", "key2", "key3"]
        for i, key in enumerate(keys):
            await fanout_stores[i].put(key, test_data)
        result_keys = {k async for k in fanout_store.keys()}
        assert result_keys == set(keys)


class TestAsyncCachingStore:
    @pytest.fixture
    def cache_stores(self):
        return AsyncDictStore(), AsyncDictStore()  # main_store, cache_store

    async def test_caching_behavior(self, cache_stores, test_key, test_data):
        main_store, cache_store = cache_stores
        caching_store = AsyncCachingStore(main_store, cache_store)

        # put writes to both stores
        await caching_store.put(test_key, test_data)
        assert await main_store.get(test_key) == test_data
        assert await cache_store.get(test_key) == test_data

        # get uses cache when available
        modified_data = b"modified data"
        await main_store.put(test_key, modified_data)
        assert await caching_store.get(test_key) == test_data  # returns cached data

        # get populates cache when needed
        new_key = "new_key"
        await main_store.put(new_key, test_data)
        assert not await cache_store.exists(new_key)
        retrieved = await caching_store.get(new_key)
        assert retrieved == test_data
        assert await cache_store.exists(new_key)

        # clear full cache
        await caching_store.clear()
        assert not await cache_store.exists(test_key)
        assert await main_store.exists(test_key)

        # selective clear
        await caching_store.put(test_key, test_data)
        await caching_store.put(new_key, test_data)
        await caching_store.clear(test_key)
        assert not await cache_store.exists(test_key)
        assert await cache_store.exists(new_key)


class TestAsyncTransformingStore:
    async def test_basic_transform(self, store, test_key):
        class UppercaseTransformer(DataTransformer):
            def transform(self, data):
                return data.upper()

            def reverse_transform(self, data):
                return data.lower()

        transform_store = AsyncTransformingStore(store, UppercaseTransformer())
        test_str = b"hello world"

        await transform_store.put(test_key, test_str)
        assert await store.get(test_key) == b"HELLO WORLD"
        assert await transform_store.get(test_key) == b"hello world"


class TestAsyncTextEncodingStore:
    async def test_text_encoding(self, store, test_key):
        text_store = AsyncTextEncodingStore(store)
        text = "Hello, 世界"

        await text_store.put(test_key, text)
        assert await text_store.get(test_key) == text
        stored = await store.get(test_key)
        assert isinstance(stored, (bytes, bytearray))


class TestAsyncGzipStore:
    async def test_compression(self, store, test_key):
        gzip_store = AsyncGzipStore(store)
        data = b"test " * 1000  # large enough to compress

        await gzip_store.put(test_key, data)
        compressed = await store.get(test_key)
        assert len(compressed) < len(data)
        assert await gzip_store.get(test_key) == data


class TestAsyncBufferStore:
    async def test_buffer_interface(self, store, test_key, test_data):
        buffer_store = AsyncBufferStore(store)
        buf = BytesIO(test_data)

        await buffer_store.put(test_key, buf)
        retrieved = await buffer_store.get(test_key)
        assert isinstance(retrieved, BytesIO)
        assert retrieved.read() == test_data


class TestAsyncBase64Store:
    async def test_base64_encoding(self, store, test_key, test_data):
        base64_store = AsyncBase64Store(store)

        await base64_store.put(test_key, test_data)
        stored = await store.get(test_key)

        # validate we stored valid base64
        try:
            base64.b64decode(stored)
        except Exception:
            pytest.fail("Stored data is not valid base64")

        assert await base64_store.get(test_key) == test_data


class TestAsyncJsonStore:
    async def test_json_serialization(self, store, test_key):
        json_store = AsyncJsonStore(store)
        obj = {"name": "test", "values": [1, 2, 3]}

        await json_store.put(test_key, obj)
        assert await json_store.get(test_key) == obj


class TestAsyncKeyTransformingStore:
    async def test_key_transformation(self, store, test_key, test_data):
        class ReverseKeyTransformer(KeyTransformer):
            def transform_key(self, key):
                return key[::-1]

            def reverse_transform_key(self, key):
                return key[::-1]

        key_store = AsyncKeyTransformingStore(store, ReverseKeyTransformer())

        await key_store.put(test_key, test_data)
        reversed_key = test_key[::-1]
        assert await store.exists(reversed_key)
        assert await key_store.get(test_key) == test_data
        assert await key_store.exists(test_key)

        await key_store.delete(test_key)
        assert not await store.exists(reversed_key)

        # keys() should yield original key
        await key_store.put(test_key, test_data)
        keys = [k async for k in key_store.keys()]
        assert keys == [test_key]


class TestAsyncPrefixStore:
    async def test_key_prefixing(self, store, test_key, test_data):
        prefix = "prefix/"
        prefix_store = AsyncPrefixStore(store, prefix)

        await prefix_store.put(test_key, test_data)
        assert await store.exists(prefix + test_key)
        assert await prefix_store.get(test_key) == test_data

        keys = [k async for k in prefix_store.keys()]
        assert keys == [test_key]


class TestAsyncUrlEncodingStore:
    async def test_url_encoding(self, store, test_data):
        url_store = AsyncUrlEncodingStore(store)
        key = "path/with spaces/and#special&chars"
        encoded_key = "path/with%20spaces/and%23special%26chars"

        await url_store.put(key, test_data)
        assert await store.exists(encoded_key)
        assert await url_store.get(key) == test_data

        keys = [k async for k in url_store.keys()]
        assert keys == [key]


class TestAsyncHashPrefixStore:
    async def test_hash_prefix(self, store, test_key, test_data):
        hash_store = AsyncHashPrefixStore(store, hash_length=4, separator="/")

        await hash_store.put(test_key, test_data)
        assert await hash_store.get(test_key) == test_data

        stored_keys = [k async for k in store.keys()]
        assert len(stored_keys) == 1
        stored_key = stored_keys[0]
        assert len(stored_key.split("/")[0]) == 4
        assert stored_key.endswith(test_key)

        keys = [k async for k in hash_store.keys()]
        assert keys == [test_key]


class TestAsyncKeyValidatingStore:
    async def test_key_validation(self, store, test_data):
        def validator(key):
            if not key.startswith("valid_"):
                raise KeyError(f"Invalid key: {key}")

        validating_store = AsyncKeyValidatingStore(store, validator)

        await validating_store.put("valid_key", test_data)
        assert await validating_store.get("valid_key") == test_data

        with pytest.raises(KeyError):
            await validating_store.put("invalid_key", test_data)


class TestAsyncRegexValidatingStore:
    async def test_regex_validation(self, store, test_data):
        pattern = r"^[a-z]+_\d+$"
        regex_store = AsyncRegexValidatingStore(store, pattern)

        await regex_store.put("test_123", test_data)
        assert await regex_store.get("test_123") == test_data

        with pytest.raises(KeyError):
            await regex_store.put("INVALID_123", test_data)
        with pytest.raises(KeyError):
            await regex_store.put("test-123", test_data)


class TestAsyncUrlValidatingStore:
    async def test_url_validation(self, store, test_data):
        url_store = AsyncUrlValidatingStore(store)

        await url_store.put("http://example.com", test_data)
        await url_store.put("https://example.com/path", test_data)

        with pytest.raises(KeyError):
            await url_store.put("not_a_url", test_data)
        with pytest.raises(KeyError):
            await url_store.put("ftp://example.com", test_data)


class TestAsyncSafeFilesystemStore:
    async def test_safe_filesystem_store(self, tmp_path, test_key, test_data):
        from storage.aiofs import AsyncSafeFilesystemStore

        store = AsyncSafeFilesystemStore(str(tmp_path))

        # test that keys with arbitrary Unicode characters round-trip
        complex_key = "path/with Spaces/特殊字符/and#special&chars"
        await store.put(complex_key, test_data)
        assert await store.get(complex_key) == test_data
        assert await store.exists(complex_key)

        keys = [k async for k in store.keys()]
        assert keys == [complex_key]


async def test_async_copy_store(test_data):
    source = AsyncDictStore()
    target = AsyncDictStore()
    keys = ["key1", "key2", "key3"]

    for key in keys:
        await source.put(key, test_data)

    # copy without overwrite
    await async_copy_store(source, target)
    for key in keys:
        assert await target.get(key) == test_data

    # copy with overwrite=False
    modified = b"modified"
    await target.put(keys[0], modified)
    await async_copy_store(source, target, overwrite=False)
    assert await target.get(keys[0]) == modified


async def test_async_clear_store(test_data):
    store = AsyncDictStore()
    keys = ["key1", "key2", "key3"]
    for key in keys:
        await store.put(key, test_data)

    await async_clear_store(store)
    remaining = [k async for k in store.keys()]
    assert remaining == []


async def test_async_sync_stores(test_data):
    source = AsyncDictStore()
    target = AsyncDictStore()

    await source.put("key1", test_data)
    await source.put("key2", test_data)
    await target.put("key3", test_data)

    # sync without delete
    await async_sync_stores(source, target, delete=False)
    assert await target.get("key1") == test_data
    assert await target.get("key2") == test_data
    assert await target.get("key3") == test_data

    # sync with delete
    await async_sync_stores(source, target, delete=True)
    assert not await target.exists("key3")
    target_keys = {k async for k in target.keys()}
    source_keys = {k async for k in source.keys()}
    assert target_keys == source_keys


class TestAsyncContextManagers:
    """Tests that verify wrapper stores properly propagate async context manager calls."""

    async def test_prefix_store_context_manager(self, test_key, test_data):
        base_store = AsyncContextTrackingStore()
        prefix_store = AsyncPrefixStore(base_store, "prefix/")

        assert not base_store.entered
        assert not base_store.exited

        async with prefix_store as store:
            assert base_store.entered
            assert not base_store.exited
            await store.put(test_key, test_data)

        assert base_store.entered
        assert base_store.exited

    async def test_transforming_store_context_manager(self, test_key, test_data):
        base_store = AsyncContextTrackingStore()
        transform_store = AsyncGzipStore(base_store)

        assert not base_store.entered
        assert not base_store.exited

        async with transform_store as store:
            assert base_store.entered
            assert not base_store.exited
            await store.put(test_key, test_data)

        assert base_store.entered
        assert base_store.exited

    async def test_fanout_store_context_manager(self, test_key, test_data):
        child1 = AsyncContextTrackingStore()
        child2 = AsyncContextTrackingStore()
        child3 = AsyncContextTrackingStore()
        fanout_store = AsyncFanoutStore([child1, child2, child3])

        assert not child1.entered and not child2.entered and not child3.entered

        async with fanout_store as store:
            assert child1.entered and child2.entered and child3.entered
            assert not child1.exited and not child2.exited and not child3.exited
            await store.put(test_key, test_data)

        assert child1.exited and child2.exited and child3.exited

    async def test_caching_store_context_manager(self, test_key, test_data):
        main_store = AsyncContextTrackingStore()
        cache_store = AsyncContextTrackingStore()
        caching_store = AsyncCachingStore(main_store, cache_store)

        assert not main_store.entered and not cache_store.entered

        async with caching_store as store:
            assert main_store.entered and cache_store.entered
            assert not main_store.exited and not cache_store.exited
            await store.put(test_key, test_data)

        assert main_store.exited and cache_store.exited

    async def test_nested_wrapper_context_manager(self, test_key, test_data):
        """Test that nested wrappers properly propagate context manager calls."""
        base_store = AsyncContextTrackingStore()
        prefix_store = AsyncPrefixStore(base_store, "prefix/")
        gzip_store = AsyncGzipStore(prefix_store)

        assert not base_store.entered

        async with gzip_store as store:
            assert base_store.entered
            await store.put(test_key, test_data)

        assert base_store.exited

    async def test_prefix_store_with_sqlite(self, tmp_path, test_key, test_data):
        """Test AsyncPrefixStore wrapping AsyncSqliteStore."""
        from storage.aiodb import AsyncSqliteStore

        db_path = str(tmp_path / "test.db")
        base_store = AsyncSqliteStore(db_path)
        prefix_store = AsyncPrefixStore(base_store, "prefix/")

        async with prefix_store as store:
            # Should be able to perform operations without error
            await store.put(test_key, test_data)
            assert await store.exists(test_key)
            retrieved = await store.get(test_key)
            assert retrieved == test_data

        # Verify the connection was properly closed
        assert base_store.conn is None

    async def test_fanout_store_partial_failure_cleanup(self):
        """Test that AsyncFanoutStore cleans up already-entered stores if a later store fails."""

        class FailingStore:
            """Store that fails during __aenter__."""
            async def __aenter__(self):
                raise RuntimeError("Failed to initialize")

            async def __aexit__(self, exc_type, exc, tb):
                pass

        store1 = AsyncContextTrackingStore()
        store2 = AsyncContextTrackingStore()
        failing_store = FailingStore()

        fanout = AsyncFanoutStore([store1, store2, failing_store])

        # Try to enter the fanout store - should fail
        with pytest.raises(RuntimeError, match="Failed to initialize"):
            async with fanout:
                pass

        # Verify that store1 and store2 were entered AND then exited (cleaned up)
        assert store1.entered
        assert store1.exited  # This is the key - cleanup happened!
        assert store2.entered
        assert store2.exited  # This is the key - cleanup happened!

    async def test_caching_store_partial_failure_cleanup(self):
        """Test that AsyncCachingStore cleans up main_store if cache_store fails."""

        class FailingStore:
            """Store that fails during __aenter__."""
            async def __aenter__(self):
                raise RuntimeError("Cache unavailable")

            async def __aexit__(self, exc_type, exc, tb):
                pass

        main_store = AsyncContextTrackingStore()
        cache_store = FailingStore()

        caching = AsyncCachingStore(main_store, cache_store)

        # Try to enter the caching store - should fail
        with pytest.raises(RuntimeError, match="Cache unavailable"):
            async with caching:
                pass

        # Verify that main_store was entered AND then exited (cleaned up)
        assert main_store.entered
        assert main_store.exited  # This is the key - cleanup happened!
