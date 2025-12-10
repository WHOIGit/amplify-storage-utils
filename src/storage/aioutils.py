import asyncio

from storage.object import ObjectStore
from storage.utils import (
    RegexValidator,
    validate_http_url_key,
    DataTransformer,
    TextEncodingTransformer,
    GzipTransformer,
    BufferTransformer,
    Base64Transformer,
    JsonTransformer,
    KeyTransformer,
    ValidatingKeyTransformer,
    PrefixKeyTransformer,
    HashPrefixKeyTransformer,
    UrlEncodingKeyTransformer,
)

# Store implemtations that combine multiple stores in some way

class AsyncFanoutStore(ObjectStore):
    def __init__(self, children):
        self.children = children

    async def put(self, key, data):
        for child in self.children:
            await child.put(key, data)

    async def get(self, key):
        for child in self.children:
            if await child.exists(key):
                return await child.get(key)
        raise KeyError(key)

    async def exists(self, key):
        for child in self.children:
            if await child.exists(key):
                return True
        return False

    async def delete(self, key):
        for child in self.children:
            if await child.exists(key):
                await child.delete(key)

    async def keys(self):
        keys = set()
        for child in self.children:
            async for key in child.keys():
                keys.add(key)
        for key in keys:
            yield key


class AsyncCachingStore(ObjectStore):
    def __init__(self, main_store, cache_store):
        self.main_store = main_store
        self.cache_store = cache_store

    async def put(self, key, data):
        await self.main_store.put(key, data)
        await self.cache_store.put(key, data)

    async def get(self, key):
        if await self.cache_store.exists(key):
            return await self.cache_store.get(key)
        else:
            data = await self.main_store.get(key)
            await self.cache_store.put(key, data)
            return data

    async def exists(self, key):
        return await self.cache_store.exists(key) or await self.main_store.exists(key)

    async def delete(self, key):
        await self.main_store.delete(key)
        if await self.cache_store.exists(key):
            await self.cache_store.delete(key)

    async def keys(self):
        async for key in self.main_store.keys():
            yield key

    async def clear(self, key=None):
        if key is None:
            async for cached_key in self.cache_store.keys():
                await self.cache_store.delete(cached_key)
        elif await self.cache_store.exists(key):
            await self.cache_store.delete(key)


class AsyncTransformingStore(ObjectStore):
    """
    Async store that applies a DataTransformer before storing data
    and the reverse transformation after retrieving it.
    """

    def __init__(self, store, transformer=None, sync_transform=True):
        self.store = store
        self.transformer = transformer or DataTransformer()
        # set sync_transform to False if the transform/reverse_transform methods are CPU-bound (e.g., compression)
        self.sync_transform = sync_transform

    async def put(self, key, data):
        if self.sync_transform:
            transformed_data = self.transformer.transform(data)
        else:
            transformed_data = await asyncio.to_thread(self.transformer.transform, data)
        await self.store.put(key, transformed_data)

    async def get(self, key):
        data = await self.store.get(key)
        if self.sync_transform:
            return self.transformer.reverse_transform(data)
        else:
            return await asyncio.to_thread(self.transformer.reverse_transform, data)

    async def exists(self, key):
        return await self.store.exists(key)

    async def delete(self, key):
        return await self.store.delete(key)

    async def keys(self):
        async for key in self.store.keys():
            yield key


class AsyncTextEncodingStore(AsyncTransformingStore):
    """
    Async store that encodes and decodes data as text.
    """

    def __init__(self, store, encoding='utf-8'):
        super().__init__(store, TextEncodingTransformer(encoding), sync_transform=False)


class AsyncGzipStore(AsyncTransformingStore):
    """
    Async store that gzips data on write and ungzips on read.
    """

    def __init__(self, store):
        super().__init__(store, GzipTransformer(), sync_transform=False)


class AsyncBufferStore(AsyncTransformingStore):
    """
    Async store that takes file-like buffers on write and returns buffers on read.
    """

    def __init__(self, store):
        super().__init__(store, BufferTransformer())


class AsyncBase64Store(AsyncTransformingStore):
    """
    Async store that encodes and decodes data as base64.
    """

    def __init__(self, store):
        super().__init__(store, Base64Transformer(), sync_transform=False)


class AsyncJsonStore(AsyncTransformingStore):
    """
    Async store that serializes/deserializes JSON-compatible Python objects.
    """

    def __init__(self, store):
        super().__init__(store, JsonTransformer(), sync_transform=False)


class AsyncKeyTransformingStore(ObjectStore):
    """
    Async store that transforms keys before delegating to a backing store.
    """

    def __init__(self, store, transformer=None):
        self.store = store
        self.transformer = transformer or KeyTransformer()

    def transform_key(self, key):
        return self.transformer.transform_key(key)

    def reverse_transform_key(self, key):
        return self.transformer.reverse_transform_key(key)

    async def put(self, key, data):
        await self.store.put(self.transform_key(key), data)

    async def get(self, key):
        return await self.store.get(self.transform_key(key))

    async def exists(self, key):
        return await self.store.exists(self.transform_key(key))

    async def delete(self, key):
        return await self.store.delete(self.transform_key(key))

    async def keys(self):
        async for key in self.store.keys():
            yield self.reverse_transform_key(key)


class AsyncKeyValidatingStore(AsyncKeyTransformingStore):
    """
    Async key-transforming store that validates keys using a validator
    that either returns None or raises KeyError.
    """

    def __init__(self, store, validator):
        super().__init__(store, ValidatingKeyTransformer(validator))


class AsyncUrlValidatingStore(AsyncKeyValidatingStore):
    """
    Async store that validates keys as HTTP/HTTPS URLs.
    """

    def __init__(self, store):
        super().__init__(store, validate_http_url_key)


class AsyncRegexValidatingStore(AsyncKeyValidatingStore):
    """
    Async store that validates keys against a regex pattern.
    """

    def __init__(self, store, pattern):
        super().__init__(store, RegexValidator(pattern))


class AsyncPrefixStore(AsyncKeyTransformingStore):
    """
    Async store that prefixes keys with a given prefix.
    """

    def __init__(self, store, prefix):
        super().__init__(store, PrefixKeyTransformer(prefix))


class AsyncHashPrefixStore(AsyncKeyTransformingStore):
    """
    Async store that prefixes keys with a hash-derived prefix.
    """

    def __init__(self, store, hash_length=8, separator='/'):
        super().__init__(store, HashPrefixKeyTransformer(hash_length, separator))


class AsyncUrlEncodingStore(AsyncKeyTransformingStore):
    """
    Async store that URL-encodes/decodes keys while preserving slashes.
    """

    def __init__(self, store):
        super().__init__(store, UrlEncodingKeyTransformer())


# utility functions for multi-store actions

async def async_copy_store(from_store, to_store, overwrite=True):
    async for key in from_store.keys():
        if overwrite or not await to_store.exists(key):
            await to_store.put(key, await from_store.get(key))


async def async_clear_store(store):
    async for key in store.keys():
        await store.delete(key)


async def async_sync_stores(from_store, to_store, delete=False):
    async for key in from_store.keys():
        # copy if necessary
        if not await to_store.exists(key):
            await to_store.put(key, await from_store.get(key))
    if delete:
        # remove keys from to_store that are not in from_store
        async for key in to_store.keys():
            if not await from_store.exists(key):
                await to_store.delete(key)
