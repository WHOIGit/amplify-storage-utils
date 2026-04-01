import asyncio
import sys
from contextlib import AsyncExitStack

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
        self._exit_stack = None

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        try:
            for i, child in enumerate(self.children):
                if hasattr(child, '__aenter__'):
                    self.children[i] = await self._exit_stack.enter_async_context(child)
            return self
        except:
            # If any child fails to enter, close all previously entered children
            await self._exit_stack.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, exc_type, exc, tb):
        if self._exit_stack is not None:
            await self._exit_stack.__aexit__(exc_type, exc, tb)

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

    async def keys(self, **kwargs):
        keys = set()
        for child in self.children:
            async for key in child.keys(**kwargs):
                keys.add(key)
        for key in keys:
            yield key


class AsyncCachingStore(ObjectStore):
    def __init__(self, main_store, cache_store):
        self.main_store = main_store
        self.cache_store = cache_store
        self._exit_stack = None

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        try:
            if hasattr(self.main_store, '__aenter__'):
                self.main_store = await self._exit_stack.enter_async_context(self.main_store)
            if hasattr(self.cache_store, '__aenter__'):
                self.cache_store = await self._exit_stack.enter_async_context(self.cache_store)
            return self
        except:
            # If cache store fails to enter, ensure main store is closed
            await self._exit_stack.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, exc_type, exc, tb):
        if self._exit_stack is not None:
            await self._exit_stack.__aexit__(exc_type, exc, tb)

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

    async def keys(self, **kwargs):
        async for key in self.main_store.keys(**kwargs):
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

    async def __aenter__(self):
        if hasattr(self.store, '__aenter__'):
            self.store = await self.store.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if hasattr(self.store, '__aexit__'):
            await self.store.__aexit__(exc_type, exc, tb)

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

    async def keys(self, **kwargs):
        async for key in self.store.keys(**kwargs):
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

    async def __aenter__(self):
        if hasattr(self.store, '__aenter__'):
            self.store = await self.store.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if hasattr(self.store, '__aexit__'):
            await self.store.__aexit__(exc_type, exc, tb)

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

    async def keys(self, **kwargs):
        # skip any keys where reverse transformation fails
        async for key in self.store.keys(**kwargs):
            try:
                yield self.reverse_transform_key(key)
            except ValueError:
                continue


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


class AsyncRegexRoutingStore(ObjectStore):
    """
    Async store that selects a KeyTransformer based on regex matching of the key.
    Routes are checked in order and the first matching regex wins.

    Each route is a (pattern, transformer) tuple where pattern is a regex string
    and transformer is a KeyTransformer instance.
    """

    def __init__(self, store, routes=None):
        import re
        self.store = store
        self.routes = [(re.compile(p), t) for p, t in (routes or [])]

    def add_route(self, pattern, transformer):
        import re
        self.routes.append((re.compile(pattern), transformer))

    async def __aenter__(self):
        if hasattr(self.store, '__aenter__'):
            self.store = await self.store.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if hasattr(self.store, '__aexit__'):
            await self.store.__aexit__(exc_type, exc, tb)

    def _match(self, key):
        for pattern, transformer in self.routes:
            if pattern.search(key):
                return transformer
        raise KeyError(f'no route matches key: {key}')

    def _reverse_match(self, stored_key):
        for pattern, transformer in self.routes:
            try:
                original = transformer.reverse_transform_key(stored_key)
            except (ValueError, KeyError):
                continue
            if pattern.search(original):
                return original
        return None

    async def put(self, key, data):
        transformer = self._match(key)
        await self.store.put(transformer.transform_key(key), data)

    async def get(self, key):
        transformer = self._match(key)
        return await self.store.get(transformer.transform_key(key))

    async def exists(self, key):
        try:
            transformer = self._match(key)
        except KeyError:
            return False
        return await self.store.exists(transformer.transform_key(key))

    async def delete(self, key):
        transformer = self._match(key)
        await self.store.delete(transformer.transform_key(key))

    async def keys(self, **kwargs):
        seen = set()
        async for key in self.store.keys(**kwargs):
            original = self._reverse_match(key)
            if original is not None and original not in seen:
                seen.add(original)
                yield original


class AsyncRoutingStore(ObjectStore):
    """
    Async store that routes operations to child stores based on key prefixes.
    Routes are checked in order and the first matching prefix wins.

    Each route is a (prefix, store) or (prefix, store, strip_prefix) tuple.
    When strip_prefix is True the prefix is removed from the key before it is
    passed to the child store, and re-added to keys returned by keys().
    """

    def __init__(self, routes=None):
        self.routes = [self._normalise(r) for r in (routes or [])]
        self._exit_stack = None

    @staticmethod
    def _normalise(route):
        if len(route) == 2:
            prefix, store = route
            return (prefix, store, False)
        return tuple(route)

    def add_route(self, prefix, store, strip_prefix=False):
        self.routes.append((prefix, store, strip_prefix))

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        try:
            for i, (prefix, store, strip) in enumerate(self.routes):
                if hasattr(store, '__aenter__'):
                    self.routes[i] = (prefix, await self._exit_stack.enter_async_context(store), strip)
            return self
        except:
            await self._exit_stack.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, exc_type, exc, tb):
        if self._exit_stack is not None:
            await self._exit_stack.__aexit__(exc_type, exc, tb)

    def _route(self, key):
        for prefix, store, strip in self.routes:
            if key.startswith(prefix):
                routed_key = key[len(prefix):] if strip else key
                return store, routed_key
        raise KeyError(key)

    async def put(self, key, data):
        store, routed_key = self._route(key)
        await store.put(routed_key, data)

    async def get(self, key):
        store, routed_key = self._route(key)
        return await store.get(routed_key)

    async def exists(self, key):
        try:
            store, routed_key = self._route(key)
        except KeyError:
            return False
        return await store.exists(routed_key)

    async def delete(self, key):
        store, routed_key = self._route(key)
        await store.delete(routed_key)

    async def keys(self, **kwargs):
        seen = set()
        for prefix, store, strip in self.routes:
            async for key in store.keys(**kwargs):
                full_key = prefix + key if strip else key
                if full_key not in seen:
                    seen.add(full_key)
                    yield full_key


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
