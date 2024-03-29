from storage.object import ObjectStore

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
