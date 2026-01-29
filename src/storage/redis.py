import redis
import redis.asyncio

from .object import ObjectStore


class RedisStore(ObjectStore):
    def __init__(self, client=None):
        self.client = client

    def set_client(self, client):
        """Inject a Redis client instance."""
        self.client = client

    def put(self, key, data):
        self.client.set(key, data)
        return True

    def get(self, key):
        data = self.client.get(key)
        if data is None:
            raise KeyError(key)
        return data

    def exists(self, key):
        return self.client.exists(key) > 0

    def delete(self, key):
        self.client.delete(key)
        return True

    def keys(self, pattern='*', **kwargs):
        for key in self.client.scan_iter(match=pattern, **kwargs):
            yield key


class AsyncRedisStore(ObjectStore):
    def __init__(self, client=None):
        self.client = client

    def set_client(self, client):
        """Inject an async Redis client instance."""
        self.client = client

    async def put(self, key, data):
        await self.client.set(key, data)
        return True

    async def get(self, key):
        data = await self.client.get(key)
        if data is None:
            raise KeyError(key)
        return data

    async def exists(self, key):
        return await self.client.exists(key) > 0

    async def delete(self, key):
        await self.client.delete(key)
        return True

    async def keys(self, pattern='*', **kwargs):
        async for key in self.client.scan_iter(match=pattern, **kwargs):
            yield key
