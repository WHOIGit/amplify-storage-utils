import os

import aiofiles

from aiofiles import os as aios

from storage.aioutils import AsyncKeyTransformingStore
from storage.object import ObjectStore
from storage.fs import hashpath, FilesystemKeyTransformer


class AsyncFilesystemStore(ObjectStore):
    def __init__(self, root_path):
        self.root_path = root_path

    def _path(self, key):
        return os.path.join(self.root_path, key)

    async def put(self, key, data):
        async with aiofiles.open(self._path(key), 'wb') as f:
            await f.write(data)

    async def get(self, key):
        try:
            async with aiofiles.open(self._path(key), 'rb') as f:
                return await f.read()
        except FileNotFoundError:
            raise KeyError(key)
        
    async def exists(self, key):
        return await aios.path.exists(self._path(key))
    
    async def delete(self, key):
        try:
            await aios.remove(self._path(key))
            return True
        except FileNotFoundError:
            raise KeyError(key)

    async def keys(self):
        for dirpath, _, filenames in os.walk(self.root_path):
            for filename in filenames:
                yield os.path.relpath(os.path.join(dirpath, filename), self.root_path)
    

class AsyncHashdirStore(AsyncFilesystemStore):
    def __init__(self, root_path, width=2, depth=3):
        self.root_path = root_path
        self.width = width
        self.depth = depth

    def _path(self, key):
        return os.path.join(self.root_path, hashpath(key, self.width, self.depth))
    
    async def put(self, key, data):
        path = self._path(key)
        await aios.makedirs(os.path.dirname(path), exist_ok=True)
        async with aiofiles.open(path, 'wb') as f:
            await f.write(data)

    def keys(self):
        raise NotImplementedError("HashdirStore does not support listing keys")


class AsyncSafeFilesystemStore(AsyncKeyTransformingStore):
    def __init__(self, root_path):
        fs_store = AsyncFilesystemStore(root_path)
        transformer = FilesystemKeyTransformer()
        super().__init__(fs_store, transformer)
