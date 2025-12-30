from storage.object import ObjectStore

import aiosqlite

class AsyncSqliteStore(ObjectStore):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    async def open(self):
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute('CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, data BLOB)')
        await self.conn.commit()

    async def close(self):
        await self.conn.close()
        self.conn = None

    async def __aenter__(self):
        await self.open()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def put(self, key, data):
        await self.conn.execute('INSERT OR REPLACE INTO objects (key, data) VALUES (?, ?)', (key, data))
        await self.conn.commit()

    async def get(self, key):
        cursor = await self.conn.execute('SELECT data FROM objects WHERE key = ?', (key,))
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]
    
    async def exists(self, key):
        cursor = await self.conn.execute('SELECT 1 FROM objects WHERE key = ?', (key,))
        return (await cursor.fetchone()) is not None
    
    async def delete(self, key):
        cursor = await self.conn.execute('DELETE FROM objects WHERE key = ?', (key,))
        await self.conn.commit()
        if cursor.rowcount == 0:
            raise KeyError(key)

    async def keys(self, **kwargs):
        cursor = await self.conn.execute('SELECT key FROM objects')
        async for row in cursor:
            yield row[0]
    
    async def clear(self):
        await self.conn.execute('DELETE FROM objects')
        await self.conn.commit()


