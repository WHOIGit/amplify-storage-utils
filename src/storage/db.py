import sqlite3

from .object import ObjectStore


class SqliteStore(ObjectStore):
    def __init__(self, db_path):
        self.db_path = db_path

    def open(self):
        self.conn = sqlite3.connect(self.db_path)

    def close(self):
        self.conn.close()

    def __enter__(self):
        self.open()
        self.conn.execute('CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, data BLOB)')
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def put(self, key, data):
        self.conn.execute('INSERT OR REPLACE INTO objects (key, data) VALUES (?, ?)', (key, data))
        self.conn.commit()

    def get(self, key):
        cursor = self.conn.execute('SELECT data FROM objects WHERE key = ?', (key,))
        row = cursor.fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]
    
    def exists(self, key):
        cursor = self.conn.execute('SELECT 1 FROM objects WHERE key = ?', (key,))
        return cursor.fetchone() is not None
    
    def delete(self, key):
        cursor = self.conn.execute('DELETE FROM objects WHERE key = ?', (key,))
        if cursor.rowcount == 0:
            raise KeyError(key)
        self.conn.commit()

    def keys(self):
        cursor = self.conn.execute('SELECT key FROM objects')
        return [row[0] for row in cursor.fetchall()]
