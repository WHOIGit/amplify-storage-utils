import os
import sqlite3
import hashlib

from storage.object import ObjectStore


class FilesystemStore(ObjectStore):
    def __init__(self, root_path):
        self.root_path = root_path

    def _path(self, key):
        return os.path.join(self.root_path, key)

    def put(self, key, data):
        with open(self._path(key), 'wb') as f:
            f.write(data)

    def get(self, key):
        with open(self._path(key), 'rb') as f:
            return f.read()
        
    def exists(self, key):
        return os.path.exists(self._path(key))
    
    def delete(self, key):
        os.remove(self._path(key))

    def keys(self):
        return os.listdir(self.root_path)


class HashdirStore(ObjectStore):
    def __init__(self, root_path):
        self.root_path = root_path

    def _path(self, key):
        hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
        # split hash into chunks
        return os.path.join(self.root_path, *hash[:4], *hash[4:8], hash[8:])
    
    def put(self, key, data):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)

    def get(self, key):
        with open(self._path(key), 'rb') as f:
            return f.read()
        
    def exists(self, key):
        return os.path.exists(self._path(key))
    
    def delete(self, key):
        os.remove(self._path(key))