import os
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
        try:
            with open(self._path(key), 'rb') as f:
                return f.read()
        except FileNotFoundError:
            raise KeyError(key)
        
    def exists(self, key):
        return os.path.exists(self._path(key))
    
    def delete(self, key):
        try:
            os.remove(self._path(key))
            return True
        except FileNotFoundError:
            raise KeyError(key)

    def keys(self):
        return os.listdir(self.root_path)


def hashpath(key, width=2, depth=3):
    hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
    # split hash into chunks of size 'width' up to the specified depth
    path_components = [hash[i:i+width] for i in range(0, width*depth, width)]
    # append the remaining part of the hash as a single component
    path_components.append(hash[width*depth:])
    return os.path.join(*path_components)


class HashdirStore(FilesystemStore):
    def __init__(self, root_path, width=2, depth=3):
        self.root_path = root_path
        self.width = width
        self.depth = depth

    def _path(self, key):
        return os.path.join(self.root_path, hashpath(key, self.width, self.depth))
    
    def put(self, key, data):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)

    def keys(self):
        raise NotImplementedError
