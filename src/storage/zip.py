from zipfile import ZipFile

from storage.object import ObjectStore

class ZipStore(ObjectStore):
    """
    A partial ObjecStore implementation for Zip files.

    Use as a context manager to open a zip file for reading and writing.

    Deletion is not supported.
    """
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.zipfile = ZipFile(self.path, 'a')
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.zipfile.close()

    def put(self, key, data: bytearray):
        if self.exists(key):
            raise KeyError(f'zipfile entry for {key} already exists')
        self.zipfile.writestr(key, data)

    def get(self, key) -> bytearray:
        return self.zipfile.read(key)
    
    def exists(self, key):
        return key in self.zipfile.namelist()
    
    def delete(self, key):
        raise NotImplementedError('deleting from zip files is not supported')
    
    def keys(self, **kwargs):
        return self.zipfile.namelist()
