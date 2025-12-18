from abc import ABC, abstractmethod
from io import BytesIO

"""
The ObjectStore class is an abstract base class that defines the interface for an object store.

The DictStore class is a simple implementation of an object store that stores objects in a dictionary in memory.
"""

class ObjectStore(ABC):
    """
    An abstract base class for an object store.
    
    An object store is a system for storing and retrieving byte arrays (objects) using a unique key.
    
    The key is a unique identifier for the object, and can be used to retrieve the object from the store.
    
    The object store provides methods for storing, retrieving, and deleting objects, as well as checking if an object exists in the store.
    
    The object store may also provide a method for listing the keys of all objects in the store.
    
    Stateful implementations of the object store should provide a context manager interface, allowing the store to be used in a with statement.
    """
    @abstractmethod
    def get(self, key) -> bytes:
        """ return the data associated with the key"""
        pass

    @abstractmethod
    def put(self, key, data: bytes):
        """ store the data associated with the key from the file-like object data. """
        pass

    @abstractmethod
    def exists(self, key):
        """ return True if the key exists in the store, False otherwise. """
        pass

    @abstractmethod
    def delete(self, key):
        """ delete the data associated with the key. """
        pass

    def keys(self):
        """" return an iterable of keys for all objects in the store.
        classes that do not support listing keys should raise a NotImplementedError """
        raise NotImplementedError

    
class DictStore(ObjectStore):
    """
    Stores data in a dictionary in memory.
    """
    def __init__(self, objects=None):
        if objects is None:
            objects = dict()
        self.objects = objects

    def get(self, key):
        bytes = self.objects.get(key)
        if bytes is None:
            raise KeyError(key)
        return bytes

    def put(self, key, data):
        self.objects[key] = data

    def exists(self, key):
        return key in self.objects

    def delete(self, key):
        if key in self.objects:
            del self.objects[key]
        else:
            raise KeyError(key)

    def keys(self):
        return self.objects.keys()


class StoreError(Exception):
    """Base class for exceptions in this module."""
    pass
