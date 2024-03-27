from storage.object import ObjectStore

# Store implemtations that combine multiple stores in some way

class FanoutStore(ObjectStore):
    def __init__(self, children):
        self.children = children

    def put(self, key, data):
        for child in self.children:
            child.put(key, data)

    def get(self, key):
        for child in self.children:
            if child.exists(key):
                return child.get(key)
        raise KeyError(key)

    def exists(self, key):
        for child in self.children:
            if child.exists(key):
                return True
        return False

    def delete(self, key):
        for child in self.children:
            if child.exists(key):
                child.delete(key)

    def keys(self):
        keys = set()
        for child in self.children:
            keys.update(child.keys())
        return keys


class CachingStore(ObjectStore):
    def __init__(self, main_store, cache_store):
        self.main_store = main_store
        self.cache_store = cache_store

    def put(self, key, data):
        self.main_store.put(key, data)
        self.cache_store.put(key, data)

    def get(self, key):
        if self.cache_store.exists(key):
            return self.cache_store.get(key)
        else:
            data = self.main_store.get(key)
            self.cache_store.put(key, data)
            return data

    def exists(self, key):
        return self.cache_store.exists(key) or self.main_store.exists(key)

    def delete(self, key):
        self.main_store.delete(key)
        if self.cache_store.exists(key):
            self.cache_store.delete(key)

    def keys(self):
        return self.main_store.keys()

    def clear(self, key=None):
        if key is None:
            for cached_key in self.cache_store.keys():
                self.cache_store.delete(cached_key)
        elif self.cache_store.exists(key):
            self.cache_store.delete(key)

   
# utility functions for multi-store actions
            
def copy_store(from_store, to_store, overwrite=False):
    for key in from_store.keys():
        if overwrite or not to_store.exists(key):
            to_store.put(key, from_store.get(key))


def clear_store(store):
    for key in store.keys():
        store.delete(key)


def sync_stores(from_store, to_store, delete=False):
    from_keys = set(from_store.keys())
    to_keys = set(to_store.keys())
    for key in from_keys - to_keys:
        to_store.put(key, from_store.get(key))
    if delete:
        for key in to_keys - from_keys:
            to_store.delete(key)