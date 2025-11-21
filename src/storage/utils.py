from abc import abstractmethod
from base64 import b64decode, b64encode
import gzip
import hashlib
from io import BytesIO
import json
import logging
from storage.object import ObjectStore
import re
from urllib.parse import urlparse, quote, unquote


# Composable store implementations


class IdentityStore(ObjectStore):
    """a no-op store fronting a backing store"""

    def __init__(self, store):
        self.store = store

    def put(self, key, data):
        self.store.put(key, data)

    def get(self, key):
        return self.store.get(key)
    
    def exists(self, key):
        return self.store.exists(key)
    
    def delete(self, key):
        return self.store.delete(key)
    
    def keys(self):
        return self.store.keys()
    

class ReadonlyStore(IdentityStore):

    def put(self, key, data):
        raise NotImplementedError('store is read-only')

    def delete(self, key):
        raise NotImplementedError('store is read-only')
    

class WriteonlyStore(IdentityStore):
    """
    A store that only supports write operations.
    """

    def get(self, key):
        raise NotImplementedError('store is write-only')

    def exists(self, key):
        raise NotImplementedError('store is write-only')

    def keys(self):
        raise NotImplementedError('store is write-only')
    

class MirroringStore(ObjectStore):
    """
    A store that forwards operations to a list of child stores.
    """

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
    """
    A store that fronts another store with a cache
    """

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
            to_delete = []
            for cached_key in self.cache_store.keys():
                to_delete.append(cached_key)
            for cached_key in to_delete:
                self.cache_store.delete(cached_key)
        elif self.cache_store.exists(key):
            self.cache_store.delete(key)


## TODO caching store that uses a LRU cache
## TODO caching store that uses a TTL cache


class NotifyingStore(IdentityStore):
    PUT = 'put'
    DELETE = 'delete'

    def __init__(self, store):
        super().__init__(store)
        self.handlers = []

    def on_change(self, handler):
        self.handlers.append(handler)

    def on_put(self, handler):
        def wrapped(store, action, key, e):
            if action == self.PUT:
                handler(store, key, e)
        self.handlers.append(wrapped)

    def on_delete(self, handler):
        def wrapped(store, action, key, e):
            if action == self.DELETE:
                handler(store, key, e)
        self.handlers.append(wrapped)

    def put(self, key, data):
        e = None
        try:
            self.store.put(key, data)
        except Exception as e:
            pass
        for handler in self.handlers:
            handler(self.store, self.PUT, key, e)
        if e is not None:
            raise e

    def delete(self, key):
        e = None
        try:
            self.store.delete(key)
        except Exception as e:
            pass
        for handler in self.handlers:
            handler(self.store, self.DELETE, key, e)
        if e is not None:
            raise e


class LoggingStore(ObjectStore):

    def __init__(self, store, store_name='store', logger=logging.getLogger()):
        self.store = store
        self.logger = logger
        self.store_name = store_name

    def put(self, key, data):
        self.logger.info(f'{self.store_name} put {key}')
        self.store.put(key, data)

    def get(self, key):
        self.logger.info(f'{self.store_name} get {key}')
        return self.store.get(key)
    
    def exists(self, key):
        self.logger.info(f'{self.store_name} exists {key}')
        return self.store.exists(key)
    
    def delete(self, key):
        self.logger.info(f'{self.store_name} delete {key}')
        return self.store.delete(key)
    
    def keys(self):
        self.logger.info(f'{self.store_name} keys')
        return self.store.keys()


class ExceptionLoggingStore(LoggingStore):

    def put(self, key, data):
        try:
            super().put(key, data)
        except Exception as e:
            self.logger.error(f'failed to put {key}: {e}')

    def get(self, key):
        try:
            return super().get(key)
        except Exception as e:
            self.logger.error(f'failed to get {key}: {e}')
            return None
    
    def exists(self, key):
        try:
            return super().exists(key)
        except Exception as e:
            self.logger.error(f'failed to exists {key}: {e}')
            return False
    
    def delete(self, key):
        try:
            return super().delete(key)
        except Exception as e:
            self.logger.error(f'failed to delete {key}: {e}')
            return False
    
    def keys(self):
        try:
            return super().keys()
        except Exception as e:
            self.logger.error(f'failed to get keys: {e}')
            return []
        

class TransformingStore(ObjectStore):
    """
    A store that applies a transformation to data before storing it
    and a reverse transformation after retrieving it.
    """

    def __init__(self, store):
        self.store = store

    def transform(self, data):
        return data # no-op

    def reverse_transform(self, data):
        return data # no-op
    
    def put(self, key, data):
        self.store.put(key, self.transform(data))

    def get(self, key):
        return self.reverse_transform(self.store.get(key))
    
    def exists(self, key):
        return self.store.exists(key)
    
    def delete(self, key):
        return self.store.delete(key)
    
    def keys(self):
        return self.store.keys()
    

class TextEncodingStore(TransformingStore):
    """
    Store that encodes and decodes data as text
    """

    def __init__(self, store, encoding='utf-8'):
        super().__init__(store)
        self.encoding = encoding

    def transform(self, data):
        return data.encode(self.encoding)
    
    def reverse_transform(self, data):
        return data.decode(self.encoding)
    

class GzipStore(TransformingStore):

    def transform(self, data):
        return gzip.compress(data)
    
    def reverse_transform(self, data):
        return gzip.decompress(data)


class BufferStore(TransformingStore):
    """
    Store that provides a buffer interface for a backing store
    """

    def transform(self, buffer):
        return buffer.read()
    
    def reverse_transform(self, data):
        b = BytesIO(data)
        b.seek(0)
        return b
    

class Base64Store(TextEncodingStore):
    """
    Store that encodes and decodes data as base64
    """

    def transform(self, data):
        return b64encode(data)
    
    def reverse_transform(self, data):
        return b64decode(data)
    

class JsonStore(TextEncodingStore):

    def transform(self, data):
        return json.dumps(data)
    
    def reverse_transform(self, data):
        return json.loads(data)
    

# key-based transformations

class KeyTransformingStore(ObjectStore):

    def __init__(self, store):
        self.store = store

    def transform_key(self, key):
        return key # no-op
    
    def reverse_transform_key(self, key):
        return key # no-op
    
    def put(self, key, data):
        self.store.put(self.transform_key(key), data)

    def get(self, key):
        return self.store.get(self.transform_key(key))
    
    def exists(self, key):
        return self.store.exists(self.transform_key(key))
    
    def delete(self, key):
        return self.store.delete(self.transform_key(key))
    
    def keys(self):
        return (self.reverse_transform_key(key) for key in self.store.keys())


class KeyValidatingStore(KeyTransformingStore):
    """
    Store that validates keys using a validator, which either returns
    None or raise KeyError.
    """

    def __init__(self, store, validator):
        super().__init__(store)
        self.validator = validator


    def transform_key(self, key):
        self.validator(key)
        return key


class RegexValidator:
    """for use with KeyValidatingStore"""

    def __init__(self, pattern):
        self.pattern = pattern

    def __call__(self, key):
        if not re.match(self.pattern, key):
            raise KeyError(f'key {key} does not match pattern {self.pattern}')


def validate_http_url_key(key):
    if urlparse(key).scheme not in ('http', 'https'):
        raise KeyError(f'key {key} is not a valid http/s url')


class UrlValidatingStore(KeyValidatingStore):
    """
    Store that validates keys as URLs.
    """

    def __init__(self, store):
        super().__init__(store, validate_http_url_key)


class RegexValidatingStore(KeyValidatingStore):
    """
    Store that validates keys using a regex pattern.
    """

    def __init__(self, store, pattern):
        super().__init__(store, RegexValidator(pattern))


class PrefixStore(KeyTransformingStore):
    """
    Store that prefixes keys with a given prefix.
    """

    def __init__(self, store, prefix):
        super().__init__(store)
        self.prefix = prefix

    def transform_key(self, key):
        return self.prefix + key

    def reverse_transform_key(self, key):
        return key[len(self.prefix):]


class HashPrefixStore(KeyTransformingStore):

    def __init__(self, store, hash_length=8, separator='/'):
        self.store = store
        self.hash_length = hash_length
        self.separator = separator

    def transform_key(self, key):
        hash_prefix = hashlib.sha256(key.encode()).hexdigest()[:self.hash_length]
        return hash_prefix + self.separator + key

    def reverse_transform_key(self, key):
        return key[self.hash_length + len(self.separator):]
    
    def keys(self):
        for key in self.store.keys():
            yield self.reverse_transform_key(key)


class UrlEncodingStore(KeyTransformingStore):
    """
    A store that handles URL encoding and decoding of keys while preserving
    hierarchical structure (slashes).
    """
    
    def transform_key(self, key):
        # Split by slashes and encode each part separately
        parts = key.split('/')
        encoded_parts = [quote(part, safe='') for part in parts]
        return '/'.join(encoded_parts)
    
    def reverse_transform_key(self, key):
        # Split by slashes and decode each part separately
        parts = key.split('/')
        decoded_parts = [unquote(part) for part in parts]
        return '/'.join(decoded_parts)
    

# utility functions for multi-store actions

            
def copy_store(from_store, to_store, overwrite=True):
    for key in from_store.keys():
        if overwrite or not to_store.exists(key):
            to_store.put(key, from_store.get(key))


def clear_store(store):
    for key in list(store.keys()):
        store.delete(key)


def sync_stores(from_store, to_store, delete=False):
    from_keys = set(from_store.keys())
    to_keys = set(to_store.keys())
    for key in from_keys - to_keys:
        to_store.put(key, from_store.get(key))
    if delete:
        for key in to_keys - from_keys:
            to_store.delete(key)
