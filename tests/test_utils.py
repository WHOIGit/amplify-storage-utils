import pytest
from io import BytesIO
import json
import gzip
import base64
import logging
import re
from storage.object import DictStore
from storage.utils import (
    IdentityStore, NotifyingStore, ReadonlyStore, WriteonlyStore, MirroringStore,
    CachingStore, TransformingStore, TextEncodingStore, GzipStore,
    BufferStore, Base64Store, JsonStore, PrefixStore, UrlEncodingStore,
    LoggingStore, ExceptionLoggingScore, KeyValidatingStore,
    UrlValidatingStore, HashPrefixStore, RegexValidatingStore,
    copy_store, clear_store, sync_stores
)

@pytest.fixture
def store():
    return DictStore()

@pytest.fixture
def test_data():
    return b"test data"

@pytest.fixture
def test_key():
    return "test_key"

class TestIdentityStore:
    def test_basic_operations(self, store, test_key, test_data):
        identity_store = IdentityStore(store)
        
        # Test put and get
        identity_store.put(test_key, test_data)
        assert identity_store.get(test_key) == test_data
        
        # Test exists
        assert identity_store.exists(test_key)
        assert not identity_store.exists("nonexistent")
        
        # Test delete
        identity_store.delete(test_key)
        assert not identity_store.exists(test_key)
        
        # Test keys
        identity_store.put(test_key, test_data)
        assert list(identity_store.keys()) == [test_key]

class TestReadonlyStore:
    def test_readonly_operations(self, store, test_key, test_data):
        # Set up store with initial data
        store.put(test_key, test_data)
        readonly_store = ReadonlyStore(store)
        
        # Test read operations
        assert readonly_store.get(test_key) == test_data
        assert readonly_store.exists(test_key)
        assert list(readonly_store.keys()) == [test_key]
        
        # Test write operations raise NotImplementedError
        with pytest.raises(NotImplementedError):
            readonly_store.put("new_key", b"new data")
        
        with pytest.raises(NotImplementedError):
            readonly_store.delete(test_key)

class TestWriteonlyStore:
    def test_writeonly_operations(self, store, test_key, test_data):
        writeonly_store = WriteonlyStore(store)
        
        # Test write operations
        writeonly_store.put(test_key, test_data)
        assert store.get(test_key) == test_data
        
        # Test read operations raise NotImplementedError
        with pytest.raises(NotImplementedError):
            writeonly_store.get(test_key)
        
        with pytest.raises(NotImplementedError):
            writeonly_store.exists(test_key)
            
        with pytest.raises(NotImplementedError):
            list(writeonly_store.keys())

class TestMirroringStore:
    @pytest.fixture
    def mirror_stores(self):
        return [DictStore(), DictStore(), DictStore()]
    
    def test_mirroring_operations(self, mirror_stores, test_key, test_data):
        mirroring_store = MirroringStore(mirror_stores)
        
        # Test put replicates to all stores
        mirroring_store.put(test_key, test_data)
        for store in mirror_stores:
            assert store.get(test_key) == test_data
        
        # Test get retrieves from first available store
        assert mirroring_store.get(test_key) == test_data
        
        # Test exists checks all stores
        assert mirroring_store.exists(test_key)
        
        # Test delete removes from all stores
        mirroring_store.delete(test_key)
        for store in mirror_stores:
            assert not store.exists(test_key)
        
        # Test keys returns union of all stores' keys
        keys = ["key1", "key2", "key3"]
        for i, key in enumerate(keys):
            mirror_stores[i].put(key, test_data)
        assert set(mirroring_store.keys()) == set(keys)

class TestCachingStore:
    @pytest.fixture
    def cache_stores(self):
        return DictStore(), DictStore()  # main_store, cache_store
    
    def test_caching_behavior(self, cache_stores, test_key, test_data):
        main_store, cache_store = cache_stores
        caching_store = CachingStore(main_store, cache_store)
        
        # Test put writes to both stores
        caching_store.put(test_key, test_data)
        assert main_store.get(test_key) == test_data
        assert cache_store.get(test_key) == test_data
        
        # Test get uses cache when available
        modified_data = b"modified data"
        main_store.put(test_key, modified_data)
        assert caching_store.get(test_key) == test_data  # Should return cached data
        
        # Test get populates cache when needed
        new_key = "new_key"
        main_store.put(new_key, test_data)
        assert not cache_store.exists(new_key)
        retrieved_data = caching_store.get(new_key)
        assert retrieved_data == test_data
        assert cache_store.exists(new_key)
        
        # Test clear cache
        caching_store.clear()
        assert not cache_store.exists(test_key)
        assert main_store.exists(test_key)
        
        # Test selective clear
        caching_store.put(test_key, test_data)
        caching_store.put(new_key, test_data)
        caching_store.clear(test_key)
        assert not cache_store.exists(test_key)
        assert cache_store.exists(new_key)

class TestTransformingStore:
    def test_basic_transform(self, store, test_key, test_data):
        class UppercaseStore(TransformingStore):
            def transform(self, data):
                return data.upper()
            
            def reverse_transform(self, data):
                return data.lower()
        
        transform_store = UppercaseStore(store)
        test_str = b"hello world"
        
        transform_store.put(test_key, test_str)
        assert store.get(test_key) == b"HELLO WORLD"
        assert transform_store.get(test_key) == b"hello world"

class TestTextEncodingStore:
    def test_text_encoding(self, store, test_key):
        text_store = TextEncodingStore(store)
        test_str = "Hello, 世界"
        
        text_store.put(test_key, test_str)
        assert text_store.get(test_key) == test_str
        assert isinstance(store.get(test_key), bytes)

class TestGzipStore:
    def test_compression(self, store, test_key):
        gzip_store = GzipStore(store)
        test_data = b"test " * 1000  # Create larger data to compress
        
        gzip_store.put(test_key, test_data)
        compressed_size = len(store.get(test_key))
        original_size = len(test_data)
        
        assert compressed_size < original_size
        assert gzip_store.get(test_key) == test_data

class TestBufferStore:
    def test_buffer_interface(self, store, test_key, test_data):
        buffer_store = BufferStore(store)
        buffer = BytesIO(test_data)
        
        buffer_store.put(test_key, buffer)
        retrieved = buffer_store.get(test_key)
        
        assert isinstance(retrieved, BytesIO)
        assert retrieved.read() == test_data

class TestBase64Store:
    def test_base64_encoding(self, store, test_key, test_data):
        base64_store = Base64Store(store)
        
        base64_store.put(test_key, test_data)
        stored_data = store.get(test_key)
        
        # Verify the stored data is valid base64
        try:
            base64.b64decode(stored_data)
        except Exception:
            pytest.fail("Stored data is not valid base64")
            
        assert base64_store.get(test_key) == test_data

class TestJsonStore:
    def test_json_serialization(self, store, test_key):
        json_store = JsonStore(store)
        test_obj = {"name": "test", "values": [1, 2, 3]}
        
        json_store.put(test_key, test_obj)
        assert json_store.get(test_key) == test_obj

class TestPrefixStore:
    def test_key_prefixing(self, store, test_key, test_data):
        prefix = "prefix/"
        prefix_store = PrefixStore(store, prefix)
        
        prefix_store.put(test_key, test_data)
        assert store.exists(prefix + test_key)
        assert prefix_store.get(test_key) == test_data
        
        # Test keys() removes prefix
        assert list(prefix_store.keys()) == [test_key]

class TestUrlEncodingStore:
    def test_url_encoding(self, store, test_data):
        url_store = UrlEncodingStore(store)
        test_key = "path/with spaces/and#special&chars"
        
        url_store.put(test_key, test_data)
        encoded_key = "path/with%20spaces/and%23special%26chars"
        
        assert store.exists(encoded_key)
        assert url_store.get(test_key) == test_data
        assert list(url_store.keys()) == [test_key]

class TestLoggingStore:
    def test_logging_operations(self, store, test_key, test_data, caplog):
        caplog.set_level(logging.INFO)
        logging_store = LoggingStore(store, "test_store")
        
        # Test all operations are logged
        logging_store.put(test_key, test_data)
        logging_store.get(test_key)
        logging_store.exists(test_key)
        logging_store.delete(test_key)
        list(logging_store.keys())
        
        # Verify logs
        assert len(caplog.records) == 5
        assert "test_store put" in caplog.text
        assert "test_store get" in caplog.text
        assert "test_store exists" in caplog.text
        assert "test_store delete" in caplog.text
        assert "test_store keys" in caplog.text

class TestExceptionLoggingStore:
    def test_exception_handling(self, store, test_key, test_data, caplog):
        caplog.set_level(logging.ERROR)
        
        # Create a store that raises exceptions
        class ErrorStore(DictStore):
            def get(self, key):
                raise Exception("get error")
            def put(self, key, data):
                raise Exception("put error")
            def exists(self, key):
                raise Exception("exists error")
            def delete(self, key):
                raise Exception("delete error")
            def keys(self):
                raise Exception("keys error")
        
        error_store = ErrorStore()
        logging_store = ExceptionLoggingScore(error_store)
        
        # Test all operations handle exceptions
        logging_store.put(test_key, test_data)
        assert logging_store.get(test_key) is None
        assert not logging_store.exists(test_key)
        assert not logging_store.delete(test_key)
        assert list(logging_store.keys()) == []
        
        # Verify error logs
        assert len(caplog.records) == 5
        assert "failed to put" in caplog.text
        assert "failed to get" in caplog.text
        assert "failed to exists" in caplog.text
        assert "failed to delete" in caplog.text
        assert "failed to get keys" in caplog.text

class TestKeyValidatingStore:
    def test_key_validation(self, store, test_data):
        def validator(key):
            if not key.startswith("valid_"):
                raise KeyError(f"Invalid key: {key}")
        
        validating_store = KeyValidatingStore(store, validator)
        
        # Test valid key
        validating_store.put("valid_key", test_data)
        assert validating_store.get("valid_key") == test_data
        
        # Test invalid key
        with pytest.raises(KeyError):
            validating_store.put("invalid_key", test_data)

class TestRegexValidatingStore:
    def test_regex_validation(self, store, test_data):
        pattern = r"^[a-z]+_\d+$"
        regex_store = RegexValidatingStore(store, pattern)
        
        # Test valid key
        regex_store.put("test_123", test_data)
        assert regex_store.get("test_123") == test_data
        
        # Test invalid keys
        with pytest.raises(KeyError):
            regex_store.put("INVALID_123", test_data)
        with pytest.raises(KeyError):
            regex_store.put("test-123", test_data)

class TestUrlValidatingStore:
    def test_url_validation(self, store, test_data):
        url_store = UrlValidatingStore(store)
        
        # Test valid URLs
        url_store.put("http://example.com", test_data)
        url_store.put("https://example.com/path", test_data)
        
        # Test invalid URLs
        with pytest.raises(KeyError):
            url_store.put("not_a_url", test_data)
        with pytest.raises(KeyError):
            url_store.put("ftp://example.com", test_data)

class TestHashPrefixStore:
    def test_hash_prefix(self, store, test_key, test_data):
        hash_store = HashPrefixStore(store, hash_length=4, separator='/')
        
        # Test put and get
        hash_store.put(test_key, test_data)
        assert hash_store.get(test_key) == test_data
        
        # Verify hash prefix structure
        stored_keys = list(store.keys())
        assert len(stored_keys) == 1
        stored_key = stored_keys[0]
        assert len(stored_key.split('/')[0]) == 4  # hash length
        assert stored_key.endswith(test_key)
        
        # Test keys() removes hash prefix
        assert list(hash_store.keys()) == [test_key]


class TestNotifyingStore:

    def test_notify(self, store, test_key, test_data):
        def put_handler(s, operation, key, exc):
            assert s is store
            assert operation == NotifyingStore.PUT
            assert key == test_key
            assert exc is None

        notifying_store = NotifyingStore(store)
        notifying_store.on_change(put_handler)
        notifying_store.put(test_key, test_data)

        def delete_handler(s, operation, key, exc):
            assert s is store
            assert operation == NotifyingStore.DELETE
            assert key == test_key
            assert exc is None
    
        notifying_store = NotifyingStore(store)
        notifying_store.on_change(delete_handler)
        notifying_store.delete(test_key)

        def failed_put_handler(s, operation, key, exc):
            assert s is store
            assert operation == NotifyingStore.PUT
            assert key == test_key
            assert exc is not None

        def failed_delete_handler(s, operation, key, exc):
            assert s is store
            assert operation == NotifyingStore.DELETE
            assert key == test_key
            assert exc is not None

        class ErrorStore(DictStore):
            def put(self, key, data):
                raise Exception("put error")
            def delete(self, key):
                raise Exception("delete error")
            
        error_store = ErrorStore()
        notifying_store = NotifyingStore(error_store)
        notifying_store.on_change(failed_put_handler)
        notifying_store.on_change(failed_delete_handler)

        with pytest.raises(Exception):
            notifying_store.put(test_key, test_data)

        with pytest.raises(Exception):
            notifying_store.delete(test_key)


def test_copy_store(store, test_data):
    source = DictStore()
    keys = ["key1", "key2", "key3"]
    
    # Setup source store
    for key in keys:
        source.put(key, test_data)
    
    # Test copy without overwrite
    copy_store(source, store)
    for key in keys:
        assert store.get(key) == test_data
    
    # Test copy with overwrite=False
    modified_data = b"modified"
    store.put(keys[0], modified_data)
    copy_store(source, store, overwrite=False)
    assert store.get(keys[0]) == modified_data  # Should not overwrite

def test_clear_store(store, test_data):
    # Setup store with data
    keys = ["key1", "key2", "key3"]
    for key in keys:
        store.put(key, test_data)
    
    clear_store(store)
    assert list(store.keys()) == []

def test_sync_stores():
    source = DictStore()
    target = DictStore()
    test_data = b"test"
    
    # Setup initial data
    source.put("key1", test_data)
    source.put("key2", test_data)
    target.put("key3", test_data)
    
    # Test sync without delete
    sync_stores(source, target, delete=False)
    assert target.get("key1") == test_data
    assert target.get("key2") == test_data
    assert target.get("key3") == test_data
    
    # Test sync with delete
    sync_stores(source, target, delete=True)
    assert not target.exists("key3")
    assert set(target.keys()) == set(source.keys())

