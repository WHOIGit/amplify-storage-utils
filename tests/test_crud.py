import tempfile
import pytest
from storage.fs import FilesystemStore, HashdirStore
from storage.object import DictStore
from storage.db import SqliteStore

@pytest.fixture(params=[
    pytest.param(DictStore, id="dict-store"),
    pytest.param((SqliteStore, [':memory:']), id="sqlite-store"),
    pytest.param(FilesystemStore, id="filesystem-store"),
    pytest.param(HashdirStore, id="hashdir-store"),
])
def store_config(request):
    """Fixture that provides store factory and its parameters"""
    return request.param

@pytest.fixture
def store(store_config):
    """
    Fixture that handles store creation and cleanup.
    Uses the factory pattern to support complex initialization.
    """
    if isinstance(store_config, tuple):
        if len(store_config) == 2:
            factory, args = store_config
            kw = {}
        elif len(store_config) == 3:
            factory, args, kw = store_config
    else:
        factory = store_config
        args = ()
        kw = {}
    
    if factory in [FilesystemStore, HashdirStore]:
        # Special case for FilesystemStore, use temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            store_instance = factory(tmpdir)
            yield store_instance
    elif hasattr(factory, '__enter__'):
        # Context manager case (for cleanup)
        with factory(*args, **kw) as store_instance:
            yield store_instance
    else:
        # Factory function case
        store_instance = factory(*args, **kw)
        yield store_instance

def test_put_and_get(store):
    """Test basic put and get operations"""
    # Test putting and getting a simple object
    key = "test_key"
    data = b"Hello, World!"
    store.put(key, data)
    retrieved_data = store.get(key)
    assert retrieved_data == data

    # Test putting and getting an empty object
    empty_key = "empty"
    empty_data = b""
    store.put(empty_key, empty_data)
    assert store.get(empty_key) == empty_data

    # Test putting and getting binary data
    binary_key = "binary"
    binary_data = bytes(range(256))  # All possible byte values
    store.put(binary_key, binary_data)
    assert store.get(binary_key) == binary_data

def test_exists(store):
    """Test exists method"""
    key = "test_exists"
    data = b"test data"
    
    # Key should not exist initially
    assert not store.exists(key)
    
    # Key should exist after putting data
    store.put(key, data)
    assert store.exists(key)
    
    # Key should not exist after deletion
    store.delete(key)
    assert not store.exists(key)

def test_delete(store):
    """Test delete operation"""
    key = "test_delete"
    data = b"delete me"
    
    # Put data and verify it exists
    store.put(key, data)
    assert store.exists(key)
    
    # Delete data and verify it's gone
    store.delete(key)
    assert not store.exists(key)
    
    # Attempting to get deleted key should raise KeyError
    with pytest.raises(KeyError):
        store.get(key)

def test_update(store):
    """Test updating existing keys"""
    key = "test_update"
    original_data = b"original"
    updated_data = b"updated"
    
    # Put original data
    store.put(key, original_data)
    assert store.get(key) == original_data
    
    # Update with new data
    store.put(key, updated_data)
    assert store.get(key) == updated_data

def test_error_cases(store):
    """Test error cases and edge conditions"""
    # Getting non-existent key should raise KeyError
    with pytest.raises(KeyError):
        store.get("nonexistent")


def test_keys_listing(store):
    """Test keys() method if implemented"""
    try:
        # Store should be empty initially
        assert len(list(store.keys())) == 0
        
        # Add some keys
        test_keys = ["key1", "key2", "key3"]
        for key in test_keys:
            store.put(key, f"data for {key}".encode())
        
        # Get list of keys and verify
        stored_keys = list(store.keys())
        assert len(stored_keys) == len(test_keys)
        for key in test_keys:
            assert key in stored_keys
            
    except NotImplementedError:
        pytest.skip("keys() method not implemented for this store")

def test_large_data(store):
    """Test handling of larger data chunks"""
    key = "large_data"
    # Create 1MB of random-like data
    large_data = b"x" * (1024 * 1024)
    
    # Store and retrieve large data
    store.put(key, large_data)
    retrieved_data = store.get(key)
    assert retrieved_data == large_data
    
    # Verify data integrity
    assert len(retrieved_data) == len(large_data)