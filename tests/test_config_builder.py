import re
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from storage.config_builder import StoreFactory, ConfigError, register_store
from storage.object import ObjectStore
from storage.utils import ReadonlyStore, MirroringStore, CachingStore

from mocks import MockS3


@pytest.fixture
def async_s3_store():
    store = load_yaml(cfg("async.yaml"))
    store.s3_client = MockS3()
    return store


def load_yaml(yaml_path):
    store_factory = StoreFactory(yaml_path)
    return store_factory.build()


def cfg(config_name):
    return Path(__file__).parent / "test-configs" / config_name


def test_load_recursive_yaml():
    """ Ensure that recursive definitions are caught. """
    with pytest.raises(ConfigError, match="Recursive store definition found -- store1 mentioned multiple times"):
        load_yaml(cfg("recursive.yaml"))


def test_load_multi_base():
    """ Test support for multi-base stores (MirroringStore and CachingStore). """
    store = load_yaml(cfg("multi_base.yaml"))
   
    key = "test_key"
    data = "Hello, World!"
    store.put(key, data)
    retrieved_data = store.get(key)
    assert retrieved_data == data


@pytest.mark.asyncio
async def test_put_get_exists_delete(async_s3_store):
    """ Test support for AsyncBucketStores configured via YAML. Tests get/put/exists/delete functionality. """
    assert await async_s3_store.put("a.txt", b"hello") is True
    assert await async_s3_store.exists("a.txt") is True

    data = await async_s3_store.get("a.txt")
    assert data == b"hello"

    assert await async_s3_store.delete("a.txt") is True
    assert await async_s3_store.exists("a.txt") is False


@pytest.mark.asyncio
async def test_presigned_urls(async_s3_store):
    """ Test AsyncBucketStore presigned URLs. """
    url1 = await async_s3_store.presigned_put("k.txt", expiry=123)
    url2 = await async_s3_store.presigned_get("k.txt", expiry=456)
    assert "op=put_object" in url1
    assert "op=get_object" in url2


@pytest.mark.asyncio
async def test_keys_with_prefix(async_s3_store):
    """ Test AsyncBucketStore keys and paginator. """
    await async_s3_store.s3_client.put_object(Bucket="test-bucket", Key="x/1.txt", Body=b"1")
    await async_s3_store.s3_client.put_object(Bucket="test-bucket", Key="x/2.txt", Body=b"2")
    await async_s3_store.s3_client.put_object(Bucket="test-bucket", Key="y/3.txt", Body=b"3")

    keys = [k async for k in async_s3_store.keys(prefix="x/")]
    assert set(keys) == {"x/1.txt", "x/2.txt"}


@patch.dict('os.environ', {'VAR1': 'apple', 'VAR2': 'orange'}, clear=True)
def test_parse_env_vars():
    """ Test parsing environment variables in YAML. """
    store = load_yaml(cfg("vars.yaml"))

    assert store.get('item1') == 'apple' # variable present
    assert store.get('item2') == 'orange' # variable present, default ignored 
    assert store.get('item3') == 'blueberry' # variable absent, default used


@patch.dict("os.environ", {"EMPTY_VAR": ""}, clear=True)
def test_empty_env_var_value():
    """ Empty env values should not fall back to defaults. """
    store = load_yaml(cfg("empty_vars.yaml"))

    assert store.get("empty_value") == ""
    assert store.get("missing_value") == "missing-default"


def test_default_value_with_colons():
    """ Defaults can include colons and punctuation. """
    store = load_yaml(cfg("colon_default.yaml"))

    assert store.get("url") == "http://localhost:8080/api:v1"


@patch.dict("os.environ", {"DEEP_VAR": "deep", "LIST_VAR": "from-env"}, clear=True)
def test_env_vars_in_nested_structures():
    """ Ensure nested lists/dicts resolve environment variables. """
    store = load_yaml(cfg("nested_vars.yaml"))
    nested = store.get("nested")

    assert nested["list"] == ["static", "from-env", {"deep": "deep"}]


def test_missing_env_var():
    """ Ensure that missing env variables are caught. """
    with pytest.raises(ConfigError, match=re.escape("Environment variable 'MISSING' not found. Please set MISSING or provide a default value using ${MISSING:-default}")):
        load_yaml(cfg("missing_var.yaml"))


def test_custom_store_registration():
    """Test that custom stores can be registered and used in YAML configs."""
    # Define a simple custom store
    @register_store
    class AmplifyStore(ObjectStore):
        """A simple store that always returns 'amplify'."""
        def get(self, key):
            return b"amplify"

        def put(self, key, data):
            pass

        def exists(self, key):
            return True

        def delete(self, key):
            pass

    # Create a temporary YAML config that uses the custom store
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
stores:
  amplify_store:
    type: AmplifyStore

main: amplify_store
""")
        yaml_path = f.name

    try:
        # Load the store from YAML
        store = load_yaml(yaml_path)

        # Test functionality
        assert store.get("any_key") == b"amplify"
        assert store.exists("any_key") is True
    finally:
        # Clean up temp file
        Path(yaml_path).unlink()


def test_custom_store_with_params_inheriting_wrapper():
    """Test custom store that inherits from a wrapper store and uses custom params."""
    # Define a custom store that inherits from ReadonlyStore and adds a prefix
    @register_store
    class PrefixedReadonlyStore(ReadonlyStore):
        """A readonly store that prepends a prefix to all retrieved data."""
        def __init__(self, prefix, suffix, store):
            super().__init__(store)
            self.prefix = prefix
            self.suffix = suffix

        def get(self, key):
            # Get data from wrapped store and add prefix/suffix
            data = self.store.get(key)
            return self.prefix.encode() + data + self.suffix.encode()

    # Create a temporary YAML config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
stores:
  backend:
    type: DictStore

  prefixed:
    type: PrefixedReadonlyStore
    config:
      prefix: "START:"
      suffix: ":END"
    base: backend

main: prefixed
""")
        yaml_path = f.name

    try:
        # Load the store from YAML
        store = load_yaml(yaml_path)

        # Put some data (goes to backend since prefixed is readonly-wrapped)
        store.store.put("test_key", b"data")

        # Get should return data with prefix and suffix
        result = store.get("test_key")
        assert result == b"START:data:END"

        # Verify it's readonly (put should raise)
        with pytest.raises(Exception):
            store.put("new_key", b"value")
    finally:
        # Clean up temp file
        Path(yaml_path).unlink()


def test_custom_store_subclass_with_list_base():
    """Test that custom stores inheriting from multi-store types work with list bases."""
    # Define a custom store that inherits from MirroringStore
    @register_store
    class LoggingMirrorStore(MirroringStore):
        """A MirroringStore that logs operations."""
        def __init__(self, log_prefix, children):
            super().__init__(children)
            self.log_prefix = log_prefix
            self.operations = []

        def put(self, key, data):
            self.operations.append(f"{self.log_prefix}: put {key}")
            return super().put(key, data)

        def get(self, key):
            self.operations.append(f"{self.log_prefix}: get {key}")
            return super().get(key)

    # Create a temporary YAML config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
stores:
  store1:
    type: DictStore

  store2:
    type: DictStore

  logging_mirror:
    type: LoggingMirrorStore
    config:
      log_prefix: "MIRROR"
    base:
      - store1
      - store2

main: logging_mirror
""")
        yaml_path = f.name

    try:
        # Load the store from YAML
        store = load_yaml(yaml_path)

        # Test that it works like a MirroringStore
        store.put("key1", b"value1")
        result = store.get("key1")
        assert result == b"value1"

        # Verify logging functionality works
        assert "MIRROR: put key1" in store.operations
        assert "MIRROR: get key1" in store.operations

        # Verify both child stores have the data
        assert store.children[0].get("key1") == b"value1"
        assert store.children[1].get("key1") == b"value1"
    finally:
        # Clean up temp file
        Path(yaml_path).unlink()


def test_custom_store_subclass_with_dict_base():
    """Test that custom stores inheriting from CachingStore work with dict bases."""
    # Define a custom store that inherits from CachingStore
    @register_store
    class MetricsCachingStore(CachingStore):
        """A CachingStore that tracks cache hit/miss metrics."""
        def __init__(self, metric_prefix, main_store, cache_store):
            super().__init__(main_store, cache_store)
            self.metric_prefix = metric_prefix
            self.hits = 0
            self.misses = 0

        def get(self, key):
            if self.cache_store.exists(key):
                self.hits += 1
            else:
                self.misses += 1
            return super().get(key)

    # Create a temporary YAML config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
stores:
  main:
    type: DictStore

  cache:
    type: DictStore

  metrics_cache:
    type: MetricsCachingStore
    config:
      metric_prefix: "CACHE"
    base:
      main_store: main
      cache_store: cache

main: metrics_cache
""")
        yaml_path = f.name

    try:
        # Load the store from YAML
        store = load_yaml(yaml_path)

        # First get should miss (not in cache)
        store.main_store.put("key1", b"value1")
        result = store.get("key1")
        assert result == b"value1"
        assert store.hits == 0
        assert store.misses == 1

        # Second get should hit (now in cache)
        result = store.get("key1")
        assert result == b"value1"
        assert store.hits == 1
        assert store.misses == 1

        # Verify custom param was set
        assert store.metric_prefix == "CACHE"
    finally:
        # Clean up temp file
        Path(yaml_path).unlink()
