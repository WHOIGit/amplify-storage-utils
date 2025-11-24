from pathlib import Path

import pytest

from storage.config_builder import StoreFactory, ConfigError 


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
