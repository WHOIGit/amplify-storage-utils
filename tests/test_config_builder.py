from pathlib import Path

import pytest

from storage.config_builder import StoreFactory, ConfigError 

def load_yaml(yaml_path):
    store_factory = StoreFactory(yaml_path)
    return store_factory.build()

def test_load_recursive_yaml():
    cwd = Path(__file__).parent
    cfg = cwd / "test-configs" / "recursive.yaml"
    with pytest.raises(ConfigError, match="Recursive store definition found -- store1 mentioned multiple times"):
        load_yaml(cfg)
