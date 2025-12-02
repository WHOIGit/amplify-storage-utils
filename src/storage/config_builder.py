import os
import re
import yaml

from .aiodb import AsyncSqliteStore
from .aiofs import AsyncFilesystemStore, AsyncHashdirStore
from .aioutils import AsyncFanoutStore, AsyncCachingStore
from .db import SqliteStore
from .fs import FilesystemStore, HashdirStore
from .mediastore import MediaStore
from .object import DictStore
from .s3 import BucketStore, AsyncBucketStore
from .utils import IdentityStore, ReadonlyStore, WriteonlyStore, MirroringStore, CachingStore, NotifyingStore, LoggingStore, ExceptionLoggingStore, TransformingStore, TextEncodingStore, GzipStore, BufferStore, Base64Store, JsonStore, KeyTransformingStore, UrlValidatingStore, RegexValidatingStore, PrefixStore, HashPrefixStore, UrlEncodingStore

class ConfigError(Exception):
    pass


class StoreFactory:
    """ Builds stores from a YAML config file. """
    
    STORES = {
          'AsyncSqliteStore': AsyncSqliteStore,
          'AsyncFilesystemStore': AsyncFilesystemStore,
          'AsyncHashdirStore': AsyncHashdirStore,
          'AsyncFanoutStore': AsyncFanoutStore,
          'AsyncCachingStore': AsyncCachingStore,
          'SqliteStore': SqliteStore,
          'FilesystemStore': FilesystemStore,
          'HashdirStore': HashdirStore,
          'MediaStore': MediaStore,
          'DictStore': DictStore,
          'BucketStore': BucketStore,
          'AsyncBucketStore': AsyncBucketStore,
          'IdentityStore': IdentityStore,
          'ReadonlyStore': ReadonlyStore,
          'WriteonlyStore': WriteonlyStore,
          'MirroringStore': MirroringStore,
          'CachingStore': CachingStore,
          'NotifyingStore': NotifyingStore,
          'LoggingStore': LoggingStore, # Non-default logger option is unsupported in YAML configuration. Use logging.basicConfig(...) after initialization
          'ExceptionLoggingStore': ExceptionLoggingStore, # Non-default logger option is unsupported in YAML configuration. Use logging.basicConfig(...) after initialization
          'TransformingStore': TransformingStore,
          'TextEncodingStore': TextEncodingStore,
          'GzipStore': GzipStore,
          'BufferStore': BufferStore,
          'Base64Store': Base64Store,
          'JsonStore': JsonStore,
          'KeyTransformingStore': KeyTransformingStore,
          'PrefixStore': PrefixStore,
          'HashPrefixStore': HashPrefixStore,
          'UrlEncodingStore': UrlEncodingStore,
          'UrlValidatingStore': UrlValidatingStore,
          'RegexValidatingStore': RegexValidatingStore,
      }

    def __init__(self, config_path):
        """ Load the given yaml config file. """
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.stores = self.config.get('stores') 
        self.main_store = self.config.get('main')
        self.built_stores = {}
    
    def _raise_invalid_base_config(self, store_type, base_config_type):
        """ Raise a ConfigError for an improperly defined base parameter. """
        raise ConfigError(f"Invalid base store configuration for {store_type}: base store definition is a {base_config_type}")


    def _parse_dictionary_base(self, store_type, config, base_config):
        """ Parse the given base config and add it to the general config. """
        if store_type == 'CachingStore' or store_type == 'AsyncCachingStore':
            config['main_store'] = self.build(base_config['main_store'])
            config['cache_store'] = self.build(base_config['cache_store'])
        else:
            self._raise_invalid_base_config(store_type, type(base_config))
        return config


    def _parse_list_base(self, store_type, config, base_config):
        """ Parse the given base config and add it to the general config. """
        if store_type == 'MirroringStore' or store_type == 'AsyncFanoutStore':
            built_child_stores = []
            for child_store in base_config:
                built_child_stores.append(self.build(child_store))
            config['children'] = built_child_stores
        else:
            self._raise_invalid_base_config(store_type, type(base_config))
        return config

    def _resolve_values(self, values):
        """ Resolve any references to environment variables. """
        if isinstance(values, dict):
            return {k: self._resolve_values(v) for k, v in values.items()}
        elif isinstance(values, list):
            return [self._resolve_values(item) for item in values]
        elif isinstance(values, str):
            match = re.match(r'^\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}$', values)
            if match:
                var_name = match.group(1)
                default = match.group(3) # None if no default specified
                env_value = os.environ.get(var_name)

                if env_value is None:
                    if default is not None:
                        return default
                    else:
                        raise ConfigError(f"Environment variable '{var_name}' not found. Please set {var_name} or provide a default value using ${{{var_name}:-default}}")
                return env_value

        return values # return anything else (int, bool, etc.)
    def build(self, store_name=None):
        """ 
        Build the store with the given name based on the YAML config. 
        If no store name is given, build the main store.
        """
        if store_name == None:
            store_name = self.main_store

        if store_name in self.built_stores:
            raise ConfigError(f"Recursive store definition found -- {store_name} mentioned multiple times")
        self.built_stores[store_name] = True
        
        store_def = self.stores[store_name]
        store_type = store_def['type']
        store_class = self.STORES[store_type]

        config = store_def.get('config', {})
        config = self._resolve_values(config)

        base_config = store_def.get('base', {})
        base_config = self._resolve_values(base_config)
        if base_config == {}:
            pass
        elif isinstance(base_config, dict):
            config = self._parse_dictionary_base(store_type, config, base_config)
        elif isinstance(base_config, list):
            config = self._parse_list_base(store_type, config, base_config)
        elif isinstance(base_config, str):
            config['store'] = self.build(store_def['base'])
        else:
            self._raise_invalid_base_config(store_type, type(base_config))

        return store_class(**config)
