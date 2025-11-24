import yaml
from .utils import IdentityStore, ReadonlyStore, WriteonlyStore, MirroringStore, CachingStore, NotifyingStore, LoggingStore, TransformingStore, TextEncodingStore, GzipStore, BufferStore, Base64Store, JsonStore, KeyTransformingStore, KeyValidatingStore, UrlValidatingStore, RegexValidatingStore, PrefixStore, HashPrefixStore, UrlEncodingStore
from .s3 import BucketStore, AsyncBucketStore
from .object import DictStore


class ConfigError(Exception):
    pass


class StoreFactory:
    """ Builds stores from a YAML config file. """
    
    STORES = {
          'BucketStore': BucketStore,
          'AsyncBucketStore': AsyncBucketStore,
          'DictStore': DictStore,
          'IdentityStore': IdentityStore,
          'ReadonlyStore': ReadonlyStore,
          'WriteonlyStore': WriteonlyStore,
          'MirroringStore': MirroringStore,
          'CachingStore': CachingStore,
          'NotifyingStore': NotifyingStore,
          'LoggingStore': LoggingStore,
          'TransformingStore': TransformingStore,
          'TextEncodingStore': TextEncodingStore,
          'GzipStore': GzipStore,
          'BufferStore': BufferStore,
          'Base64Store': Base64Store,
          'JsonStore': JsonStore,
          'KeyTransformingStore': KeyTransformingStore,
          'KeyValidatingStore': KeyValidatingStore,
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
        if store_type == 'CachingStore':
            config['main_store'] = self.build(base_config['main_store'])
            config['cache_store'] = self.build(base_config['cache_store'])
        else:
            self._raise_invalid_base_config(store_type, type(base_config))
        return config


    def _parse_list_base(self, store_type, config, base_config):
        """ Parse the given base config and add it to the general config. """
        if store_type == 'MirroringStore':
            built_child_stores = []
            for child_store in base_config:
                built_child_stores.append(self.build(child_store))
            config['children'] = built_child_stores
        else:
            self._raise_invalid_base_config(store_type, type(base_config))
        return config
       

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

        base_config = store_def.get('base', {})
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
