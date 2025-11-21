import yaml
from .utils import IdentityStore, ReadonlyStore, WriteonlyStore, MirroringStore, CachingStore, NotifyingStore, LoggingStore, TransformingStore, TextEncodingStore, GzipStore, BufferStore, Base64Store, JsonStore, KeyTransformingStore, KeyValidatingStore, UrlValidatingStore, RegexValidatingStore, PrefixStore, HashPrefixStore, UrlEncodingStore
from .s3 import BucketStore, AsyncBucketStore
from .object import DictStore

class StoreFactory:
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
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.stores = self.config.get('stores') 
        self.main_store = self.config.get('main')

    def build(self, store_name=None):
        if store_name == None:
            store_name = self.main_store
        store_def = self.stores[store_name]

        store_type = store_def['type']
        store_class = self.STORES[store_type]

        if 'config' in store_def:
            config = store_def.get('config')
        else:
            config = {}
        if 'base' in store_def:
            config['store'] = self.build(store_def['base'])

        return store_class(**config)
