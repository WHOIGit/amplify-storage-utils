# Object Store

This project provides an abstraction layer for object storage systems. It defines a common interface for interacting with object stores and provides implementations for several storage backends.

## Design

### Core

The core of the project is the `ObjectStore` abstract base class defined in `object.py`. This class defines the interface that all object store implementations must adhere to. The interface includes methods for storing, retrieving, deleting, and checking the existence of objects, as well as optionally listing the keys of all objects in the store.

### Implementations

Concrete implementations of the `ObjectStore` interface are provided for various storage backends:

- In-memory dictionary (`DictStore`)
- Filesystem (`FilesystemStore`)
- Filesystem with hashed directory structure (`HashdirStore`)
- SQLite database (`SqliteStore`)
- S3-compatible object storage (`BucketStore`)
- Zip files (`ZipStore`)

`asyncio` based implementations are available for some of these backends.

### Utilities

The `storage.utils` module provides utilities for working with more complex store setups, including:

- Multi-store setups 
  - `MirroringStore` which replicates operations across a set of child stores
  - `CachingStore` in which a faster store can be used as a cache for a slower store
- `ReadonlyStore` to create a store without put or delete functionality
- `WriteonlyStore` to create a store without get, exists, or key listing functionality
- `NotifyingStore` to set handlers that run on changes and/or put calls
- `LoggingStore` to create a store with a logger
- `ExceptionLoggingStore` to create a store that logs any exceptions encountered
- Data transformation stores
  - `TextEncodingStore` that encodes and decodes data as text via a specified encoding
  - `GzipStore` that stores data with gzip compression
  - `BufferStore` that provides a buffer interface for a backing store
  - `Base64Store` that encodes and decodes data as base64
- Key transforming stores
  - `KeyValidatingStore` that validates keys using a given validator
  - `RegexValidatingStore`, that uses Regex to validate keys
  - `UrlValidatingStore` that validates keys as URLs
  - `PrefixStore` that prefixes keys with a given prefix
  - `HashPrefixStore` that hashes each key and adds the hash as a prefix
  - `UrlEncodingStore` that handles URL encoding and decoding of keys while preserving hierarchical structure
- Utility functions including: 
  - `sync_stores` for making two stores' contents identical
  - `copy_store` for copying a store into another
  - `clear_store` for removing all store data

`asyncio` versions of some of the utilities are provided in the `aioutils` module.

## Usage

To use an object store, first create an instance of the desired implementation class. For example, to use the `FilesystemStore`:

```python
from storage.fs import FilesystemStore

store = FilesystemStore('/path/to/storage/directory')
```

Then, you can use the instance to interact with the object store:

```python
# Retrieve an object
data = store.get('my_object_key')

# Check if an object exists
exists = store.exists('my_object_key')

# Store an object (if supported by the implementation)
store.put('my_object_key', b'my object data')

# Delete an object (if supported by the implementation)
store.delete('my_object_key')

# List all object keys (if supported by the implementation)
keys = list(store.keys())

# For S3 stores, store.keys() returns a Generator to support S3 pagination. 
# To list all object keys
keys = []
for key in store.keys():
  keys.append(key)
```

Some implementations, such as `SqliteStore` and `ZipStore`, are stateful and should be used as context managers:

```python
from storage.db import SqliteStore

with SqliteStore('/path/to/db.sqlite') as store:
    store.put('my_object_key', b'my object data')
    data = store.get('my_object_key')
```

The `AsyncBucketStore` class provides an asynchronous interface for S3-compatible object storage. It can be used with the asyncio library:

```python
from storage.s3 import AsyncBucketStore

async def main():
    async with AsyncBucketStore(bucket_name="bucket-name", endpoint_url="http://endpoint", s3_access_key="X", s3_secret_key="Y") as store:
        await store.put('my_object_key', b'my object data')
        data = await store.get('my_object_key')

asyncio.run(main())
```

## YAML configuration

As an alternative to using Python to define your store configurations, you can also use YAML to create a configuration schema. 

Config files must define each store under "stores".

Each store must have a type, specifiying the store class. If the store is a decorator, it must have a "base" parameter specifiying the name of the store that it will decorate. 

```
stores:
  store1:
    type: DictStore

  readonly_store:
    type: ReadonlyStore
    base: store1

main: readonly_store

```

Config files must have a "main" parameter specifiying the name of the store that will be built by default for the file.

If the store has initialization parameters, they may be specified by the "config" parameter.

```
  async_store:
    type: AsyncBucketStore
    config:
      endpoint_url: http://test
      s3_access_key: X
      s3_secret_key: Y
      bucket_name: test-bucket
```

CachingStores and their async equivalent have two specified bases:

```
  caching_store:
    type: CachingStore
    base:
      main_store: hash_prefix_store
      cache_store: backup_store
```

MirroringStores and their async equivalent have a list of base stores:

```
  mirror_store:
    type: MirroringStore
    base:
      - caching_store
      - text_encoding_store
```
