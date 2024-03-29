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

The `storage.utils` module provides utilities for working with multiple stores, including:

- `FanoutStore` which replicates operations across a set of child stores
- `CachingStore` in which a faster store can be used as a cache for a slower store
- the `sync_stores` function for making two stores' contents identical

`asyncio` versions of the utilities are provided in the `aioutils` module.

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
    # configuring S3 client left as an exercise to the reader
    async with ... as s3_client:
        async with AsyncBucketStore(s3_client, 'my-bucket') as store:
            await store.put('my_object_key', b'my object data')
            data = await store.get('my_object_key')

asyncio.run(main())
```
