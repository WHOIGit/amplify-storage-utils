# Object Store

This project provides an abstraction layer for object storage systems. It defines a common interface for interacting with object stores and provides implementations for several storage backends.

## Design

The core of the project is the `ObjectStore` abstract base class defined in `object.py`. This class defines the interface that all object store implementations must adhere to. The interface includes methods for storing, retrieving, deleting, and checking the existence of objects, as well as optionally listing the keys of all objects in the store.

Concrete implementations of the `ObjectStore` interface are provided for the following storage backends:

- In-memory dictionary (`DictStore`)
- Filesystem (`FilesystemStore`)
- Filesystem with hashed directory structure (`HashdirStore`)
- SQLite database (`SqliteStore`)
- S3-compatible object storage (`BucketStore` and `AsyncBucketStore`)

## Usage

To use an object store, first create an instance of the desired implementation class. For example, to use the `FilesystemStore`:

```python
from storage.fs import FilesystemStore

store = FilesystemStore('/path/to/storage/directory')
```

Then, you can use the instance to interact with the object store:

```python
# Store an object
store.put('my_object_key', b'my object data')

# Retrieve an object
data = store.get('my_object_key')

# Check if an object exists
exists = store.exists('my_object_key')

# Delete an object
store.delete('my_object_key')

# List all object keys (if supported by the implementation)
keys = store.keys()
```

Some implementations, such as `SqliteStore` and `BucketStore`, are stateful and should be used as context managers:

```python
from storage.db import SqliteStore

with SqliteStore('/path/to/db.sqlite') as store:
    store.put('my_object_key', b'my object data')
    data = store.get('my_object_key')
```

The `AsyncBucketStore` class provides an asynchronous interface for S3-compatible object storage. It can be used with the asyncio library:

```python
import asyncio
from storage.s3 import AsyncBucketStore

async def main():
    async with AsyncBucketStore(s3_client, 'my-bucket') as store:
        await store.put('my_object_key', b'my object data')
        data = await store.get('my_object_key')

asyncio.run(main())
```

## Implementations

### DictStore

Stores objects in an in-memory dictionary. Useful for testing and temporary storage.

### FilesystemStore

Stores objects as files on the filesystem. Object keys are used as file paths relative to a specified root directory.

### HashdirStore

Similar to `FilesystemStore`, but organizes objects in a hashed directory structure to avoid having too many files in a single directory. Useful for storing a large number of objects.

### SqliteStore

Stores objects in a SQLite database. Object keys and data are stored in a single table.

### BucketStore and AsyncBucketStore

Store objects in an S3-compatible object storage system. Requires providing S3 credentials and bucket name. `AsyncBucketStore` provides an asynchronous interface.