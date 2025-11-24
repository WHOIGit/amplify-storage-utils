from pathlib import Path

import pytest

from storage.config_builder import StoreFactory, ConfigError
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
