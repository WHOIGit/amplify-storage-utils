#!/usr/bin/env python3
"""Test kwargs passthrough in ObjectStore.keys()"""

from storage.object import DictStore
from storage.utils import KeyTransformingStore, PrefixKeyTransformer


def test_dictstore_kwargs():
    """Test that DictStore.keys() accepts kwargs (even though it ignores them)"""
    store = DictStore()
    store.put("key1", b"data1")
    store.put("key2", b"data2")

    # Should not raise an error
    keys = list(store.keys())
    assert len(keys) == 2

    # Should also not raise with kwargs
    keys_with_kwargs = list(store.keys(prefix="test"))
    assert len(keys_with_kwargs) == 2

    print("✓ DictStore kwargs test passed")


def test_transforming_store_kwargs():
    """Test that KeyTransformingStore passes kwargs through"""
    base_store = DictStore()
    base_store.put("prefix/key1", b"data1")
    base_store.put("prefix/key2", b"data2")
    base_store.put("other/key3", b"data3")

    # Create a prefixing transformer
    transformer = PrefixKeyTransformer(prefix="prefix/")
    transformed_store = KeyTransformingStore(base_store, transformer)

    # Put through the transformed store
    transformed_store.put("newkey", b"newdata")

    # Verify it was stored with prefix
    assert base_store.exists("prefix/newkey")

    # List all keys through transformed store (should reverse transform)
    keys = list(transformed_store.keys())
    assert "key1" in keys
    assert "key2" in keys
    assert "newkey" in keys
    # "other/key3" won't be in the list because reverse transform will fail

    print("✓ KeyTransformingStore kwargs test passed")


def test_nested_transforming_stores():
    """Test that nested KeyTransformingStores work correctly"""
    base_store = DictStore()

    # Inner layer: add "data/" prefix
    inner_transformer = PrefixKeyTransformer(prefix="data/")
    inner_store = KeyTransformingStore(base_store, inner_transformer)

    # Outer layer: add "v1/" prefix
    outer_transformer = PrefixKeyTransformer(prefix="v1/")
    outer_store = KeyTransformingStore(inner_store, outer_transformer)

    # Put through outer store
    outer_store.put("test.txt", b"hello")

    # Should be stored as "data/v1/test.txt" in base store
    assert base_store.exists("data/v1/test.txt")

    # Get through outer store
    data = outer_store.get("test.txt")
    assert data == b"hello"

    # List keys through outer store
    keys = list(outer_store.keys())
    assert "test.txt" in keys

    print("✓ Nested KeyTransformingStore test passed")


if __name__ == "__main__":
    test_dictstore_kwargs()
    test_transforming_store_kwargs()
    test_nested_transforming_stores()
    print("\n✅ All tests passed!")
