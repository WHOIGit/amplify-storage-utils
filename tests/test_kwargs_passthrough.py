#!/usr/bin/env python3
"""Test kwargs passthrough in ObjectStore.keys()"""

from storage.object import DictStore, ObjectStore
from storage.utils import KeyTransformingStore, PrefixKeyTransformer


class MockS3Store(ObjectStore):
    """Mock store that tracks what prefix was passed to keys()"""

    def __init__(self):
        self.data = {}
        self.last_prefix_arg = None

    def put(self, key, data):
        self.data[key] = data

    def get(self, key):
        return self.data[key]

    def exists(self, key):
        return key in self.data

    def delete(self, key):
        del self.data[key]

    def keys(self, prefix='', **kwargs):
        # Track what prefix was passed
        self.last_prefix_arg = prefix

        # Simulate S3 prefix filtering
        for key in self.data.keys():
            if key.startswith(prefix):
                yield key


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


def test_prefix_passthrough():
    """Test that prefix kwarg flows through nested stores to the base store"""

    # Create mock S3 store with some data
    base_store = MockS3Store()
    base_store.put("ifcb_data/2025/D20250114T172241_IFCB109/00001.png", b"data1")
    base_store.put("ifcb_data/2025/D20250114T172241_IFCB109/00002.png", b"data2")
    base_store.put("ifcb_data/2025/D20250114T172241_IFCB110/00001.png", b"data3")
    base_store.put("other_data/file.png", b"data4")

    # Create nested KeyTransformingStores (simulating our real setup)
    # Inner layer: adds "ifcb_data/" prefix
    prefix_transformer = PrefixKeyTransformer(prefix="ifcb_data/")
    prefix_store = KeyTransformingStore(base_store, prefix_transformer)

    # Outer layer: adds year/bin structure (simplified for test)
    year_transformer = PrefixKeyTransformer(prefix="2025/")
    roi_store = KeyTransformingStore(prefix_store, year_transformer)

    # Call keys with a prefix on the OUTER store
    # This should transform through both layers and reach the base store
    list(roi_store.keys(prefix="ifcb_data/2025/D20250114T172241_IFCB109/"))

    # Verify the prefix was actually passed to the base store
    assert base_store.last_prefix_arg == "ifcb_data/2025/D20250114T172241_IFCB109/"

    # Verify that the base store used it for filtering
    # (only keys matching that prefix should have been yielded)
    filtered_keys = list(base_store.keys(prefix="ifcb_data/2025/D20250114T172241_IFCB109/"))
    assert len(filtered_keys) == 2
    assert all("IFCB109" in key for key in filtered_keys)


def test_prefix_enables_efficient_s3_filtering():
    """Test that prefix enables efficient S3 filtering vs fetching all keys"""

    base_store = MockS3Store()

    # Add lots of keys in different bins
    for bin_num in range(100, 110):
        for roi_num in range(1, 6):
            key = f"ifcb_data/2025/D20250114T172241_IFCB{bin_num}/{roi_num:05d}.png"
            base_store.put(key, b"data")

    # Total: 10 bins * 5 ROIs = 50 keys
    assert len(base_store.data) == 50

    # Without prefix: would iterate all 50 keys
    all_keys = list(base_store.keys())
    assert len(all_keys) == 50

    # With prefix: only gets keys for one bin (5 keys)
    filtered_keys = list(base_store.keys(prefix="ifcb_data/2025/D20250114T172241_IFCB109/"))
    assert len(filtered_keys) == 5
    assert all("IFCB109" in key for key in filtered_keys)


if __name__ == "__main__":
    test_dictstore_kwargs()
    test_prefix_passthrough()
    test_prefix_enables_efficient_s3_filtering()
