from base64 import b64encode
import re
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from storage.config_builder import StoreFactory, ConfigError
from storage.utils import Base64TransmissionStore
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


@patch.dict('os.environ', {'VAR1': 'apple', 'VAR2': 'orange'}, clear=True)
def test_parse_env_vars():
    """ Test parsing environment variables in YAML. """
    store = load_yaml(cfg("vars.yaml"))

    assert store.get('item1') == 'apple' # variable present
    assert store.get('item2') == 'orange' # variable present, default ignored 
    assert store.get('item3') == 'blueberry' # variable absent, default used


@patch.dict("os.environ", {"EMPTY_VAR": ""}, clear=True)
def test_empty_env_var_value():
    """ Empty env values should not fall back to defaults. """
    store = load_yaml(cfg("empty_vars.yaml"))

    assert store.get("empty_value") == ""
    assert store.get("missing_value") == "missing-default"


def test_default_value_with_colons():
    """ Defaults can include colons and punctuation. """
    store = load_yaml(cfg("colon_default.yaml"))

    assert store.get("url") == "http://localhost:8080/api:v1"


@patch.dict("os.environ", {"DEEP_VAR": "deep", "LIST_VAR": "from-env"}, clear=True)
def test_env_vars_in_nested_structures():
    """ Ensure nested lists/dicts resolve environment variables. """
    store = load_yaml(cfg("nested_vars.yaml"))
    nested = store.get("nested")

    assert nested["list"] == ["static", "from-env", {"deep": "deep"}]


def test_missing_env_var():
    """ Ensure that missing env variables are caught. """
    with pytest.raises(ConfigError, match=re.escape("Environment variable 'MISSING' not found. Please set MISSING or provide a default value using ${MISSING:-default}")):
        load_yaml(cfg("missing_var.yaml"))


@patch('storage.utils.format_image')
@patch('storage.utils.DataDirectory')
@patch('storage.utils.Pid')
def test_b64_roi_store(mock_pid, mock_data_dir, mock_format):
    """ Confirm that Base64TransmissionStore and IFCBRoiStore function properly. """
    # Configure PID mock
    mock_pid_instance = Mock()
    mock_pid_instance.bin_lid = "D20231015T123456_IFCB001"
    mock_pid_instance.target = "5"
    mock_pid.return_value = mock_pid_instance

    # Configure format_image mock
    test_png_data = b'iVBORw0KGgoAAAANSUhEUgAAAGQAAABkCAIAAAD/gAIDAAABHElEQVR4nO3YsQ0CMRAAQZtq6IeQkqiD8Pv5Cr4OciQsNiBAP5Nal6wusG5u+zE+u12fi9dtv59q9rJ4441YgViBWIFYgViBWIFYgVjBHOOxeP7Hf/bvZm1WIFYgViBWIFYgViBWIFYgVjDd4L+ftVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWIEbfJi1WYFYgViBWIFYgViBWIFYgViBG3yYtVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWIEbfJi1WYFYgViBWIFYgViBWIFYgViBG3yYtVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWMEL4p6WjTDs8dYAAAAASUVORK5CYII=' # placeholder checkerboard image
    mock_buffer = Mock()
    mock_buffer.getvalue.return_value = test_png_data
    mock_format.return_value = mock_buffer

    # Configure DataDirectory mock
    mock_single_bin = MagicMock()
    mock_single_bin.images = {5: "fake_image_array"}
    
    mock_bin = MagicMock()
    mock_bin.as_single.return_value.__enter__.return_value = mock_single_bin
    mock_bin.as_single.return_value.__exit__ = MagicMock(return_value=False)
    mock_bin.images.keys.return_value = [5, 10, 15]

    mock_dir_instance = MagicMock()
    mock_dir_instance.__getitem__.return_value = mock_bin
    mock_dir_instance.__contains__.return_value = True
    mock_data_dir.return_value = mock_dir_instance

    store = load_yaml(cfg("ifcb_roi.yaml"))

    result = store.get("D20231015T123456_IFCB001_00005")
    expected = b64encode(test_png_data)
    assert result == expected

    assert store.exists("D20231015T123456_IFCB001_00005") is True

    mock_pid.assert_called_with("D20231015T123456_IFCB001_00005")
    mock_data_dir.assert_called_with("/placeholder/dir")
    mock_format.assert_called_with("fake_image_array", "image/png")


@patch('storage.utils.format_image')
@patch('storage.utils.DataDirectory')
@patch('storage.utils.Pid')
def test_ifcb_roi_base64_store_classmethod(mock_pid, mock_data_dir, mock_format):
    """ Test the IfcbRoiStore.base64_store() class method. """
    from base64 import b64encode, b64decode
    from storage.utils import IfcbRoiStore, Base64Store

    # Configure the same mocks as before
    mock_pid_instance = MagicMock()
    mock_pid_instance.bin_lid = "D20231015T123456_IFCB001"
    mock_pid_instance.target = "5"
    mock_pid.return_value = mock_pid_instance

    test_png_data = b'iVBORw0KGgoAAAANSUhEUgAAAGQAAABkCAIAAAD/gAIDAAABHElEQVR4nO3YsQ0CMRAAQZtq6IeQkqiD8Pv5Cr4OciQsNiBAP5Nal6wusG5u+zE+u12fi9dtv59q9rJ4441YgViBWIFYgViBWIFYgVjBHOOxeP7Hf/bvZm1WIFYgViBWIFYgViBWIFYgVjDd4L+ftVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWIEbfJi1WYFYgViBWIFYgViBWIFYgViBG3yYtVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWIEbfJi1WYFYgViBWIFYgViBWIFYgViBG3yYtVmBWIFYgViBWIFYgViBWIFYgRt8mLVZgViBWIFYgViBWIFYgViBWMEL4p6WjTDs8dYAAAAASUVORK5CYII='
    mock_buffer = MagicMock()
    mock_buffer.getvalue.return_value = test_png_data
    mock_format.return_value = mock_buffer

    mock_single_bin = MagicMock()
    mock_single_bin.images = {5: "fake_image_array"}

    mock_bin = MagicMock()
    mock_bin.as_single.return_value.__enter__.return_value = mock_single_bin
    mock_bin.images.keys.return_value = [5, 10, 15]

    mock_dir_instance = MagicMock()
    mock_dir_instance.__getitem__.return_value = mock_bin
    mock_dir_instance.__contains__.return_value = True
    mock_data_dir.return_value = mock_dir_instance

    # Call the class method
    store = IfcbRoiStore.base64_store(data_dir="/test/data/ifcb")

    # Verify it returns a Base64Store instance
    assert isinstance(store, Base64TransmissionStore)

    # Test that get returns base64-encoded data
    # Note: Base64Store encodes for storage and decodes for retrieval
    # So we need to pass b64-encoded data and expect decoded back
    result = store.get("D20231015T123456_IFCB001_00005")

    # Base64Store.get should decode the underlying PNG data
    expected = b64encode(test_png_data)
    assert result == expected

    # Verify exists works
    assert store.exists("D20231015T123456_IFCB001_00005") is True
