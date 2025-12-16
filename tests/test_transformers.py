import hashlib
from io import BytesIO
import pytest

from storage.utils import (
    Base64Transformer,
    BufferTransformer,
    DataTransformer,
    GzipTransformer,
    HashPrefixKeyTransformer,
    JsonTransformer,
    KeyTransformer,
    KeyTransformingStore,
    PrefixKeyTransformer,
    RegexValidator,
    TextEncodingTransformer,
    UrlEncodingKeyTransformer,
    ValidatingKeyTransformer,
    validate_http_url_key,
)


class MemoryStore:
    """Simple in-memory ObjectStore test double."""

    def __init__(self):
        self._d = {}

    def put(self, key, data):
        self._d[key] = data

    def get(self, key):
        if key not in self._d:
            raise KeyError(key)
        return self._d[key]

    def exists(self, key):
        return key in self._d

    def delete(self, key):
        if key not in self._d:
            raise KeyError(key)
        del self._d[key]

    def keys(self):
        return set(self._d.keys())


# -------------------------
# DataTransformer tests
# -------------------------


def test_data_transformer_identity_roundtrip():
    t = DataTransformer()
    data = b"abc\x00\xff"
    assert t.transform(data) == data
    assert t.reverse_transform(data) == data


def test_text_encoding_transformer_roundtrip_default_utf8():
    t = TextEncodingTransformer()
    s = "Hello π 世界"
    b = t.transform(s)
    assert isinstance(b, (bytes, bytearray))
    assert t.reverse_transform(b) == s


def test_text_encoding_transformer_roundtrip_explicit_encoding():
    t = TextEncodingTransformer(encoding="utf-8")
    s = "café"
    assert t.reverse_transform(t.transform(s)) == s


def test_gzip_transformer_roundtrip_and_changes_bytes():
    t = GzipTransformer()
    data = b"A" * 1024  # compressible
    gz = t.transform(data)
    assert isinstance(gz, (bytes, bytearray))
    assert gz != data
    assert t.reverse_transform(gz) == data


def test_buffer_transformer_transform_reads_all_and_reverse_returns_seeked_buffer():
    t = BufferTransformer()
    original = b"\x00\x01\x02hello"
    buf = BytesIO(original)

    out = t.transform(buf)
    assert out == original

    buf2 = t.reverse_transform(out)
    assert isinstance(buf2, BytesIO)
    # reverse_transform should produce a buffer positioned at 0
    assert buf2.tell() == 0
    assert buf2.read() == original


def test_base64_transformer_roundtrip():
    t = Base64Transformer()
    data = b"\x00\xffbinary\x10data"
    enc = t.transform(data)
    assert isinstance(enc, (bytes, bytearray))
    assert enc != data
    assert t.reverse_transform(enc) == data


def test_json_transformer_roundtrip_produces_str():
    t = JsonTransformer()
    obj = {"a": 1, "b": [True, None, "x"], "c": {"nested": "ok"}}
    s = t.transform(obj)
    assert isinstance(s, str)
    assert t.reverse_transform(s) == obj


# -------------------------
# KeyTransformer tests
# -------------------------


def test_key_transformer_identity_roundtrip():
    t = KeyTransformer()
    key = "a/b/c"
    assert t.transform_key(key) == key
    assert t.reverse_transform_key(key) == key


def test_validating_key_transformer_calls_validator_and_allows_key():
    calls = {"n": 0}

    def validator(k):
        calls["n"] += 1
        assert k == "ok"

    t = ValidatingKeyTransformer(validator)
    assert t.transform_key("ok") == "ok"
    assert calls["n"] == 1


def test_validating_key_transformer_raises_keyerror_from_validator():
    def validator(_k):
        raise KeyError("bad")

    t = ValidatingKeyTransformer(validator)
    with pytest.raises(KeyError):
        t.transform_key("nope")


@pytest.mark.parametrize(
    "pattern,key,should_pass",
    [
        (r"^[a-z]+$", "abc", True),
        (r"^[a-z]+$", "abc123", False),
        (r"^foo/.*", "foo/bar", True),
        (r"^foo/.*", "bar/foo", False),
    ],
)
def test_regex_validator(pattern, key, should_pass):
    v = RegexValidator(pattern)
    if should_pass:
        v(key)  # should not raise
    else:
        with pytest.raises(KeyError):
            v(key)


@pytest.mark.parametrize(
    "key,should_pass",
    [
        ("http://example.com/a", True),
        ("https://example.com/a", True),
        ("ftp://example.com/a", False),
        ("file:///etc/passwd", False),
        ("example.com/no-scheme", False),
    ],
)
def test_validate_http_url_key(key, should_pass):
    if should_pass:
        validate_http_url_key(key)
    else:
        with pytest.raises(KeyError):
            validate_http_url_key(key)


def test_prefix_key_transformer_roundtrip():
    t = PrefixKeyTransformer(prefix="pfx/")
    key = "a/b/c"
    transformed = t.transform_key(key)
    assert transformed == "pfx/a/b/c"
    assert t.reverse_transform_key(transformed) == key


def test_hash_prefix_key_transformer_roundtrip_and_matches_sha256_prefix():
    t = HashPrefixKeyTransformer(hash_length=8, separator="/")
    key = "path/to/file.txt"
    transformed = t.transform_key(key)

    expected_prefix = hashlib.sha256(key.encode()).hexdigest()[:8]
    assert transformed.startswith(expected_prefix + "/")
    assert transformed.endswith(key)
    assert t.reverse_transform_key(transformed) == key


def test_hash_prefix_key_transformer_custom_separator():
    t = HashPrefixKeyTransformer(hash_length=4, separator="::")
    key = "k"
    transformed = t.transform_key(key)
    expected_prefix = hashlib.sha256(key.encode()).hexdigest()[:4]
    assert transformed == f"{expected_prefix}::{key}"
    assert t.reverse_transform_key(transformed) == key


def test_url_encoding_key_transformer_preserves_slashes_and_encodes_parts():
    t = UrlEncodingKeyTransformer()
    key = "folder with space/üñïçødé/100%/a+b"
    transformed = t.transform_key(key)

    # Slashes separate parts and should remain as separators.
    assert transformed.count("/") == key.count("/")

    # Some characters should be percent-encoded within parts.
    assert " " not in transformed
    assert "%" in transformed  # percent signs will appear due to encoding

    assert t.reverse_transform_key(transformed) == key


def test_key_transforming_store_applies_transform_and_reverse_on_keys_iteration():
    backing = MemoryStore()
    kt = PrefixKeyTransformer(prefix="p/")
    store = KeyTransformingStore(backing, transformer=kt)

    store.put("a/b", b"1")
    store.put("c", b"2")

    # Underlying keys are transformed
    assert backing.exists("p/a/b")
    assert backing.exists("p/c")

    # Store.keys() yields original keys (reverse-transformed)
    assert set(store.keys()) == {"a/b", "c"}

    # get/exists/delete operate on untransformed keys
    assert store.exists("a/b") is True
    assert store.get("a/b") == b"1"
    store.delete("a/b")
    assert store.exists("a/b") is False
    assert backing.exists("p/a/b") is False