import pytest
import httpx

from storage.http import HttpStore, AsyncHttpStore
from storage.object import StoreError


class DummyResponse:
    def __init__(self, status_code, content=b"", raise_exc=None):
        self.status_code = status_code
        self.content = content
        self._raise_exc = raise_exc

    async def aread(self):
        return self.content

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc


def test_httpstore_get_success(monkeypatch):
    calls = {}

    def fake_get(url, follow_redirects=True):
        calls["follow_redirects"] = follow_redirects
        return DummyResponse(200, b"ok")

    monkeypatch.setattr(httpx, "get", fake_get)
    store = HttpStore(follow_redirects=False)
    assert store.get("https://example.test/object") == b"ok"
    assert calls["follow_redirects"] is False


def test_httpstore_get_not_found(monkeypatch):
    def fake_get(url, follow_redirects=True):
        return DummyResponse(404)

    monkeypatch.setattr(httpx, "get", fake_get)
    store = HttpStore()
    with pytest.raises(KeyError):
        store.get("https://example.test/missing")


def test_httpstore_get_error_raises_storeerror(monkeypatch):
    def fake_get(url, follow_redirects=True):
        return DummyResponse(500, raise_exc=ValueError("boom"))

    monkeypatch.setattr(httpx, "get", fake_get)
    store = HttpStore()
    with pytest.raises(StoreError):
        store.get("https://example.test/error")


def test_httpstore_exists_head_405_fallback(monkeypatch):
    calls = {"head": 0, "get": 0}

    def fake_head(url, follow_redirects=True):
        calls["head"] += 1
        return DummyResponse(405)

    def fake_get(url, follow_redirects=True):
        calls["get"] += 1
        return DummyResponse(200)

    monkeypatch.setattr(httpx, "head", fake_head)
    monkeypatch.setattr(httpx, "get", fake_get)
    store = HttpStore()
    assert store.exists("https://example.test/object") is True
    assert calls["head"] == 1
    assert calls["get"] == 1


@pytest.mark.asyncio
async def test_asynchttpstore_get_success(monkeypatch):
    calls = {}

    def get_response(url):
        return DummyResponse(200, b"ok")

    class FakeAsyncClient:
        def __init__(self, follow_redirects=True):
            calls["follow_redirects"] = follow_redirects

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            return get_response(url)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    store = AsyncHttpStore(follow_redirects=False)
    assert await store.get("https://example.test/object") == b"ok"
    assert calls["follow_redirects"] is False


@pytest.mark.asyncio
async def test_asynchttpstore_get_error_raises_storeerror(monkeypatch):
    def get_response(url):
        return DummyResponse(500, raise_exc=ValueError("boom"))

    class FakeAsyncClient:
        def __init__(self, follow_redirects=True):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            return get_response(url)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    store = AsyncHttpStore()
    with pytest.raises(StoreError):
        await store.get("https://example.test/error")


@pytest.mark.asyncio
async def test_asynchttpstore_exists_head_405_fallback(monkeypatch):
    calls = {"head": 0, "get": 0}

    def head_response(url):
        calls["head"] += 1
        return DummyResponse(405)

    def get_response(url):
        calls["get"] += 1
        return DummyResponse(200)

    class FakeAsyncClient:
        def __init__(self, follow_redirects=True):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def head(self, url):
            return head_response(url)

        async def get(self, url):
            return get_response(url)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    store = AsyncHttpStore()
    assert await store.exists("https://example.test/object") is True
    assert calls["head"] == 1
    assert calls["get"] == 1
