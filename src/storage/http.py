import httpx

from .object import StoreError


class HttpStore:

    def __init__(self, follow_redirects=True):
        self._follow_redirects = follow_redirects

    def get(self, key):
        try:
            response = httpx.get(key, follow_redirects=self._follow_redirects)
        except Exception as e:
            raise StoreError(f"HTTP request failed for key {key}") from e
        if response.status_code == 200:
            return response.content
        if response.status_code == 404:
            raise KeyError(key)
        try:
            response.raise_for_status()
        except Exception as e:
            raise StoreError(f"HTTP error occurred: {response.status_code}") from e
    
    def put(self, key, data):
        raise NotImplementedError("HttpStore does not support put operation.")
    
    def exists(self, key):
        """attempts HEAD, if that 404s, tries GET as fallback"""
        try:
            response = httpx.head(key, follow_redirects=self._follow_redirects)
        except Exception as e:
            raise StoreError(f"HTTP request failed for key {key}") from e
        status = response.status_code
        if status == 200:
            return True
        elif status == 405:  # Method Not Allowed
            try:
                response = httpx.get(key, follow_redirects=self._follow_redirects)
            except Exception as e:
                raise StoreError(f"HTTP request failed for key {key}") from e
            return response.status_code == 200
        return False
    
    def delete(self, key):
        raise NotImplementedError("HttpStore does not support delete operation.")
    
    def keys(self, **kwargs):
        raise NotImplementedError("HttpStore does not support listing keys.")
    

class AsyncHttpStore:
    
    def __init__(self, follow_redirects=True):
        self._follow_redirects = follow_redirects

    async def get(self, key):
        async with httpx.AsyncClient(follow_redirects=self._follow_redirects) as client:
            try:
                response = await client.get(key)
                if response.status_code == 200:
                    return await response.aread()
                if response.status_code == 404:
                    raise KeyError(key)
                try:
                    response.raise_for_status()
                except Exception as e:
                    raise StoreError(f"HTTP error occurred: {response.status_code}") from e
            except KeyError:
                raise
            except Exception as e:
                raise StoreError(f"HTTP request failed for key {key}") from e
            
    async def put(self, key, data):
        raise NotImplementedError("AsyncHttpStore does not support put operation.")
    
    async def exists(self, key):
        """attempts HEAD, if that 404s, tries GET as fallback"""
        async with httpx.AsyncClient(follow_redirects=self._follow_redirects) as client:
            try:
                response = await client.head(key)
                status = response.status_code
                if status == 200:
                    return True
                elif status == 405:  # Method Not Allowed
                    try:
                        get_response = await client.get(key)
                        return get_response.status_code == 200
                    except Exception as e:
                        raise StoreError(f"HTTP request failed for key {key}") from e
                return False
            except Exception as e:
                raise StoreError(f"HTTP request failed for key {key}") from e
            
    async def delete(self, key):
        raise NotImplementedError("AsyncHttpStore does not support delete operation.")
    
    async def keys(self, **kwargs):
        raise NotImplementedError("AsyncHttpStore does not support listing keys.")
