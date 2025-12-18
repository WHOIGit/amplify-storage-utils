import requests

from .object import StoreError


class HttpStore:

    def __init__(self):
        pass

    def get(self, key):
        try:
            response = requests.get(key)
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
        response = requests.head(key)
        status = response.status_code
        if status == 200:
            return True
        elif status in (404, 405):
            response = requests.get(key)
            return response.status_code == 200
        return False
    
    def delete(self, key):
        raise NotImplementedError("HttpStore does not support delete operation.")
    
    def keys(self):
        raise NotImplementedError("HttpStore does not support listing keys.")
    

class AsyncHttpStore:
    
    def __init__(self):
        pass

    async def get(self, key):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(key) as response:
                    if response.status == 200:
                        return await response.read()
                    if response.status == 404:
                        raise KeyError(key)
                    try:
                        response.raise_for_status()
                    except Exception as e:
                        raise StoreError(f"HTTP error occurred: {response.status}") from e
            except KeyError:
                raise
            except Exception as e:
                raise StoreError(f"HTTP request failed for key {key}") from e
            
    async def put(self, key, data):
        raise NotImplementedError("AsyncHttpStore does not support put operation.")
    
    async def exists(self, key):
        """attempts HEAD, if that 404s, tries GET as fallback"""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.head(key) as response:
                if response.status == 200:
                    return True
                elif response.status in (404, 405):
                    async with session.get(key) as get_response:
                        return get_response.status == 200
                return False
            
    async def delete(self, key):
        raise NotImplementedError("AsyncHttpStore does not support delete operation.")
    
    async def keys(self):
        raise NotImplementedError("AsyncHttpStore does not support listing keys.")
