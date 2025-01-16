import requests
import base64
from typing import Optional
from storage.object import ObjectStore

class MediaStore(ObjectStore):
    """
    Implementation of ObjectStore that uses the NinjaAPI REST interface for media storage.
    Handles authentication and implements the object store interface by making
    appropriate API calls.
    """
    def __init__(self, 
                 base_url: str, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 token: Optional[str] = None,
                 store_config: Optional[dict] = None,
                 pid_type: str = "DEFAULT"):
        """
        Initialize the MediaStore with connection details.
        
        Args:
            base_url: Base URL of the API (e.g. "http://api.example.com")
            username: Username for authentication
            password: Password for authentication
            store_config: Optional store configuration for media creation. If not provided,
                        will use the first available store.
            pid_type: Type of PID to use for new media objects (default: "custom")
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self._token = token
        self.store_config = store_config
        self.pid_type = pid_type
        self._session = requests.Session()

    def __enter__(self):
        """Authenticate when entering context"""
        self._authenticate()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Clean up session when exiting context"""
        self._session.close()

    def _authenticate(self):
        """Authenticate with the API and store the token"""
        if self._token is None:
            response = self._session.post(
                f"{self.base_url}/api/login",
                json={"username": self.username, "password": self.password}
            )
            response.raise_for_status()
            self._token = response.json()["token"]
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

    def _ensure_store_config(self):
        """Ensure we have a store configuration, fetching the first one if needed"""
        if self.store_config is None:
            response = self._session.get(f"{self.base_url}/api/stores")
            response.raise_for_status()
            stores = response.json()
            if not stores:
                raise RuntimeError("No stores available")
            self.store_config = stores[0]

    def get(self, key) -> bytearray:
        """Get object data by key (pid)"""
        response = self._session.get(f"{self.base_url}/api/download/{key}")
        response.raise_for_status()
        
        data = response.json()
        if data.get("error"):
            raise KeyError(f"Failed to get object: {data['error']}")
            
        # Handle either base64 or presigned URL
        if data.get("base64"):
            return bytearray(base64.b64decode(data["base64"]))
        elif data.get("presigned_get"):
            download_response = requests.get(data["presigned_get"])
            download_response.raise_for_status()
            return bytearray(download_response.content)
        else:
            raise RuntimeError("No data returned from API")

    def put(self, key: str, data: bytearray):
        """Store object data with given key (pid)"""
        self._ensure_store_config()

        # Prepare media creation request
        media_data = {
            "pid": key,
            "pid_type": self.pid_type,
            "store_config": self.store_config
        }
        
        # First, make request with just metadata to see if we get a presigned URL
        upload_data = {
            "mediadata": media_data
        }
        
        response = self._session.post(
            f"{self.base_url}/api/upload",
            json=upload_data
        )
        response.raise_for_status()
        
        result = response.json()
        if result.get("error"):
            raise RuntimeError(f"Failed to upload: {result['error']}")
        
        # If we got a presigned URL, use that
        if result.get("presigned_put"):
            upload_response = requests.put(
                result["presigned_put"],
                data=data
            )
            upload_response.raise_for_status()
        else:
            # No presigned URL, use base64
            base64_data = base64.b64encode(data).decode('utf-8')
            upload_data["base64"] = base64_data
            
            response = self._session.post(
                f"{self.base_url}/api/upload",
                json=upload_data
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get("error"):
                raise RuntimeError(f"Failed to upload: {result['error']}")

    def exists(self, key: str) -> bool:
        """Check if object exists by key (pid)"""
        try:
            response = self._session.get(f"{self.base_url}/api/media/{key}")
            return response.status_code == 200
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise

    def delete(self, key: str):
        """Delete object by key (pid)"""
        response = self._session.delete(f"{self.base_url}/api/media/{key}")
        if response.status_code == 404:
            raise KeyError(key)
        response.raise_for_status()

    def keys(self):
        """List all object keys (pids)"""
        response = self._session.get(f"{self.base_url}/api/media/dump")
        response.raise_for_status()
        return [media["pid"] for media in response.json()]
