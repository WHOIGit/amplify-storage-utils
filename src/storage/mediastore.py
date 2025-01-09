import tempfile
import base64

from storage.object import ObjectStore

from media_store_client import ApiClient, ApiResponse
from schemas.mediastore import DownloadSchemaOutput


class MediaStore(ObjectStore):

    def __init__(self, mediastore_client: ApiClient, pid_type: str, store_config: dict):
        self.client = mediastore_client
        self.pid_type = pid_type
        self.store_config = store_config

    def put(self, key, data):
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(data)
            self.client.upload_media(key, temp_file.name, self.pid_type, self.store_config)

    def get(self, key):
        api_response: ApiResponse = self.client.get_download_media(key)
        response: DownloadSchemaOutput = api_response.response
        b64_data = response.base64
        return base64.b64decode(b64_data)

    def exists(self, key):
        try:
            self.client.get_single_media(key)
            return True
        except:
            return False


    def delete(self, key):
        raise NotImplementedError

    def keys(self):
        raise NotImplementedError