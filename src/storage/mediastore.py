import tempfile
import base64

import requests

from storage.object import ObjectStore

from media_store_client import ApiClient, ApiResponse
from schemas.mediastore import DownloadSchemaOutput


class MediaStore(ObjectStore):

    def __init__(self, mediastore_client: ApiClient, pid_type: str, store_config: dict):
        self.client = mediastore_client
        self.pid_type = pid_type
        self.store_config = store_config

    def put(self, key, data):
        # create temporary file because upload_media expects a file path
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
        except: ## TODO don't return False in all exception cases
            return False

    def delete(self, key):
        api_response = self.client.delete_single_media(key)

    def keys(self):
        raise NotImplementedError


class S3MediaStore(MediaStore):

    def __init__(self, mediastore_client: ApiClient, pid_type: str, store_config: dict):
        super().__init__(mediastore_client, pid_type, store_config)

    def get(self, key):
        repsonse: ApiResponse = self.client.get_download_media_url(key)
        download: DownloadSchemaOutput = repsonse.response
        url = download.presigned_get
        response = requests.get(url).content
    