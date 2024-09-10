import boto3
import aiobotocore
from aiobotocore.session import get_session

from .object import ObjectStore


class BucketStore(ObjectStore):
    def __init__(self, s3_url, s3_access_key, s3_secret_key, bucket_name):
        self.s3_url = s3_url
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.bucket_name = bucket_name
        self.session = None
        self.s3_client = None

    def __enter__(self):
        self.session = boto3.session.Session()
        self.s3_client = self.session.client(
            's3',
            endpoint_url=self.s3_url,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.s3_client.close()

    def put(self, key, data):
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data
        )
        return True
    
    def get(self, key):
        response = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=key
        )
        file_contents = response['Body'].read()
        return file_contents
    
    def exists(self, key):
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        
    def delete(self, key):
        self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )
        return True
    
    def keys(self, prefix=''):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        keys = [obj['Key'] for obj in response['Contents']]
        return keys

    def presigned_put(self, key, expiry=3600):
        return self.s3_client.generate_presigned_url('put_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )

    def presigned_get(self, key, expiry=3600):
        return self.s3_client.generate_presigned_url('get_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )

class AsyncBucketStore(ObjectStore):
    def __init__(self, s3_client, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = s3_client

    async def put(self, key, data):
        await self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data
        )
        return True
    
    async def get(self, key):
        response = await self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=key
        )
        file_contents = await response['Body'].read()
        return file_contents
    
    async def exists(self, key):
        try:
            await self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        
    async def delete(self, key):
        response = await self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )
        return True
    
    async def keys(self, prefix=''):
        # FIXME paginate
        response = await self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        keys = [obj['Key'] for obj in response['Contents']]
        return keys

    async def presigned_put(self, key, expiry=3600):
        return await self.s3_client.generate_presigned_url('put_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )

    async def presigned_get(self, key, expiry=3600):
        return await self.s3_client.generate_presigned_url('get_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )
