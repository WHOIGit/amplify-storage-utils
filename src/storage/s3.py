import boto3
import botocore
import aiobotocore
from aiobotocore.session import get_session

from .object import ObjectStore


class BucketStore(ObjectStore):
    def __init__(self, s3_url, s3_access_key, s3_secret_key, bucket_name, botocore_config_kwargs={}):
        self.s3_url = s3_url
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.bucket_name = bucket_name
        self.session = None
        self.s3_client = None
        self.botocore_config_kwargs = botocore_config_kwargs

    def __enter__(self):
        self.session = boto3.session.Session()
        config = botocore.config.Config(**self.botocore_config_kwargs) if self.botocore_config_kwargs else None
        self.s3_client = self.session.client(
            's3',
            endpoint_url=self.s3_url,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            config = config
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
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NoSuchKey':
                return False
            else:
                raise

    def delete(self, key):
        self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )
        return True
    
    def keys(self, prefix=''):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        for page in page_iterator:
            for obj in page.get('Contents', []):
                yield obj['Key']

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

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
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NoSuchKey':
                return False
            else:
                raise

    async def delete(self, key):
        response = await self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )
        return True
    
    async def keys(self, prefix=''):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                yield obj['Key']

    async def presigned_put(self, key, expiry=3600):
        return await self.s3_client.generate_presigned_url('put_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )

    async def presigned_get(self, key, expiry=3600):
        return await self.s3_client.generate_presigned_url('get_object',
            Params={'Key':key, 'Bucket':self.bucket_name}, ExpiresIn=expiry
        )
