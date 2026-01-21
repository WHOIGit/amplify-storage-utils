import boto3
import botocore
import aiobotocore
from aiobotocore.session import get_session

from .object import ObjectStore


class BucketStore(ObjectStore):
    def __init__(self, bucket_name, client=None):
        self.s3_client = client
        self.bucket_name = bucket_name

    def set_client(self, client):
        """Inject an S3 client instance."""
        self.s3_client = client

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
    
    def __init__(self, bucket_name, client=None):
        self.s3_client = client
        self.bucket_name = bucket_name

    def set_client(self, client):
        """Inject an async S3 client instance."""
        self.s3_client = client

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

    def presigned_put(self, key, expiry=3600):
        return self.s3_client.generate_presigned_url(
            'put_object',
            Params={'Key': key, 'Bucket': self.bucket_name},
            ExpiresIn=expiry,
        )

    def presigned_get(self, key, expiry=3600):
        return self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Key': key, 'Bucket': self.bucket_name},
            ExpiresIn=expiry,
        )
