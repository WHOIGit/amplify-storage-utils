import io
import botocore


class AsyncBody:
    """ Mock streaming object. """

    def __init__(self, data):
        self._buf = io.BytesIO(data)

    async def read(self):
        return self._buf.getvalue()


class _PaginatorImpl:
    """ Mock async paginator. """

    def __init__(self, store_ref):
        self._store = store_ref

    async def paginate(self, *, Bucket, Prefix=""):
        contents = [
            {"Key": key}
            for (b, key), _ in self._store.items()
            if b == Bucket and key.startswith(Prefix)
        ]
        yield {"Contents": contents}


class MockS3:
    """ Mock async S3 implementation for testing. """
    
    def __init__(self):
        self._store = {}


    async def put_object(self, *, Bucket, Key, Body):
        self._store[(Bucket, Key)] = Body
        return {}


    async def get_object(self, *, Bucket, Key):
        try:
            data = self._store[(Bucket, Key)]
        except KeyError:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
            )
        return {"Body": AsyncBody(data)}


    async def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return {}


    async def delete_object(self, *, Bucket, Key):
        self._store.pop((Bucket, Key), None)
        return {}


    def get_paginator(self, name):
        return _PaginatorImpl(self._store)


    async def generate_presigned_url(self, op_name, *, Params, ExpiresIn):
        return f"https://example.test/{Params['Bucket']}/{Params['Key']}?op={op_name}&exp={ExpiresIn}"
