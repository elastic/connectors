import aioboto3


class S3Connector:
    def __init__(self, definition):
        self.session = aioboto3.Session()
        self.bucket = definition["bucket"]
        self.region = definition.get("region", "eu-central-1")

    async def get_docs(self):
        async with session.resource("s3", region_name=self.region) as s3:
            bucket = await s3.Bucket(self.bucket)
            async for s3_object in bucket.objects.all():
                yield s3_object
