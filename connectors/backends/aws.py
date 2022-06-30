#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import aioboto3


class S3Connector:
    """Amazon S3
    """
    def __init__(self, definition):
        self.session = aioboto3.Session()
        self.bucket = definition["bucket"]
        self.region = definition.get("region", "eu-central-1")

    async def get_docs(self):
        async with session.resource("s3", region_name=self.region) as s3:
            bucket = await s3.Bucket(self.bucket)
            async for s3_object in bucket.objects.all():
                yield s3_object
