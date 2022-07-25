#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from hashlib import md5
import asyncio
from functools import partial

import aioboto3
from connectors.source import BaseDataSource
from connectors.logger import logger


SUPPORTED_CONTENT_TYPE = [
    "text/plain",
]
SUPPORTED_FILETYPE = [".py", ".rst"]
ONE_MEGA = 1048576


class S3DataSource(BaseDataSource):
    """Amazon S3"""

    def __init__(self, connector):
        super().__init__(connector)
        self.session = aioboto3.Session()
        self.loop = asyncio.get_event_loop()

    async def close(self):
        await self.dl_client.close()

    async def ping(self):
        pass

    async def _get_content(self, key, timestamp):
        # reuse the same for all files
        async with self.session.client(
            "s3", region_name=self.configuration["region"]
        ) as s3:

            # XXX limit the size
            logger.debug(f"Downloading {key}")
            resp = await s3.get_object(Bucket=self.configuration["bucket"], Key=key)
            data = ""
            while True:
                chunk = await resp["Body"].read(ONE_MEGA)
                if not chunk:
                    break
                data += chunk.decode("utf8")
            return {"timestamp": timestamp, "text": data}

    async def get_docs(self):
        async with self.session.resource(
            "s3", region_name=self.configuration["region"]
        ) as s3:
            bucket = await s3.Bucket(self.configuration["bucket"])
            async for obj_summary in bucket.objects.all():
                doc_id = md5(obj_summary.key.encode("utf8")).hexdigest()
                obj = await obj_summary.Object()
                last_modified = await obj.last_modified

                doc = {
                    "_id": doc_id,
                    "filename": obj_summary.key,
                    "size": await obj_summary.size,
                    "content-type": await obj.content_type,
                    "timestamp": last_modified.isoformat(),
                }

                async def _download(doc_id, timestamp=None, doit=None):
                    if not doit:
                        return
                    # XXX check the checksum_crc32 of the file
                    if (
                        doc["content-type"] not in SUPPORTED_CONTENT_TYPE
                        and os.path.splitext(obj_summary.key)[-1]
                        not in SUPPORTED_FILETYPE
                    ):
                        return

                    content = await self._get_content(
                        obj_summary.key, last_modified.isoformat()
                    )
                    content["_id"] = doc_id
                    return content

                yield doc, partial(_download, doc_id)

    @classmethod
    def get_default_configuration(cls):
        return {
            "bucket": {
                "value": "ent-search-ingest-dev",
                "label": "AWS Bucket",
                "type": "str",
            },
            "region": {
                "value": "eu-central-1",
                "label": "AWS Region",
                "type": "str",
            },
        }
