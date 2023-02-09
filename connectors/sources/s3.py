#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from functools import partial
from hashlib import md5
from io import BytesIO

import aioboto3
from aiobotocore.config import AioConfig
from aiobotocore.response import AioReadTimeoutError
from aiobotocore.utils import logger as aws_logger
from aiohttp.client_exceptions import ServerTimeoutError
from botocore.exceptions import ClientError

from connectors.logger import logger, set_extra_logger
from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, get_base64_value

MAX_CHUNK_SIZE = 1048576
DEFAULT_MAX_FILE_SIZE = 10485760
DEFAULT_PAGE_SIZE = 100
DEFAULT_CONTENT_EXTRACTION = True

if "AWS_ENDPOINT_URL" in os.environ:
    AWS_ENDPOINT = f"{os.environ['AWS_ENDPOINT_URL']}:{os.environ['AWS_PORT']}"
else:
    AWS_ENDPOINT = None


class S3DataSource(BaseDataSource):
    """Amazon S3"""

    name = "Amazon S3"
    service_type = "s3"

    def __init__(self, configuration):
        """Set up the connection to the Amazon S3.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.session = aioboto3.Session()
        set_extra_logger(aws_logger, log_level=logging.DEBUG, prefix="S3")
        set_extra_logger("aioboto3.resources", log_level=logging.INFO, prefix="S3")
        self.bucket_list = []
        self.buckets = self.configuration["buckets"]
        self.config = AioConfig(
            read_timeout=self.configuration["read_timeout"],
            connect_timeout=self.configuration["connect_timeout"],
            retries={"max_attempts": self.configuration["max_attempts"]},
        )
        self.enable_content_extraction = self.configuration["enable_content_extraction"]

    @asynccontextmanager
    async def client(self, **kwargs):
        """This method creates client object."""
        if AWS_ENDPOINT is not None:
            logger.debug(f"Creating a session against {AWS_ENDPOINT}")

        async with self.session.client(
            service_name="s3", config=self.config, endpoint_url=AWS_ENDPOINT, **kwargs
        ) as s3:
            yield s3

    def _validate_configuration(self):
        """Validates whether user input is empty or not for configuration fields

        Raises:
            Exception: Configured keys can't be empty
        """
        if self.configuration["buckets"] == [""]:
            raise Exception("Configured keys: buckets can't be empty.")

    async def ping(self):
        """Verify the connection with AWS"""
        logger.info("Validating Amazon S3 Configuration...")
        self._validate_configuration()
        try:
            async with self.client() as s3:
                self.bucket_list = await s3.list_buckets()
                logger.info("Successfully connected to AWS Server.")
        except Exception:
            logger.exception("Error while connecting to AWS.")
            raise

    async def _get_content(self, doc, region, timestamp=None, doit=None):
        """Extracts the content for allowed file types.

        Args:
            doc (dict): Dictionary of document
            region (string): Name of region
            timestamp (timestamp): Timestamp of object last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Document of file content
        """
        # Reuse the same for all files
        if not (doit and self.enable_content_extraction):
            return
        filename = doc["filename"]
        bucket = doc["bucket"]
        if os.path.splitext(filename)[-1] not in TIKA_SUPPORTED_FILETYPES:
            logger.debug(f"{filename} can't be extracted")
            return
        if doc["size"] > DEFAULT_MAX_FILE_SIZE:
            logger.warning(
                f"File size for {filename} is larger than {DEFAULT_MAX_FILE_SIZE} bytes. Discarding the file content"
            )
            return
        logger.debug(f"Downloading {filename}")
        async with self.client(region_name=region) as s3:
            try:
                resp = await s3.get_object(Bucket=bucket, Key=filename)
                await asyncio.sleep(0)
                file_content, chunk = BytesIO(), True
                while chunk:
                    chunk = await resp["Body"].read(MAX_CHUNK_SIZE) or b""
                    file_content.write(chunk)
                file_content.seek(0)
                data = file_content.read()
                file_content.close()
                logger.debug(f"Downloaded {filename} for {doc['size']} bytes ")
                return {
                    "_timestamp": timestamp,
                    "_attachment": get_base64_value(content=data),
                    "_id": doc["id"],
                }
            except (ClientError, ServerTimeoutError, AioReadTimeoutError) as exception:
                if (
                    exception.response.get("Error", {}).get("Code")
                    == "InvalidObjectState"
                ):
                    logger.warning(
                        f"{filename} of {bucket} is archived and inaccessible until restored. Error: {exception}"
                    )
                else:
                    logger.error(
                        f"Something went wrong while extracting data from {filename} of {bucket}. Error: {exception}"
                    )
                    raise

    async def get_bucket_region(self, bucket_name):
        """This method return the name of region for a bucket.
        :param bucket_name (str): Name of bucket
        Returns:
            region: Name of region
        """
        region = None
        try:
            async with self.client() as s3:
                response = await s3.get_bucket_location(
                    Bucket=bucket_name,
                )
                region = response.get("LocationConstraint")
        except ClientError:
            logger.warning("Unable to fetch the region")

        return region

    def get_bucket_list(self):
        """Returns bucket list from list_buckets response

        Returns:
            list: List of buckets
        """
        return [bucket["Name"] for bucket in self.bucket_list["Buckets"]]

    async def get_docs(self, filtering=None):
        """Get documents from Amazon S3

        Returns:
            dictionary: Document of file content

        Yields:
            dictionary: Document from Amazon S3.
        """
        bucket_list = self.buckets if self.buckets != ["*"] else self.get_bucket_list()
        page_size = int(self.configuration.get("page_size", DEFAULT_PAGE_SIZE))
        for bucket in bucket_list:
            region_name = await self.get_bucket_region(bucket)
            async with self.session.resource(
                service_name="s3",
                config=self.config,
                endpoint_url=AWS_ENDPOINT,
                region_name=region_name,
            ) as s3:
                try:
                    bucket_obj = await s3.Bucket(bucket)
                    await asyncio.sleep(0)

                    async for obj_summary in bucket_obj.objects.page_size(page_size):
                        doc_id = md5(
                            f"{bucket}/{obj_summary.key}".encode("utf8")
                        ).hexdigest()
                        owner = await obj_summary.owner
                        doc = {
                            "_id": doc_id,
                            "filename": obj_summary.key,
                            "size": await obj_summary.size,
                            "bucket": bucket,
                            "owner": owner.get("DisplayName") if owner else "",
                            "storage_class": await obj_summary.storage_class,
                            "_timestamp": (await obj_summary.last_modified).isoformat(),
                        }

                        yield doc, partial(
                            self._get_content, doc=doc, region=region_name
                        )
                except Exception as exception:
                    logger.warning(
                        f"Something went wrong while fetching documents from {bucket}. Error: {exception}"
                    )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Amazon S3.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "buckets": {
                "value": "ent-search-ingest-dev",
                "label": "AWS Buckets",
                "type": "list",
            },
            "read_timeout": {
                "value": 90,
                "label": "Read timeout",
                "type": "int",
            },
            "connect_timeout": {
                "value": 90,
                "label": "Connection timeout",
                "type": "int",
            },
            "max_attempts": {
                "value": 5,
                "label": "Maximum retry attempts",
                "type": "int",
            },
            "page_size": {
                "value": DEFAULT_PAGE_SIZE,
                "label": "Maximum size of page",
                "type": "int",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
        }
