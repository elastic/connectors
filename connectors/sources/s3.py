#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import logging
import os
from contextlib import AsyncExitStack
from functools import partial
from hashlib import md5

import aioboto3
import aiofiles
from aiobotocore.config import AioConfig
from aiobotocore.response import AioReadTimeoutError
from aiobotocore.utils import logger as aws_logger
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerTimeoutError
from botocore.exceptions import ClientError

from connectors.logger import logger, set_extra_logger
from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, convert_to_b64

MAX_CHUNK_SIZE = 1048576
DEFAULT_MAX_FILE_SIZE = 10485760
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_RETRY_ATTEMPTS = 5
DEFAULT_CONNECTION_TIMEOUT = 90
DEFAULT_READ_TIMEOUT = 90

if "AWS_ENDPOINT_URL" in os.environ:
    AWS_ENDPOINT = f"{os.environ['AWS_ENDPOINT_URL']}:{os.environ['AWS_PORT']}"
else:
    AWS_ENDPOINT = None


class S3Client:
    """Amazon S3 client to handle method calls made to S3"""

    def __init__(self, configuration):
        self.configuration = configuration
        self._logger = logger
        self.session = aioboto3.Session(
            aws_access_key_id=self.configuration["aws_access_key_id"],
            aws_secret_access_key=self.configuration["aws_secret_access_key"],
        )
        set_extra_logger(aws_logger, log_level=logging.DEBUG, prefix="S3")
        set_extra_logger("aioboto3.resources", log_level=logging.INFO, prefix="S3")
        self.config = AioConfig(
            read_timeout=self.configuration["read_timeout"],
            connect_timeout=self.configuration["connect_timeout"],
            retries={"max_attempts": self.configuration["max_attempts"]},
        )
        self.clients = {}
        self.client_context = []

    def set_logger(self, logger_):
        self._logger = logger_

    async def client(self, region=None):
        """This method creates context manager and client session object for s3.
        Args:
            region (str): Name of bucket region. Defaults to None
        """
        region_name = region if region else "default"

        if region_name in self.clients:
            return self.clients[region_name]

        if AWS_ENDPOINT is not None:
            self._logger.debug(f"Creating a session against {AWS_ENDPOINT}")

        # AsyncExitStack, supports asynchronous context managers, used to create client using enter_async_context and
        # these context manager will be stored in client_context list also client will be stored in clients dict with their region
        s3_context_stack = AsyncExitStack()
        s3_client = await s3_context_stack.enter_async_context(
            self.session.client(
                service_name="s3",
                config=self.config,
                endpoint_url=AWS_ENDPOINT,
                region_name=region,
            )
        )
        self.client_context.append(s3_context_stack)

        self.clients[region_name] = s3_client
        return self.clients[region_name]

    async def close_client(self):
        """Closes unclosed client session"""
        for context in self.client_context:
            await context.aclose()

    async def fetch_buckets(self):
        """This method used to list all the buckets from Amazon S3"""
        s3 = await self.client()
        await s3.list_buckets()

    async def get_bucket_list(self):
        """Returns bucket list from list_buckets response

        Returns:
            list: List of buckets
        """
        if self.configuration["buckets"] == ["*"]:
            s3 = await self.client()
            bucket_list = await s3.list_buckets()
            buckets = [bucket["Name"] for bucket in bucket_list["Buckets"]]
        else:
            buckets = self.configuration["buckets"]
        return buckets

    async def get_bucket_objects(self, bucket):
        """Returns bucket list from list_buckets response
        Args:
            bucket (str): Name of bucket
        Yields:
            obj_summary: Bucket objects metadata
            s3_client: S3 client object
        """
        page_size = self.configuration["page_size"]
        region_name = await self.get_bucket_region(bucket)
        s3_client = await self.client(region=region_name)
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
                    yield obj_summary, s3_client
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while fetching documents from {bucket}. Error: {exception}"
                )

    async def get_bucket_region(self, bucket_name):
        """This method return the name of region for a bucket.
        Args
            bucket_name (str): Name of bucket
        Returns:
            region: Name of region
        """
        region = None
        try:
            s3 = await self.client()
            response = await s3.get_bucket_location(
                Bucket=bucket_name,
            )
            region = response.get("LocationConstraint")
        except ClientError:
            self._logger.warning(f"Unable to fetch the region for {bucket_name}")

        return region

    async def get_content(self, doc, s3_client, timestamp=None, doit=None):
        """Extracts the content for allowed file types.

        Args:
            doc (dict): Dictionary of document
            s3_client (obj): S3 client instance
            timestamp (timestamp): Timestamp of object last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Document of file content
        """
        # Reuse the same for all files
        if not (doit):
            return
        filename = doc["filename"]
        if (os.path.splitext(filename)[-1]).lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(f"{filename} can't be extracted")
            return
        if doc["size_in_bytes"] > DEFAULT_MAX_FILE_SIZE:
            self._logger.warning(
                f"File size for {filename} is larger than {DEFAULT_MAX_FILE_SIZE} bytes. Discarding the file content"
            )
            return

        bucket = doc["bucket"]
        self._logger.debug(f"Downloading {filename}")
        document = {
            "_id": doc["id"],
            "_timestamp": doc["_timestamp"],
        }
        source_file_name = ""
        try:
            async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
                source_file_name = async_buffer.name
                await s3_client.download_fileobj(
                    Bucket=bucket, Key=filename, Fileobj=async_buffer
                )
            await asyncio.to_thread(
                convert_to_b64,
                source=source_file_name,
            )
            async with aiofiles.open(file=source_file_name, mode="r") as async_buffer:
                document["_attachment"] = (await async_buffer.read()).strip()

            self._logger.debug(
                f"Downloaded {filename} for {doc['size_in_bytes']} bytes "
            )
            return document
        except ClientError as exception:
            if (
                getattr(exception, "response", {}).get("Error", {}).get("Code")
                == "InvalidObjectState"
            ):
                self._logger.warning(
                    f"{filename} of {bucket} is archived and inaccessible until restored. Error: {exception}"
                )
            else:
                self._logger.error(
                    f"Something went wrong while extracting data from {filename} of {bucket}. Error: {exception}"
                )
                raise
        except (ServerTimeoutError, AioReadTimeoutError) as exception:
            self._logger.error(
                f"Something went wrong while extracting data from {filename} of {bucket}. Error: {exception}"
            )
            raise
        finally:
            await remove(source_file_name)  # pyright: ignore


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
        self.s3_client = S3Client(configuration=configuration)

    def _set_internal_logger(self):
        self.s3_client.set_logger(self._logger)

    async def ping(self):
        """Verify the connection with AWS"""
        try:
            await self.s3_client.fetch_buckets()
            self._logger.info("Successfully connected to AWS.")
        except Exception:
            self._logger.exception("Error while connecting to AWS.")
            raise

    async def format_document(self, bucket_name, bucket_object):
        """Prepare document for bucket object.

        Args:
            bucket_name: Name of bucket
            bucket_object: Response of bucket objects
        Returns:
            document: Modified document.
        """

        doc_id = md5(f"{bucket_name}/{bucket_object.key}".encode("utf8")).hexdigest()
        owner = await bucket_object.owner
        document = {
            "_id": doc_id,
            "filename": bucket_object.key,
            "size_in_bytes": await bucket_object.size,
            "bucket": bucket_name,
            "owner": owner.get("DisplayName") if owner else "",
            "storage_class": await bucket_object.storage_class,
            "_timestamp": (await bucket_object.last_modified).isoformat(),
        }
        return document

    async def get_docs(self, filtering=None):
        """Get documents from Amazon S3

        Returns:
            dictionary: Document of file content

        Yields:
            dictionary: Document from Amazon S3.
        """
        bucket_list = await self.s3_client.get_bucket_list()
        for bucket in bucket_list:
            async for obj_summary, s3_client in self.s3_client.get_bucket_objects(
                bucket
            ):
                document = await self.format_document(
                    bucket_name=bucket, bucket_object=obj_summary
                )
                yield document, partial(
                    self.s3_client.get_content,
                    doc=document,
                    s3_client=s3_client,
                )

    async def close(self):
        """Closes unclosed client session"""
        await self.s3_client.close_client()

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Amazon S3.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "buckets": {
                "display": "textarea",
                "label": "AWS Buckets",
                "order": 1,
                "type": "list",
            },
            "aws_access_key_id": {
                "label": "AWS Access Key Id",
                "order": 2,
                "type": "str",
            },
            "aws_secret_access_key": {
                "label": "AWS Secret Key",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "read_timeout": {
                "default_value": DEFAULT_READ_TIMEOUT,
                "display": "numeric",
                "label": "Read timeout",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "connect_timeout": {
                "default_value": DEFAULT_CONNECTION_TIMEOUT,
                "display": "numeric",
                "label": "Connection timeout",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "max_attempts": {
                "default_value": DEFAULT_MAX_RETRY_ATTEMPTS,
                "display": "numeric",
                "label": "Maximum retry attempts",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "page_size": {
                "default_value": DEFAULT_PAGE_SIZE,
                "display": "numeric",
                "label": "Maximum size of page",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }
