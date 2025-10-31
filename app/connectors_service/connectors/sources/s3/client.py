#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from contextlib import AsyncExitStack

import aioboto3
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError
from connectors_sdk.logger import logger

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
            self.session.client(  # type: ignore[arg-type]
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

    async def get_bucket_objects(self, bucket, **kwargs):
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

                if kwargs.get("prefix"):
                    objects = bucket_obj.objects.filter(
                        Prefix=kwargs["prefix"]
                    ).page_size(page_size)
                else:
                    objects = bucket_obj.objects.page_size(page_size)

                async for obj_summary in objects:
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
