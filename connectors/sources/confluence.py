#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Confluence source module responsible to fetch documents from Confluence Cloud/Server.
"""
import os

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import CancellableSleeps, iso_utc, ssl_context

FILE_SIZE_LIMIT = 10485760
RETRY_INTERVAL = 2
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.is_cloud = self.configuration["is_cloud"]
        self.host_url = self.configuration["host_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]
        if self.is_cloud:
            self.host_url = os.path.join(self.host_url, "wiki")

        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False
        self.session = None

    def get_session(self):
        """Generate and return base client session with configuration fields

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        if self.session:
            return self.session
        if self.is_cloud:
            auth = (
                self.configuration["service_account_id"],
                self.configuration["api_token"],
            )
        else:
            auth = self.configuration["username"], self.configuration["password"]

        basic_auth = aiohttp.BasicAuth(login=auth[0], password=auth[1])
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        self.session = aiohttp.ClientSession(
            auth=basic_auth,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        self.session = None

    async def _api_call(self, url):
        """Make a GET call for Atlassian API using the passed url with retry for the failed API calls.

        Args:
            url: Request URL to hit the get call

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Client response
        """
        retry_counter = 0
        while True:
            try:
                async with self.get_session().get(
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    yield response
                    break
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.session.close()  # pyright: ignore
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry_counter)


class ConfluenceDataSource(BaseDataSource):
    """Confluence"""

    name = "Confluence"
    service_type = "confluence"

    def __init__(self, configuration):
        """Setup the connection to Confluence

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.enable_content_extraction = self.configuration["enable_content_extraction"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.confluence_client = ConfluenceClient(configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Confluence

        Returns:
            dictionary: Default configuration.
        """
        return {
            "is_cloud": {
                "value": False,
                "label": "True if Confluence Cloud, False if Confluence Server",
                "type": "bool",
            },
            "username": {
                "value": "admin",
                "label": "Confluence Server username",
                "type": "str",
            },
            "password": {
                "value": "abc@123",
                "label": "Confluence Server password",
                "type": "str",
            },
            "service_account_id": {
                "value": "me@example.com",
                "label": "Confluence Cloud username",
                "type": "str",
            },
            "api_token": {
                "value": "abc#123",
                "label": "Confluence Cloud API token",
                "type": "str",
            },
            "host_url": {
                "value": "http://127.0.0.1:5000",
                "label": "Confluence host url",
                "type": "str",
            },
            "ssl_enabled": {
                "value": False,
                "label": "Enable SSL verification (true/false)",
                "type": "bool",
            },
            "ssl_ca": {
                "value": "",
                "label": "SSL certificate",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": True,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
            "retry_count": {
                "value": 3,
                "label": "Maximum retries per request",
                "type": "int",
            },
            "concurrent_downloads": {
                "value": MAX_CONCURRENT_DOWNLOADS,
                "label": "Maximum concurrent downloads",
                "type": "int",
            },
        }

    async def close(self):
        """Closes unclosed client session"""
        await self.confluence_client.close_session()

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Confluence

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields

        Raises:
            Exception: Configured fields can't be empty.
            Exception: SSL certificate must be configured.
            Exception: Concurrent downloads can't be set more than maximum allowed value.
        """
        logger.info("Validating Confluence Configuration...")

        connection_fields = (
            ["host_url", "service_account_id", "api_token"]
            if self.confluence_client.is_cloud
            else ["host_url", "username", "password"]
        )
        default_config = self.get_default_configuration()

        if empty_connection_fields := [
            default_config[field]["label"]
            for field in connection_fields
            if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )
        if self.confluence_client.ssl_enabled and (
            self.confluence_client.certificate == ""
            or self.confluence_client.certificate is None
        ):
            raise Exception("SSL certificate must be configured.")

        if self.concurrent_downloads > MAX_CONCURRENT_DOWNLOADS:
            raise Exception(
                f"Configured concurrent downloads can't be set more than {MAX_CONCURRENT_DOWNLOADS}."
            )

    async def ping(self):
        """Verify the connection with Confluence"""
        try:
            await anext(
                self.confluence_client._api_call(
                    url=os.path.join(self.confluence_client.host_url, PING_URL),
                )
            )
            logger.info("Successfully connected to Confluence")
        except Exception:
            logger.exception("Error while connecting to Confluence")
            raise

    async def get_docs(self, filtering=None):
        # yield dummy document to make the chunked PR work, subsequent PR will replace it with actual implementation
        yield {"_id": "123", "timestamp": iso_utc()}, None
