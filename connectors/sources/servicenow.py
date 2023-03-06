#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""
import json

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import CancellableSleeps, iso_utc

RETRY_INTERVAL = 2
FETCH_SIZE = 1000
MAX_CONCURRENCY_SUPPORT = 15

ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
}


class InvalidResponse(Exception):
    """Raise when retrieving invalid response..

    Args:
        Exception (InvalidResponse): Invalid response exception.
    """

    pass


class ServiceNowDataSource(BaseDataSource):
    """ServiceNow"""

    name = "ServiceNow"
    service_type = "servicenow"

    def __init__(self, configuration):
        """Setup the connection to the ServiceNow instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        super().__init__(configuration=configuration)
        self.session = None
        self.sleeps = CancellableSleeps()
        self.retry_count = self.configuration["retry_count"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by ServiceNow

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for ServiceNow.

        Returns:
            dict: Default configuration.
        """
        return {
            "url": {
                "value": "http://127.0.0.1:9318",
                "label": "Service URL",
                "type": "str",
            },
            "username": {
                "value": "admin",
                "label": "Username",
                "type": "str",
            },
            "password": {
                "value": "changeme",
                "label": "Password",
                "type": "str",
            },
            "services": {
                "value": "*",
                "label": "List of services",
                "type": "list",
            },
            "retry_count": {
                "value": 3,
                "label": "Maximum retries for failed requests",
                "type": "int",
            },
            "enable_content_extraction": {
                "value": True,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
            "concurrent_downloads": {
                "value": MAX_CONCURRENCY_SUPPORT,
                "label": "Maximum concurrent downloads",
                "type": "int",
            },
        }

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.

        Raises:
            Exception: Configured keys can't be empty.
            Exception: Invalid configured concurrent_downloads.
        """

        logger.info("Validating ServiceNow Configuration")
        connection_fields = ["url", "username", "password", "services"]
        default_config = self.get_default_configuration()

        if empty_connection_fields := [
            default_config[field]["label"]
            for field in connection_fields
            if self.configuration[field] in ["", [""]]
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )

        if self.concurrent_downloads > MAX_CONCURRENCY_SUPPORT:
            raise Exception(
                f"Configured concurrent downloads can't be set more than {MAX_CONCURRENCY_SUPPORT}."
            )

    def _generate_session(self):
        """Generate aiohttp client session with configuration fields."""

        logger.debug("Generating aiohttp client session")
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY_SUPPORT)
        basic_auth = aiohttp.BasicAuth(
            login=self.configuration["username"],
            password=self.configuration["password"],
        )
        request_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore

        self.session = aiohttp.ClientSession(
            connector=connector,
            base_url=self.configuration["url"],
            auth=basic_auth,
            headers=request_headers,
            timeout=timeout,
            raise_for_status=True,
        )

    async def _api_call(self, url, params, is_attachment=False):
        """Handle every api call to ServiceNow instance with retries.

        Args:
            url (str): ServiceNow query to be executed.
            params (dict): Params to format the query.
            is_attachment (bool, False): Flag for handle attachment api call. Defaults to False.

        Raises:
            InvalidResponse: An instance of InvalidResponse class.
            Exception: An instance of an exception class.

        Yields:
            list: Table & Attachment metadata.
            str: Attachment content.
        """
        retry = 1
        offset = 0

        if not is_attachment:
            query = "ORDERBYsys_created_on^"
            if "sysparm_query" in params.keys():
                query += params["sysparm_query"]
            params.update({"sysparm_query": query, "sysparm_limit": FETCH_SIZE})

        while True:
            if not is_attachment:
                params["sysparm_offset"] = offset

            try:
                async with self.session.get(  # pyright: ignore
                    url=url, params=params
                ) as response:
                    if response.headers.get("Connection") == "close":
                        raise Exception("Couldn't connect to ServiceNow instance")

                    if is_attachment:
                        yield response
                        break

                    fetched_response = await response.read()
                    if fetched_response == b"":
                        raise InvalidResponse(
                            "Request hasn't processed from ServiceNow server"
                        )
                    elif (
                        not response.headers["Content-Type"]
                        .strip()
                        .startswith("application/json")
                    ):
                        raise InvalidResponse(
                            "Request retrieved with invalid content type to process further"
                        )

                    json_response = json.loads(fetched_response)
                    result = json_response["result"]

                    response_length = len(result)
                    if response_length == 0:
                        break

                    offset += response_length
                    retry = 1
                    yield result

            except Exception as exception:
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}."
                )
                await self.sleeps.sleep(RETRY_INTERVAL**retry)

                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.session.close()  # pyright: ignore
                    self.session = self._generate_session()

                if retry == self.retry_count:
                    raise exception
                retry += 1

    async def close(self):
        """Closes unclosed client session."""
        self.sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()

    async def ping(self):
        """Verify the connection with ServiceNow."""

        try:
            logger.info("Pinging ServiceNow instance")
            if self.session is None:
                self._generate_session()

            payload = {
                "sysparm_query": "label=Incident",
                "sysparm_fields": "label, name",
            }
            await anext(
                self._api_call(
                    url=ENDPOINTS["TABLE"].format(table="sys_db_object"), params=payload
                )
            )
            logger.debug("Successfully connected to the ServiceNow.")

        except Exception:
            logger.exception("Error while connecting to the ServiceNow.")
            raise

    async def get_docs(self, filtering=None):
        # yield dummy document to make the chunked PR work, subsequent PR will replace it with actual implementation
        yield {"_id": "123", "timestamp": iso_utc()}, None
