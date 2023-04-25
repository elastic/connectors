#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""
import json
import os

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import CancellableSleeps, iso_utc

RETRIES = 3
RETRY_INTERVAL = 2
FETCH_SIZE = 1000
MAX_CONCURRENCY_SUPPORT = 10

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
    "ATTACHMENT": "/api/now/attachment",
    "DOWNLOAD": "/api/now/attachment/{sys_id}/file",
}


class InvalidResponse(Exception):
    """Raise when retrieving invalid response..

    Args:
        Exception (InvalidResponse): Invalid response exception.
    """

    pass


class ServiceNowClient(BaseDataSource):
    """ServiceNow Client"""

    def __init__(self, configuration):
        """Setup the ServiceNow client.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        self.session = None
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.services = self.configuration["services"]
        self.retry_count = self.configuration["retry_count"]

    def _get_session(self):
        """Generate aiohttp client session with configuration fields.

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """

        if self.session:
            return self.session

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
        return self.session

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
                async with self._get_session().get(  # pyright: ignore
                    url=url, params=params
                ) as response:
                    if (
                        not RUNNING_FTEST
                        and response.headers.get("Connection") == "close"
                    ):
                        raise Exception("Couldn't connect to ServiceNow instance")

                    if is_attachment:
                        yield response
                        break

                    fetched_response = await response.read()
                    if fetched_response == b"":
                        raise InvalidResponse(
                            "Request hasn't processed from ServiceNow server"
                        )
                    elif not response.headers["Content-Type"].startswith(
                        "application/json"
                    ):
                        raise InvalidResponse(
                            f"Cannot proceed due to unexpected response type '{response.headers["Content-Type"]}'; response type must begin with 'application/json'."
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
                    f"Retry attempt {retry} out of {self.retry_count} failed. Exception: {exception}."
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry)

                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.session.close()  # pyright: ignore

                if retry == self.retry_count:
                    raise exception
                retry += 1

    async def filter_services(self):
        """Filter services based on service mappings.

        Returns:
            list, list: Valid service names, Invalid services.
        """

        try:
            logger.debug("Filtering services")
            service_names, invalid_services = [], self.services.copy()

            payload = {"sysparm_fields": "label, name"}
            async for response in self._api_call(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"), params=payload
            ):
                for mapping in response:  # pyright: ignore
                    if mapping["label"] in invalid_services:
                        service_names.append(mapping["name"])
                        invalid_services.remove(mapping["label"])

            return service_names, invalid_services

        except Exception as exception:
            logger.exception(f"Error while filtering services. Exception: {exception}.")
            raise

    async def ping(self):

        payload = {
            "sysparm_query": "label=Incident",
            "sysparm_fields": "label, name",
        }
        await anext(
            self._api_call(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"), params=payload
            )
        )

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        self.session = None


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
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.servicenow_client = ServiceNowClient(configuration=configuration)

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by ServiceNow

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    @classmethod
    def get_default_configuration(cls):
        return {
            "url": {
                "label": "Service URL",
                "order": 1,
                "type": "str",
                "value": "http://127.0.0.1:9318",
            },
            "username": {
                "label": "Username",
                "order": 2,
                "type": "str",
                "value": "admin",
            },
            "password": {
                "label": "Password",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "changeme",
            },
            "services": {
                "display": "textarea",
                "label": "Comma-separated list of services",
                "order": 4,
                "type": "list",
                "value": "*",
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENCY_SUPPORT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": MAX_CONCURRENCY_SUPPORT,
            },
        }

    async def _remote_validation(self):
        """Validate configured services

        Raises:
            ConfigurableFieldValueError: Unavaliable services error.
        """

        if self.servicenow_client.services != ["*"]:
            (
                service_names,
                invalid_services,
            ) = await self.servicenow_client.filter_services()
            if invalid_services:
                raise ConfigurableFieldValueError(
                    f"Services '{', '.join(invalid_services)}' are not available. Available services are: '{', '.join(service_names)}'"
                )

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured services are available in ServiceNow."""

        self.configuration.check_valid()
        await self._remote_validation()

    async def close(self):

        await self.servicenow_client.close_session()

    async def ping(self):
        """Verify the connection with ServiceNow."""

        try:
            await self.servicenow_client.ping()
            logger.debug("Successfully connected to the ServiceNow.")

        except Exception:
            logger.exception("Error while connecting to the ServiceNow.")
            raise

    async def get_docs(self, filtering=None):
        # yield dummy document to make the chunked PR work, subsequent PR will replace it with actual implementation
        yield {"_id": "123", "timestamp": iso_utc()}, None
