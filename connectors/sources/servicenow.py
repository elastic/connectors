#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""
import asyncio
import json
import os
from enum import Enum
from functools import cached_property, partial

import aiofiles
import aiohttp
import dateutil.parser as parser
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    convert_to_b64,
    iso_utc,
)

RETRIES = 3
RETRY_INTERVAL = 2
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
FILE_SIZE_LIMIT = 10485760  # Size in Bytes
FETCH_SIZE = 1000
MAX_CONCURRENCY_SUPPORT = 10

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

ORDER_BY_CREATION_DATE_QUERY = "ORDERBYsys_created_on^"
ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
    "ATTACHMENT": "/api/now/attachment",
    "DOWNLOAD": "/api/now/attachment/{sys_id}/file",
}
DEFAULT_SERVICE_NAMES = (
    "sys_user",
    "sc_req_item",
    "incident",
    "kb_knowledge",
    "change_request",
)


class END_SIGNAL(Enum):
    SERVICE = "SERVICE_TASK_FINISHED"
    RECORD = "RECORD_TASK_FINISHED"


class InvalidResponse(Exception):
    pass


class ServiceNowClient:
    """ServiceNow Client"""

    def __init__(self, configuration):
        """Setup the ServiceNow client.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.services = self.configuration["services"]
        self.retry_count = self.configuration["retry_count"]

    @cached_property
    def _get_session(self):
        """Generate aiohttp client session with configuration fields.

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """

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

        return aiohttp.ClientSession(
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
            url (str): ServiceNow url to be executed.
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
            query = ORDER_BY_CREATION_DATE_QUERY
            if "sysparm_query" in params.keys():
                query += params["sysparm_query"]
            params.update({"sysparm_query": query, "sysparm_limit": FETCH_SIZE})

        while True:
            if not is_attachment:
                params["sysparm_offset"] = offset

            try:
                async with self._get_session.get(  # pyright: ignore
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
                            "Request to ServiceNow server returned an empty response."
                        )
                    elif not response.headers["Content-Type"].startswith(
                        "application/json"
                    ):
                        raise InvalidResponse(
                            f"Cannot proceed due to unexpected response type '{response.headers['Content-Type']}'; response type must begin with 'application/json'."
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
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self._get_session.close()
                    del self._get_session

                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}."
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry)

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

    async def _fetch_data(self, url, params):
        """Hit API call and iterate over the fetched response.

        Args:
            url (str): ServiceNow query to be executed.
            params (dict): Params to format the query.

        Yields:
            dict: Formatted document.
        """
        async for response in self._api_call(url=url, params=params):
            for record in response:  # pyright: ignore
                yield record

    async def get_table_data(self, url):
        async for table_data in self._fetch_data(url=url, params={}):
            yield table_data

    async def get_attachment_data(self, url, params):
        async for attachment_data in self._fetch_data(url=url, params=params):
            yield attachment_data

    async def fetch_attachment_content(self, metadata, timestamp=None, doit=False):
        """Fetch attachment content via metadata.

        Args:
            metadata (dict): Attachment metadata.
            timestamp (timestamp, None): Attachment last modified timestamp. Defaults to None.
            doit (bool, False): Whether to get content or not. Defaults to False.

        Returns:
            dict: Document with id, timestamp & content.
        """

        attachment_size = int(metadata["size_bytes"])
        if not (doit and attachment_size > 0):
            return

        attachment_name = metadata["file_name"]
        attachment_extension = os.path.splitext(attachment_name)[-1]
        if attachment_extension == "":
            logger.warning(
                f"Files without extension are not supported by TIKA, skipping {attachment_name}."
            )
            return
        elif attachment_extension not in TIKA_SUPPORTED_FILETYPES:
            logger.warning(
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content."
            )
            return

        document = {"_id": metadata["id"], "_timestamp": metadata["_timestamp"]}

        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            temp_filename = str(async_buffer.name)

            try:
                async for response in self._api_call(
                    url=ENDPOINTS["DOWNLOAD"].format(sys_id=metadata["id"]),
                    params={},
                    is_attachment=True,
                ):
                    async for data, _ in response.content.iter_chunks():
                        await async_buffer.write(data)

            except Exception as exception:
                logger.warning(
                    f"Skipping content for {attachment_name}. Exception: {exception}."
                )
                return

        logger.debug(f"Calling convert_to_b64 for file : {attachment_name}.")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)

        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            document["_attachment"] = (await async_buffer.read()).strip()

        try:
            await remove(temp_filename)
        except Exception as exception:
            logger.warning(
                f"Error while deleting the file: {temp_filename} from disk. Error: {exception}"
            )

        return document

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
        await self._get_session.close()
        del self._get_session


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

        self.valid_services = []
        self.invalid_services = []

        self.task_count = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY_SUPPORT)

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
            ConfigurableFieldValueError: Unavailable services error.
        """

        if self.servicenow_client.services != ["*"] and self.invalid_services == []:
            (
                self.valid_services,
                self.invalid_services,
            ) = await self.servicenow_client.filter_services()
        if self.invalid_services:
            raise ConfigurableFieldValueError(
                f"Services '{', '.join(self.invalid_services)}' are not available. Available services are: '{', '.join(set(self.servicenow_client.services)-set(self.invalid_services))}'"
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

    def _format_doc(self, data):
        """Format document for handling empty values & type casting.

        Args:
            data (dict): Fetched record from ServiceNow.

        Returns:
            dict: Formatted document.
        """

        data = {key: value for key, value in data.items() if value}
        data.update(
            {
                "_id": data["sys_id"],
                "_timestamp": iso_utc(parser.parse(data["sys_updated_on"])),
            }
        )
        return self.serialize(doc=data)

    async def _attachment_fetcher(self, table_sys_id):
        """Method to add attachment metadata to queue

        Args:
            table_sys_id (str): Table Id for fetching attachment metadata.
        """
        try:
            async for attachment_metadata in self.servicenow_client.get_attachment_data(
                url=ENDPOINTS["ATTACHMENT"], params={"table_sys_id": table_sys_id}
            ):
                formatted_attachment_metadata = self._format_doc(
                    data=attachment_metadata
                )
                serialized_attachment_metadata = self.serialize(
                    doc=formatted_attachment_metadata
                )
                await self.queue.put(
                    (  # pyright: ignore
                        serialized_attachment_metadata,
                        partial(
                            self.servicenow_client.fetch_attachment_content,
                            serialized_attachment_metadata,
                        ),
                    )
                )
        except Exception as exception:
            logger.warning(
                f"Skipping attachment data for {table_sys_id}. Exception: {exception}."
            )

        await self.queue.put(END_SIGNAL.RECORD)  # pyright: ignore

    async def _producer(self, service_name):
        """Fetch data for configured service name.

        Args:
            service_name (str): Service Name for preparing URL.
        """

        logger.debug(f"Fetching {service_name} data")
        try:
            async for table_data in self.servicenow_client.get_table_data(
                url=ENDPOINTS["TABLE"].format(table=service_name)
            ):
                formatted_table_data = self._format_doc(data=table_data)
                serialized_table_data = self.serialize(doc=formatted_table_data)
                await self.fetchers.put(
                    partial(
                        self._attachment_fetcher,
                        serialized_table_data["_id"],
                    )
                )
                self.task_count += 1

                await self.queue.put((serialized_table_data, None))  # pyright: ignore
        except Exception as exception:
            logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}."
            )

        await self.queue.put(END_SIGNAL.SERVICE)  # pyright: ignore

    async def _consumer(self):
        """Consume the queue for the documents.

        Yields:
            dict: Formatted document.
        """

        while self.task_count > 0:
            _, item = await self.queue.get()

            if isinstance(item, END_SIGNAL):
                self.task_count -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Get documents from ServiceNow.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from ServiceNow.
        """

        logger.info("Fetching ServiceNow data")
        if self.servicenow_client.services != ["*"] and self.valid_services == []:
            (
                self.valid_services,
                self.invalid_services,
            ) = await self.servicenow_client.filter_services()
        for service_name in self.valid_services or DEFAULT_SERVICE_NAMES:
            await self.fetchers.put(partial(self._producer, service_name))
            self.task_count += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
