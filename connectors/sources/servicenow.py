#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""
import asyncio
import base64
import json
import os
import uuid
from enum import Enum
from functools import cached_property, partial
from urllib.parse import urlencode

import aiofiles
import aiohttp
import dateutil.parser as parser
import fastjsonschema
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    convert_to_b64,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
CHUNK_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
FILE_SIZE_LIMIT = 10485760  # Size in Bytes
CONCURRENT_TASKS = 1000  # Depends on total number of services and size of each service
MAX_CONCURRENT_CLIENT_SUPPORT = 10
TABLE_FETCH_SIZE = 50
TABLE_BATCH_SIZE = 5
ATTACHMENT_BATCH_SIZE = 10

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

ORDER_BY_CREATION_DATE_QUERY = "ORDERBYsys_created_on^"
ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
    "ATTACHMENT": "/api/now/attachment",
    "DOWNLOAD": "/api/now/attachment/{sys_id}/file",
    "BATCH": "/api/now/v1/batch",
}
DEFAULT_SERVICE_NAMES = (
    "sys_user",
    "sc_req_item",
    "incident",
    "kb_knowledge",
    "change_request",
)


class EndSignal(Enum):
    SERVICE = "SERVICE_TASK_FINISHED"
    RECORD = "RECORD_TASK_FINISHED"
    ATTACHMENT = "ATTACHMENT_TASK_FINISHED"


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
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_session(self):
        """Generate aiohttp client session with configuration fields.

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """

        self._logger.debug("Generating aiohttp client session")
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CLIENT_SUPPORT)
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

    async def _read_response(self, response):
        fetched_response = await response.read()
        if fetched_response == b"":
            raise InvalidResponse(
                "Request to ServiceNow server returned an empty response."
            )
        elif not response.headers["Content-Type"].startswith("application/json"):
            if response.headers.get("Connection") == "close":
                raise Exception("Couldn't connect to ServiceNow instance")
            raise InvalidResponse(
                f"Cannot proceed due to unexpected response type '{response.headers['Content-Type']}'; response type must begin with 'application/json'."
            )
        return fetched_response

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_table_length(self, table_name):
        try:
            url = ENDPOINTS["TABLE"].format(table=table_name)
            params = {"sysparm_limit": 1}
            response = await self._api_call(
                url=url, params=params, actions={}, method="get"
            )
            await self._read_response(response=response)
            return int(response.headers.get("x-total-count", 0))
        except Exception as exception:
            self._logger.warning(
                f"Error while fetching {table_name} length. Exception: {exception}."
            )
            raise

    def _prepare_url(self, url, params, offset):
        if not url.endswith("/file"):
            query = ORDER_BY_CREATION_DATE_QUERY
            if "sysparm_query" in params.keys():
                query += params["sysparm_query"]
            params.update(
                {
                    "sysparm_query": query,
                    "sysparm_limit": TABLE_FETCH_SIZE,
                    "sysparm_offset": offset,
                }
            )
        full_url = url
        if params:
            params_string = urlencode(params)
            full_url = f"{url}?{params_string}"
        return full_url

    def get_filter_apis(self, rules, mapping):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for rule in rules:
            params = {"sysparm_query": rule["query"]}
            table_name = mapping[rule["service"]]
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(
                        url=ENDPOINTS["TABLE"].format(table=table_name),
                        params=params.copy(),
                        offset=0,
                    ),
                }
            )
        return apis

    def get_record_apis(self, url, params, total_count):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for page in range(int(total_count / TABLE_FETCH_SIZE) + 1):
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(
                        url=url,
                        params=params.copy(),
                        offset=page * TABLE_FETCH_SIZE,
                    ),
                }
            )
        return apis

    def get_attachment_apis(self, url, ids):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for id_ in ids:
            params = {"table_sys_id": id_}
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(url=url, params=params.copy(), offset=0),
                }
            )
        return apis

    async def get_data(self, batched_apis):
        try:
            batch_data = self._prepare_batch(requests=batched_apis)
            async for response in self._batch_api_call(batch_data=batch_data):
                yield response
        except Exception as exception:
            self._logger.debug(
                f"Error while fetching batch: {batched_apis} data. Exception: {exception}."
            )
            raise

    def _prepare_batch(self, requests):
        return {"batch_request_id": str(uuid.uuid4()), "rest_requests": requests}

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _batch_api_call(self, batch_data):
        response = await self._api_call(
            url=ENDPOINTS["BATCH"], params={}, actions=batch_data, method="post"
        )
        json_response = json.loads(await self._read_response(response=response))

        for response in json_response["serviced_requests"]:
            if response["status_code"] != 200:
                error_message = json.loads(base64.b64decode(response["body"]))["error"]
                raise InvalidResponse(
                    f"Cannot proceed due to invalid status code {response['status_code']}; Message {error_message}."
                )
            yield json.loads(base64.b64decode(response["body"]))["result"]

    async def _api_call(self, url, params, actions, method):
        return await getattr(self._get_session, method)(
            url=url, params=params, json=actions
        )

    async def filter_services(self, configured_service):
        """Filter services based on service mappings.

        Args:
            configured_service (list): Services need to validate.

        Returns:
            dict, list: Servicenow mapping, Invalid services.
        """

        try:
            self._logger.debug("Filtering services")
            servicenow_mapping, invalid_services = {}, configured_service

            payload = {"sysparm_fields": "label, name"}
            table_length = await self.get_table_length(table_name="sys_db_object")
            record_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"),
                params=payload,
                total_count=table_length,
            )

            for batched_apis_index in range(0, len(record_apis), TABLE_BATCH_SIZE):
                batched_apis = record_apis[
                    batched_apis_index : (batched_apis_index + TABLE_BATCH_SIZE)  # noqa
                ]
                async for table_data in self.get_data(batched_apis=batched_apis):
                    for mapping in table_data:  # pyright: ignore
                        if mapping["label"] in invalid_services:
                            servicenow_mapping[mapping["label"]] = mapping["name"]
                            invalid_services.remove(mapping["label"])

            return servicenow_mapping, invalid_services

        except Exception as exception:
            self._logger.exception(
                f"Error while filtering services. Exception: {exception}."
            )
            raise

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
            self._logger.warning(
                f"Files without extension are not supported by TIKA, skipping {attachment_name}."
            )
            return
        elif attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content."
            )
            return

        document = {"_id": metadata["id"], "_timestamp": metadata["_timestamp"]}

        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            temp_filename = str(async_buffer.name)

            try:
                response = await self._api_call(
                    url=ENDPOINTS["DOWNLOAD"].format(sys_id=metadata["id"]),
                    params={},
                    actions={},
                    method="get",
                )
                async for data in response.content.iter_chunked(CHUNK_SIZE):
                    await async_buffer.write(data)

            except Exception as exception:
                self._logger.warning(
                    f"Skipping content for {attachment_name}. Exception: {exception}."
                )
                return

        self._logger.debug(f"Calling convert_to_b64 for file : {attachment_name}.")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)

        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            document["_attachment"] = (await async_buffer.read()).strip()

        try:
            await remove(temp_filename)
        except Exception as exception:
            self._logger.warning(
                f"Error while deleting the file: {temp_filename} from disk. Error: {exception}"
            )

        return document

    async def ping(self):
        await self.get_table_length(table_name="sys_db_object")

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session


class ServiceNowAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "service": {"type": "string", "minLength": 1},
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["service", "query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": RULES_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    async def _remote_validation(self, advanced_rules):
        try:
            ServiceNowAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        services_to_filter = set(rule["service"] for rule in advanced_rules)

        (
            _,
            invalid_services,
        ) = await self.source.servicenow_client.filter_services(
            configured_service=services_to_filter.copy()
        )

        if len(invalid_services) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Services '{', '.join(invalid_services)}' are not available. Available services are: '{', '.join(set(services_to_filter)-set(invalid_services))}'",
            )

        await self.source.servicenow_client.close_session()

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


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

        self.servicenow_mapping = {}
        self.invalid_services = []

        self.task_count = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=CONCURRENT_TASKS)

    def advanced_rules_validators(self):
        return [ServiceNowAdvancedRulesValidator(self)]

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
                "value": "",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "sensitive": True,
                "type": "str",
                "value": "",
            },
            "services": {
                "display": "textarea",
                "label": "Comma-separated list of services",
                "order": 4,
                "tooltip": "List of services is ignored when Advanced Sync Rules are used.",
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
                "default_value": MAX_CONCURRENT_CLIENT_SUPPORT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": MAX_CONCURRENT_CLIENT_SUPPORT,
            },
        }

    async def _remote_validation(self):
        """Validate configured services

        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """

        if self.servicenow_client.services != ["*"] and self.invalid_services == []:
            (
                self.servicenow_mapping,
                self.invalid_services,
            ) = await self.servicenow_client.filter_services(
                configured_service=self.servicenow_client.services.copy()
            )
        if self.invalid_services:
            raise ConfigurableFieldValueError(
                f"Services '{', '.join(self.invalid_services)}' are not available. Available services are: '{', '.join(set(self.servicenow_client.services)-set(self.invalid_services))}'"
            )

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured services are available in ServiceNow."""

        await super().validate_config()
        await self._remote_validation()

    async def close(self):
        await self.servicenow_client.close_session()

    async def ping(self):
        """Verify the connection with ServiceNow."""

        try:
            await self.servicenow_client.ping()
            self._logger.debug("Successfully connected to the ServiceNow.")

        except Exception:
            self._logger.exception("Error while connecting to the ServiceNow.")
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
        return data

    async def _fetch_attachment_metadata(self, batched_apis):
        try:
            async for attachments_metadata in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                for record in attachments_metadata:
                    formatted_attachment_metadata = self._format_doc(data=record)
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
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )

        await self.queue.put(EndSignal.ATTACHMENT)  # pyright: ignore

    async def _attachment_metadata_producer(self, record_ids):
        attachment_apis = self.servicenow_client.get_attachment_apis(
            url=ENDPOINTS["ATTACHMENT"], ids=record_ids
        )

        for batched_apis_index in range(0, len(attachment_apis), ATTACHMENT_BATCH_SIZE):
            batched_apis = attachment_apis[
                batched_apis_index : (  # noqa
                    batched_apis_index + ATTACHMENT_BATCH_SIZE
                )
            ]
            await self.fetchers.put(
                partial(self._fetch_attachment_metadata, batched_apis)
            )
            self.task_count += 1

        await self.queue.put(EndSignal.RECORD)  # pyright: ignore

    async def _fetch_table_data(self, batched_apis):
        try:
            async for table_data in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                record_ids = []
                for record in table_data:
                    formatted_table_data = self._format_doc(data=record)
                    serialized_table_data = self.serialize(doc=formatted_table_data)
                    record_ids.append(serialized_table_data["_id"])
                    await self.queue.put(
                        (serialized_table_data, None)  # pyright: ignore
                    )
                await self.fetchers.put(
                    partial(
                        self._attachment_metadata_producer,
                        record_ids,
                    )
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )

        await self.queue.put(EndSignal.RECORD)  # pyright: ignore

    async def _table_data_producer(self, service_name):
        self._logger.debug(f"Fetching {service_name} data")
        try:
            table_length = await self.servicenow_client.get_table_length(
                table_name=service_name
            )
            record_apis = self.servicenow_client.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table=service_name),
                params={},
                total_count=table_length,
            )

            for batched_apis_index in range(0, len(record_apis), TABLE_BATCH_SIZE):
                batched_apis = record_apis[
                    batched_apis_index : (batched_apis_index + TABLE_BATCH_SIZE)  # noqa
                ]
                await self.fetchers.put(partial(self._fetch_table_data, batched_apis))
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}."
            )

        await self.queue.put(EndSignal.SERVICE)  # pyright: ignore

    async def _consumer(self):
        """Consume the queue for the documents.

        Yields:
            dict: Formatted document.
        """

        while self.task_count > 0:
            _, item = await self.queue.get()

            if isinstance(item, EndSignal):
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

        self._logger.info("Fetching ServiceNow data")
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            services = set(rule["service"] for rule in advanced_rules)

            (
                servicenow_mapping,
                _,
            ) = await self.servicenow_client.filter_services(
                configured_service=services.copy()
            )

            for advanced_rules_index in range(0, len(advanced_rules), TABLE_BATCH_SIZE):
                batched_advanced_rules = advanced_rules[
                    advanced_rules_index : (
                        advanced_rules_index + TABLE_BATCH_SIZE
                    )  # noqa
                ]
                filter_apis = self.servicenow_client.get_filter_apis(
                    rules=batched_advanced_rules, mapping=servicenow_mapping
                )

                await self.fetchers.put(partial(self._fetch_table_data, filter_apis))
                self.task_count += 1

        else:
            if (
                self.servicenow_client.services != ["*"]
                and self.servicenow_mapping == {}
            ):
                (
                    self.servicenow_mapping,
                    self.invalid_services,
                ) = await self.servicenow_client.filter_services(
                    configured_service=self.servicenow_client.services.copy()
                )
            for service_name in (
                self.servicenow_mapping.values() or DEFAULT_SERVICE_NAMES
            ):
                await self.fetchers.put(
                    partial(self._table_data_producer, service_name)
                )
                self.task_count += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
