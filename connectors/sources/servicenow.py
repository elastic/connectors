#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""

import base64
import json
import math
import os
import uuid
from enum import Enum
from functools import cached_property, partial
from urllib.parse import urlencode

import aiohttp
import dateutil.parser as parser
import fastjsonschema

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    iso_utc,
    retryable,
)

RETRIES = 2
RETRY_INTERVAL = 2
QUEUE_MEM_SIZE = 1024 * 1024 * 1024 # 1GB - Optimized for HUGE records
CONCURRENT_TASKS = 200              # Reduced from 1000 for better resource management
MAX_CONCURRENT_CLIENT_SUPPORT = 10  # Increase for better attachment download performance
SYS_DB_OBJECT_FETCH_SIZE = 500      # Agressive fetch size for sys_db_object table
SYS_DB_OBJECT_BATCH_SIZE = 10       # Batch size for sys_db_object table
TABLE_FETCH_SIZE = 100               # Number of records to grab per API call
TABLE_BATCH_SIZE = 1                # Batch size for all other tables
ATTACHMENT_BATCH_SIZE = 1          # Optimized for 10K+ attachments

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
DEFAULT_SERVICE_NAMES = {
    "sys_user": ["admin"],
    "sc_req_item": [
        "admin",
        "sn_request_read",
        "asset",
        "atf_test_designer",
        "atf_test_admin",
    ],
    "incident": ["admin", "sn_incident_read", "ml_report_user", "ml_admin", "itil"],
    "kb_knowledge": ["admin", "knowledge", "knowledge_manager", "knowledge_admin"],
    "change_request": ["admin", "sn_change_read", "itil"],
}
ACLS_QUERY = "sys_security_acl.operation=read^sys_security_acl.name={table_name}"


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


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
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CLIENT_SUPPORT, ssl=False)
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
        """Read and validate response from ServiceNow API.

        Args:
            response: aiohttp response object

        Returns:
            bytes: Valid response content

        Raises:
            InvalidResponse: If response is invalid or malformed
        """
        try:
            fetched_response = await response.read()
        except Exception as e:
            msg = f"Failed to read response from ServiceNow: {e}"
            raise InvalidResponse(msg)

        if fetched_response == b"":
            msg = "Request to ServiceNow server returned an empty response."
            raise InvalidResponse(msg)

        # Check for extremely large responses that might be truncated
        if len(fetched_response) > 10 * 1024 * 1024:  # 100MB limit
            msg = f"Response too large ({len(fetched_response)} bytes), may be truncated"
            self._logger.warning(msg)

        content_type = response.headers.get("Content-Type", "")
        if not content_type.startswith("application/json"):
            if response.headers.get("Connection") == "close":
                msg = "Couldn't connect to ServiceNow instance"
                raise Exception(msg)
            msg = f"Cannot proceed due to unexpected response type '{content_type}'; response type must begin with 'application/json'."
            raise InvalidResponse(msg)

        return fetched_response

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_table_length(self, table_name, custom_filter=None):
        """Get the total count of records in a table.
        
        Args:
            table_name (str): Name of the ServiceNow table
            custom_filter (str, optional): Custom query filter to apply. Defaults to None.
        
        Returns:
            int: Total count of records matching the criteria
        """
        try:
            url = ENDPOINTS["TABLE"].format(table=table_name)
            params = {"sysparm_limit": 1}
            
            # Add custom filter if provided
            if custom_filter:
                params["sysparm_query"] = custom_filter
                
            response = await self._api_call(
                url=url, params=params, actions={}, method="get"
            )
            await self._read_response(response=response)
            return int(response.headers.get("x-total-count", 0))
        except Exception as exception:
            filter_info = f" with filter '{custom_filter}'" if custom_filter else ""
            self._logger.warning(
                f"Error while fetching {table_name} length{filter_info}. Exception: {exception}."
            )
            raise

    def _prepare_url(self, url, params, offset, table_name=None):
        if not url.endswith("/file"):
            query = ORDER_BY_CREATION_DATE_QUERY
            if "sysparm_query" in params.keys():
                query += params["sysparm_query"]
            
            # Use appropriate fetch size based on table type
            fetch_size = SYS_DB_OBJECT_FETCH_SIZE if table_name == "sys_db_object" else TABLE_FETCH_SIZE
            
            params.update(
                {
                    "sysparm_query": query,
                    "sysparm_limit": fetch_size,
                    "sysparm_offset": offset,
                }
            )
        full_url = url
        if params:
            params_string = urlencode(params)
            full_url = f"{url}?{params_string}"
        return full_url

    async def get_filter_apis(self, rules, mapping):
        all_apis = []

        # First, generate all individual API calls for all rules
        for rule in rules:
            params = {"sysparm_query": rule["query"]}
            table_name = mapping[rule["service"]]
            # Use the updated get_table_length with custom filter
            total_count = await self.get_table_length(table_name, rule["query"])
            paginated_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table=table_name),
                params=params,
                total_count=total_count,
                table_name=table_name,
            )
            all_apis.extend(paginated_apis)

        self._logger.debug(f"get_record_apis() generated {len(all_apis)} total paginated API calls")

        # Now batch the API calls based on TABLE_BATCH_SIZE
        for batched_apis_index in range(0, len(all_apis), TABLE_BATCH_SIZE):
            batched_apis = all_apis[
                           batched_apis_index: (
                                   batched_apis_index + TABLE_BATCH_SIZE
                           )  # noqa
                           ]
            self._logger.debug(
                f"Created advanced rules batch with {len(batched_apis)} API calls (target: {TABLE_BATCH_SIZE})")
            yield batched_apis

    # Uses TABLE_FETCH_SIZE or SYS_DB_OBJECT_FETCH_SIZE to create API calls with rows limited
    # by sysparm_limit and offset by sysparm_offset
    def get_record_apis(self, url, params, total_count, table_name=None):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        
        # Use appropriate fetch size based on table type
        fetch_size = SYS_DB_OBJECT_FETCH_SIZE if table_name == "sys_db_object" else TABLE_FETCH_SIZE
        
        apis = []
        for page in range(math.ceil(total_count / fetch_size)):
            apis.append(
                {
                    "id": str(uuid.uuid4()),
                    "headers": headers,
                    "method": "GET",
                    "url": self._prepare_url(
                        url=url,
                        params=params.copy(),
                        offset=page * fetch_size,
                        table_name=table_name,
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
                    "url": self._prepare_url(url=url, params=params.copy(), offset=0, table_name="attachment"),
                }
            )
        return apis

    async def get_data(self, batched_apis, skip_debug_logging=False):
        # Only log detailed debug info if not skipping (i.e., not for sys_db_object)
        if not skip_debug_logging:
            self._logger.debug(f"get_data(). Number of API calls in this batch: {len(batched_apis)} Batch API List: {batched_apis}")
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
        """Execute batch API call with comprehensive error handling for JSON responses.
        
        Args:
            batch_data (dict): Batch request data
            
        Yields:
            dict: Parsed response data from each request in the batch
            
        Raises:
            InvalidResponse: If response is malformed or contains errors
        """
        response = await self._api_call(
            url=ENDPOINTS["BATCH"], params={}, actions=batch_data, method="post"
        )
        json_response = json.loads(await self._read_response(response=response))

        for response in json_response["serviced_requests"]:
            if response["status_code"] != 200:
                error_message = json.loads(base64.b64decode(response["body"]))["error"]
                msg = f"Cannot proceed due to invalid status code {response['status_code']}; Message {error_message}."
                raise InvalidResponse(msg)
            yield json.loads(base64.b64decode(response["body"]))["result"]

    async def _api_call(self, url, params, actions, method):
        return await getattr(self._get_session, method)(
            url=url, params=params, json=actions
        )

    async def download_func(self, url):
        response = await self._api_call(url, {}, {}, "get")
        yield response

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

            payload = {"sysparm_fields": "sys_id, label, name"}
            # Always use get_table_length without custom filter for sys_db_object
            table_length = await self.get_table_length(table_name="sys_db_object")
            record_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"),
                params=payload,
                total_count=table_length,
                table_name="sys_db_object",
            )

            batch_size = SYS_DB_OBJECT_BATCH_SIZE
            for batched_apis_index in range(0, len(record_apis), batch_size):
                batched_apis = record_apis[
                    batched_apis_index : (
                        batched_apis_index + batch_size
                    )  # noqa
                ]
                # Skip debug logging for sys_db_object operations
                async for table_data in self.get_data(batched_apis=batched_apis, skip_debug_logging=True):
                    for mapping in table_data:  # pyright: ignore
                        sys_id = mapping.get("sys_id")
                        name = mapping.get("name")
                        if not name:
                            self._log_missing_sysparm_field(sys_id, "name")
                            continue

                        label = mapping.get("label")
                        if not label:
                            self._log_missing_sysparm_field(sys_id, "label")
                            continue

                        if label in invalid_services:
                            servicenow_mapping[label] = name
                            invalid_services.remove(label)

            return servicenow_mapping, invalid_services

        except Exception as exception:
            self._logger.exception(
                f"Error while filtering services. Exception: {exception}."
            )
            raise

    def _log_missing_sysparm_field(self, sys_id, field):
        msg = f"Entry in sys_db_object with sys_id '{sys_id}' is missing sysparm_field '{field}'. This is a non-issue if no invalid services are flagged."
        self._logger.debug(msg)

    async def ping(self):
        # Always use get_table_length without custom filter for sys_db_object
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

        services_to_filter = {rule["service"] for rule in advanced_rules}

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
                validation_message=f"Services '{', '.join(invalid_services)}' are not available. Available services are: '{', '.join(set(services_to_filter) - set(invalid_services))}'",
            )

        await self.source.servicenow_client.close_session()

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class ServiceNowDataSource(BaseDataSource):
    """ServiceNow"""

    name = "ServiceNow"
    service_type = "servicenow"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

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
            },
            "username": {
                "label": "Username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "sensitive": True,
                "type": "str",
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
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_CLIENT_SUPPORT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 8,
                "tooltip": "Document level security ensures identities and permissions set in ServiceNow are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    async def _user_access_control_doc(self, user):
        user_id = user.get("_id", "")
        user_name = user.get("user_name", "")
        user_email = user.get("email", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_user_name = _prefix_username(user=user_name)
        _prefixed_email = _prefix_email(email=user_email)
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "display_name": _prefixed_user_name,
                "email": _prefixed_email,
            },
            "created_at": user.get("_timestamp"),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_user_name, _prefixed_email]
        )

    async def _fetch_all_users(self):
        self._logger.debug("Fetching all users.")
        async for user in self._table_data_generator(
            service_name="sys_user", params={}
        ):
            yield user

    async def _fetch_users_by_roles(self, role):
        self._logger.debug(f"Fetching users with role: {role}.")
        role_user_params = {"sysparm_query": f"role={role}"}
        async for user in self._table_data_generator(
            service_name="sys_user_has_role", params=role_user_params
        ):
            yield user

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for user in self._fetch_all_users():
            yield await self._user_access_control_doc(user=user)

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

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
            msg = f"Services '{', '.join(self.invalid_services)}' are not available. Available services are: '{', '.join(set(self.servicenow_client.services) - set(self.invalid_services))}'"
            raise ConfigurableFieldValueError(msg)

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

    async def _fetch_attachment_metadata(self, batched_apis, table_access_control):
        try:
            async for attachments_metadata in self.servicenow_client.get_data(
                batched_apis=batched_apis, skip_debug_logging=True
            ):
                for record in attachments_metadata:
                    formatted_attachment_metadata = self._format_doc(data=record)
                    serialized_attachment_metadata = self.serialize(
                        doc=formatted_attachment_metadata
                    )
                    attachment_with_access_control = self._decorate_with_access_control(
                        document=serialized_attachment_metadata,
                        access_control=table_access_control,
                    )
                    await self.queue.put(
                        (
                            attachment_with_access_control,
                            partial(
                                self.get_content,
                                attachment_with_access_control,
                            ),
                        )
                    )
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.ATTACHMENT)

    async def _attachment_metadata_producer(self, record_ids, table_access_control):
        attachment_apis = None
        try:
            attachment_apis = self.servicenow_client.get_attachment_apis(
                url=ENDPOINTS["ATTACHMENT"], ids=record_ids
            )

            for batched_apis_index in range(
                0, len(attachment_apis), ATTACHMENT_BATCH_SIZE
            ):
                batched_apis = attachment_apis[
                    batched_apis_index :   (  # noqa
                        batched_apis_index + ATTACHMENT_BATCH_SIZE
                    )
                ]
                await self.fetchers.put(
                    partial(
                        self._fetch_attachment_metadata,
                        batched_apis,
                        table_access_control,
                    )
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.exception(
                f"Skipping attachment metadata for {attachment_apis}. Exception: {exception}."
            )
            raise
        finally:
            await self.queue.put(EndSignal.RECORD)

    async def _yield_table_data(self, batched_apis):
        self._logger.debug(f"Yielding table data.  Number of API calls in this batch: {len(batched_apis)}")
        try:
            record_count = 0
            async for table_data in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                if not isinstance(table_data, list):
                    self._logger.warning(f"Expected list of records, got {type(table_data)}")
                    continue
                    
                for record in table_data:
                    if not isinstance(record, dict):
                        self._logger.warning(f"Skipping invalid record: not a dict ({type(record)})")
                        continue
                        
                    try:
                        formatted_table_data = self._format_doc(data=record)
                        serialized_table_data = self.serialize(doc=formatted_table_data)
                        record_count += 1
                        yield serialized_table_data
                    except Exception as format_error:
                        self._logger.warning(
                            f"Failed to format/serialize record {record.get('sys_id', 'unknown')}: {format_error}"
                        )
                        continue
                        
            self._logger.debug(f"Successfully processed {record_count} records from batch")
            
        except Exception as exception:
            self._logger.warning(
                f"Error processing batch data: {exception}",
                exc_info=True,
            )
            # Don't re-raise - let the caller handle the empty generator

    async def _fetch_table_data(self, batched_apis, table_access_control):
        try:
            async for table_data in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                record_ids = []
                for record in table_data:
                    formatted_table_data = self._format_doc(data=record)
                    serialized_table_data = self.serialize(doc=formatted_table_data)
                    record_ids.append(serialized_table_data["_id"])
                    table_data_with_access_control = self._decorate_with_access_control(
                        document=serialized_table_data,
                        access_control=table_access_control,
                    )
                    await self.queue.put(
                        (
                            table_data_with_access_control,
                            None,
                        )
                    )
                await self.fetchers.put(
                    partial(
                        self._attachment_metadata_producer,
                        record_ids,
                        table_access_control,
                    )
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.RECORD)

    async def _fetch_access_controls(self, table_name):
        access_control, user_roles, roles = [], [], {}
        if table_name in DEFAULT_SERVICE_NAMES.keys():
            async for role in self._table_data_generator(
                service_name="sys_user_role", params={}
            ):
                roles[role.get("name")] = role.get("sys_id")

            for role in DEFAULT_SERVICE_NAMES.get(table_name, []):
                async for user in self._fetch_users_by_roles(roles[role]):
                    access_control.append(
                        _prefix_user_id(user_id=user.get("user", {}).get("value"))
                    )
        else:
            async for role in self._table_data_generator(
                service_name="sys_user_role", params={}
            ):
                roles[role.get("sys_id")] = role.get("name")

            self._logger.info(f"Fetching roles of {table_name} with read operation.")
            acl_params = {
                "sys_security_acl.operation": "read",
                "sys_security_acl.name": table_name,
                "sys_security_acl.script": "",
                "sys_security_acl.condition": "",
            }
            async for acl in self._table_data_generator(
                service_name="sys_security_acl_role", params=acl_params
            ):
                user_roles.append(acl.get("sys_user_role", {}).get("value"))

            for role in user_roles:
                if roles.get(role).lower() == "public":
                    self._logger.info(
                        f"Found public role in {table_name}, Fetching all users."
                    )
                    async for user in self._fetch_all_users():
                        access_control.append(
                            _prefix_user_id(user_id=user.get("sys_id"))
                        )

                async for user in self._fetch_users_by_roles(role):
                    access_control.append(
                        _prefix_user_id(user_id=user.get("user", {}).get("value"))
                    )
        return list(set(access_control))

    # uses TABLE_BATCH_SIZE or SYS_DB_OBJECT_BATCH_SIZE to create batches of API calls
    async def _get_batched_apis(self, service_name, params):
        table_length = await self.servicenow_client.get_table_length(table_name=service_name)
        record_apis = self.servicenow_client.get_record_apis(
            url=ENDPOINTS["TABLE"].format(table=service_name),
            params=params,
            total_count=table_length,
            table_name=service_name,
        )

        # Use appropriate batch size based on table type
        batch_size = SYS_DB_OBJECT_BATCH_SIZE if service_name == "sys_db_object" else TABLE_BATCH_SIZE
        
        for batched_apis_index in range(0, len(record_apis), batch_size):
            batched_apis = record_apis[
                batched_apis_index : (
                    batched_apis_index + batch_size
                )  # noqa
            ]

            self._logger.debug(f"Creating SNOW batch api call. \n"
                               f"Service Name: {service_name}\n"
                               f"Total Records: {table_length}\n"
                               f"Batch Target Size: {batch_size}\n"
                               f"Number APIs: {len(record_apis)}\n"
                               f"Number APIs in batch: {len(batched_apis)}\n")

            yield batched_apis

    async def _table_data_generator(self, service_name, params):
        self._logger.debug(f"Fetching {service_name} data")
        try:
            async for batched_apis in self._get_batched_apis(service_name, params):
                async for user in self._yield_table_data(batched_apis=batched_apis):
                    yield user
        except Exception as exception:
            self._logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}.",
                exc_info=True,
            )

    async def _table_data_producer(self, service_name, params, table_access_control):
        self._logger.debug(f"Fetching {service_name} data")
        try:
            async for batched_apis in self._get_batched_apis(service_name, params):
                await self.fetchers.put(
                    partial(self._fetch_table_data, batched_apis, table_access_control)
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.SERVICE)

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
            services = {rule["service"] for rule in advanced_rules}

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
                async for filter_apis_batch in self.servicenow_client.get_filter_apis(
                    rules=batched_advanced_rules, mapping=servicenow_mapping
                ):
                    await self.fetchers.put(
                        partial(self._fetch_table_data, filter_apis_batch, [])
                    )
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
                self.servicenow_mapping.values() or DEFAULT_SERVICE_NAMES.keys()
            ):
                table_access_control = []
                if self._dls_enabled():
                    table_access_control = await self._fetch_access_controls(
                        table_name=service_name
                    )
                await self.fetchers.put(
                    partial(
                        self._table_data_producer,
                        service_name,
                        {},
                        table_access_control,
                    )
                )
                self.task_count += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()

    async def get_content(self, metadata, timestamp=None, doit=False):
        file_size = int(metadata["size_bytes"])
        if not (doit and file_size > 0):
            return

        filename = metadata["file_name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {"_id": metadata["id"], "_timestamp": metadata["_timestamp"]}
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.servicenow_client.download_func,
                    ENDPOINTS["DOWNLOAD"].format(sys_id=metadata["id"]),
                ),
            ),
        )
            
