#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
import json
import math
import uuid
from functools import cached_property
from urllib.parse import urlencode

import aiohttp

from connectors.utils import CancellableSleeps, retryable, RetryStrategy
from connectors_sdk.logger import logger


MAX_CONCURRENT_CLIENT_SUPPORT = 10
TABLE_FETCH_SIZE = 50
TABLE_BATCH_SIZE = 5
RETRIES = 3
RETRY_INTERVAL = 2
ORDER_BY_CREATION_DATE_QUERY = "ORDERBYsys_created_on^"
ENDPOINTS = {
    "TABLE": "/api/now/table/{table}",
    "ATTACHMENT": "/api/now/attachment",
    "DOWNLOAD": "/api/now/attachment/{sys_id}/file",
    "BATCH": "/api/now/v1/batch",
}


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
            msg = "Request to ServiceNow server returned an empty response."
            raise InvalidResponse(msg)
        elif not response.headers["Content-Type"].startswith("application/json"):
            if response.headers.get("Connection") == "close":
                msg = "Couldn't connect to ServiceNow instance"
                raise Exception(msg)
            msg = f"Cannot proceed due to unexpected response type '{response.headers['Content-Type']}'; response type must begin with 'application/json'."
            raise InvalidResponse(msg)
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

    async def get_filter_apis(self, rules, mapping):
        apis = []
        for rule in rules:
            params = {"sysparm_query": rule["query"]}
            table_name = mapping[rule["service"]]
            total_count = await self.get_table_length(table_name)
            paginated_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table=table_name),
                params=params,
                total_count=total_count,
            )
            apis.extend(paginated_apis)
        return apis

    def get_record_apis(self, url, params, total_count):
        headers = [
            {"name": "Content-Type", "value": "application/json"},
            {"name": "Accept", "value": "application/json"},
        ]
        apis = []
        for page in range(math.ceil(total_count / TABLE_FETCH_SIZE)):
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
            table_length = await self.get_table_length(table_name="sys_db_object")
            record_apis = self.get_record_apis(
                url=ENDPOINTS["TABLE"].format(table="sys_db_object"),
                params=payload,
                total_count=table_length,
            )

            for batched_apis_index in range(0, len(record_apis), TABLE_BATCH_SIZE):
                batched_apis = record_apis[
                    batched_apis_index : (
                        batched_apis_index + TABLE_BATCH_SIZE
                    )  # noqa
                ]
                async for table_data in self.get_data(batched_apis=batched_apis):
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
        await self.get_table_length(table_name="sys_db_object")

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session
