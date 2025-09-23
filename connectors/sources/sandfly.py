#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Sandfly Security source module to fetch documents from a Sandfly Security Server.
"""

import json
import socket
from _asyncio import Task
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union

# import aiofiles
import aiohttp
from aiohttp.client import ClientSession
from aiohttp.client_exceptions import (
    ClientResponseError,
)

from connectors.es.sink import OP_INDEX
from connectors.logger import ExtraLogger, logger
from connectors.source import (
    CURSOR_SYNC_TIMESTAMP,
    BaseDataSource,
    DataSourceConfiguration,
)
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    hash_id,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2

RESULTS_SIZE = 999
CURSOR_SEQUENCE_ID_KEY = "sequence_id"


def extract_sandfly_date(datestr: str) -> datetime:
    return datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%SZ")


def format_sandfly_date(date: datetime, flag: bool) -> str:
    if flag:
        return date.strftime("%Y-%m-%dT00:00:00Z")  # date with time as midnight
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


class FetchTokenError(Exception):
    pass


class ResourceNotFound(Exception):
    pass


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass


class SandflyLicenseExpired(Exception):
    pass


class SandflyNotLicensed(Exception):
    pass


class SandflyAccessToken:
    def __init__(
        self,
        http_session: ClientSession,
        configuration: Union[Dict[str, Union[bool, str, int]], DataSourceConfiguration],
        logger_: ExtraLogger,
    ) -> None:
        self._token_cache = CacheWithTimeout()
        self._http_session = http_session
        self._logger = logger_

        self.server_url = configuration["server_url"]
        self.username = configuration["username"]
        self.password = configuration["password"]

    def set_logger(self, logger_: ExtraLogger) -> None:
        self._logger = logger_

    async def get(self, is_cache: bool = True) -> Generator[Task, None, str]:
        cached_value = self._token_cache.get_value() if is_cache else None

        if cached_value:
            return cached_value

        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token()
        self._token_cache.set_value(access_token, now + timedelta(seconds=expires_in))

        return access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _fetch_token(self) -> Tuple[str, int]:
        url = f"{self.server_url}/auth/login"
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        data = {
            "username": self.username,
            "password": self.password,
        }

        try:
            async with self._http_session.post(
                url=url, headers=request_headers, data=json.dumps(data)
            ) as response:
                json_response = await response.json()
                return json_response.get("access_token"), 3599
        except Exception as exception:
            msg = f"Error while generating access token. Exception {exception}."
            raise FetchTokenError(msg) from exception


class SandflySession:
    def __init__(
        self,
        http_session: ClientSession,
        token: SandflyAccessToken,
        logger_: ExtraLogger,
    ) -> None:
        self._sleeps = CancellableSleeps()
        self._logger = logger_

        self._http_session = http_session
        self._token = token

    def set_logger(self, logger_: ExtraLogger) -> None:
        self._logger = logger_

    def close(self) -> None:
        self._sleeps.cancel()

    async def ping(self, server_url: str) -> bool:
        try:
            await self._http_session.head(server_url)
            return True
        except ClientResponseError as exception:
            # The 401 Unauthorized means the Sandfly Server is up and running, so return True
            if exception.status == 401:
                return True
            else:
                raise
        except Exception:
            raise

    @asynccontextmanager
    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[ResourceNotFound, FetchTokenError],
    )
    async def _get(self, absolute_url: str) -> Iterator[Task]:
        try:
            access_token = await self._token.get()
            headers = {
                "Accept": "application/json",
                "Content-type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

            async with self._http_session.get(
                url=absolute_url, headers=headers
            ) as response:
                yield response
        except ClientResponseError as exception:
            if exception.status == 401:
                await self._token.get(is_cache=False)
                raise
            elif exception.status == 404:
                msg = "Resource Not Found"
                raise ResourceNotFound(msg) from exception
            else:
                raise
        except FetchTokenError:
            raise
        except Exception:
            raise

    @asynccontextmanager
    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[ResourceNotFound, FetchTokenError],
    )
    async def _post(
        self,
        absolute_url: str,
        payload: Dict[
            str, Union[int, str, Dict[str, List[Dict[str, str]]], List[Dict[str, str]]]
        ],
    ) -> Iterator[Task]:
        try:
            access_token = await self._token.get()
            headers = {
                "Accept": "application/json",
                "Content-type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

            async with self._http_session.post(
                url=absolute_url, headers=headers, data=json.dumps(payload)
            ) as response:
                yield response
        except ClientResponseError as exception:
            if exception.status == 401:
                await self._token.get(is_cache=False)
                raise
            elif exception.status == 404:
                msg = "Resource Not Found"
                raise ResourceNotFound(msg) from exception
            else:
                raise
        except FetchTokenError:
            raise
        except Exception:
            raise

    async def content_get(self, url: str) -> Generator[Task, None, str]:
        try:
            async with self._get(absolute_url=url) as response:
                return await response.text()
        except Exception as exception:
            self._logger.warning(
                f"Content-GET for {url} is being skipped. Error: {exception}."
            )
            raise

    async def content_post(
        self,
        url: str,
        payload: Dict[
            str,
            Union[
                int,
                Dict[str, List[Dict[str, str]]],
                List[Dict[str, str]],
                Dict[str, Union[str, List[Dict[str, str]]]],
                str,
            ],
        ],
    ) -> Generator[Task, None, str]:
        try:
            async with self._post(absolute_url=url, payload=payload) as response:
                return await response.text()
        except Exception as exception:
            self._logger.warning(
                f"Content-POST for {url} is being skipped. Error: {exception}."
            )
            raise


class SandflyClient:
    def __init__(
        self,
        configuration: Union[Dict[str, Union[bool, str, int]], DataSourceConfiguration],
    ) -> None:
        self._sleeps = CancellableSleeps()
        self._logger = logger

        self.configuration = configuration

        self.server_url = self.configuration["server_url"]
        self.verify_ssl = self.configuration["verify_ssl"]

        connector = aiohttp.TCPConnector(
            family=socket.AF_INET, verify_ssl=self.verify_ssl
        )
        self.http_session = aiohttp.ClientSession(
            connector=connector, raise_for_status=True
        )

        self.token = SandflyAccessToken(
            http_session=self.http_session,
            configuration=configuration,
            logger_=self._logger,
        )
        self.client = SandflySession(
            http_session=self.http_session,
            token=self.token,
            logger_=self._logger,
        )

    def set_logger(self, logger_: ExtraLogger) -> None:
        self._logger = logger_
        self.token.set_logger(self._logger)
        self.client.set_logger(self._logger)

    async def close(self) -> None:
        await self.http_session.close()
        self.client.close()

    async def ping(self) -> bool:
        try:
            await self.client.ping(self.server_url)
            self._logger.info(
                f"SandflyClient PING : Successfully connected to Sandfly Security [{self.server_url}]"
            )
            return True
        except Exception:
            self._logger.error(
                f"SandflyClient PING : Error while connecting to Sandfly Security [{self.server_url}]"
            )
            raise

    async def get_results_by_id(self, sequence_id, enable_pass):
        results_url = f"{self.server_url}/results"

        payload = None

        if enable_pass:
            payload = {
                "size": RESULTS_SIZE,
                "filter": {
                    "items": [
                        {
                            "columnField": "sequence_id",
                            "operatorValue": ">",
                            "value": sequence_id,
                        }
                    ]
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }
        else:
            payload = {
                "size": RESULTS_SIZE,
                "filter": {
                    "items": [
                        {
                            "columnField": "data.status",
                            "operatorValue": "notEquals",
                            "value": "pass",
                        },
                        {
                            "columnField": "sequence_id",
                            "operatorValue": ">",
                            "value": sequence_id,
                        },
                    ],
                    "linkoperator": "and",
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }

        content = await self.client.content_post(url=results_url, payload=payload)

        if content is not None:
            content_json = json.loads(content)
            total = content_json["total"]
            more_results = content_json["more_results"]
            self._logger.debug(f"GET_RESULTS_BY_ID : [{total}] : [{more_results}]")

            data_list = content_json["data"]
            for result_item in data_list:
                yield result_item, more_results

    async def get_results_by_time(
        self, time_since: str, enable_pass: bool
    ) -> Iterator[Task]:
        results_url = f"{self.server_url}/results"

        if enable_pass:
            payload = {
                "size": RESULTS_SIZE,
                "time_since": time_since,
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }
        else:
            payload = {
                "size": RESULTS_SIZE,
                "time_since": time_since,
                "filter": {
                    "items": [
                        {
                            "columnField": "data.status",
                            "operatorValue": "notEquals",
                            "value": "pass",
                        }
                    ]
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }

        content = await self.client.content_post(url=results_url, payload=payload)

        if content is not None:
            content_json = json.loads(content)

            total = content_json["total"]
            more_results = content_json["more_results"]
            self._logger.debug(f"GET_RESULTS_BY_TIME : [{total}] : [{more_results}]")

            data_list = content_json["data"]
            for result_item in data_list:
                yield result_item, more_results

    async def get_ssh_keys(self):
        ssh_url = f"{self.server_url}/sshhunter/summary"
        content = await self.client.content_get(url=ssh_url)

        if content is not None:
            content_json = json.loads(content)

            more_results = content_json["more_results"]
            data_list = content_json["data"]

            for key in data_list:
                key_id = key["id"]
                key_url = f"{self.server_url}/sshhunter/key/{key_id}"

                key_details = await self.client.content_get(url=key_url)
                if key_details is not None:
                    yield json.loads(key_details), more_results

    async def get_hosts(self):
        hosts_url = f"{self.server_url}/hosts"
        content = await self.client.content_get(url=hosts_url)

        if content is not None:
            content_json = json.loads(content)

            data_list = content_json["data"]
            for host in data_list:
                yield host

    async def get_license(self) -> Iterator[Task]:
        license_url = f"{self.server_url}/license"
        content = await self.client.content_get(url=license_url)

        if content is not None:
            content_json = json.loads(content)
            yield content_json


class SandflyDataSource(BaseDataSource):
    """Sandfly Security"""

    name = "Sandfly Security"
    service_type = "sandfly"
    incremental_sync_enabled = True

    def __init__(self, configuration: DataSourceConfiguration) -> None:
        super().__init__(configuration=configuration)
        self._logger = logger

        self.server_url = self.configuration["server_url"]
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.enable_pass = self.configuration["enable_pass"]
        self.verify_ssl = self.configuration["verify_ssl"]
        self.fetch_days = self.configuration["fetch_days"]

    @cached_property
    def client(self) -> SandflyClient:
        return SandflyClient(configuration=self.configuration)

    @classmethod
    def get_default_configuration(cls) -> Dict[str, Dict[str, Union[bool, str, int]]]:
        return {
            "server_url": {
                "label": "Sandfly Server URL",
                "order": 1,
                "tooltip": "Sandfly Server URL including the API version (v4).",
                "type": "str",
                "validations": [],
                "value": "https://server-name/v4",
            },
            "username": {
                "label": "Sandfly Server Username",
                "order": 2,
                "type": "str",
                "validations": [],
                "value": "<username>",
            },
            "password": {
                "label": "Sandfly Server Password",
                "order": 3,
                "sensitive": True,
                "type": "str",
                "validations": [],
                "value": "<password>",
            },
            "enable_pass": {
                "display": "toggle",
                "label": "Enable Pass Results",
                "order": 4,
                "tooltip": "Enable Pass Results, default is to include only Alert and Error Results.",
                "type": "bool",
                "value": False,
            },
            "verify_ssl": {
                "display": "toggle",
                "label": "Verify SSL Certificate",
                "order": 5,
                "tooltip": "Verify Sandfly Server SSL Certificate, disable to allow self-signed certificates.",
                "type": "bool",
                "value": True,
            },
            "fetch_days": {
                "display": "numeric",
                "label": "Days of results history to fetch",
                "order": 6,
                "tooltip": "Number of days of results history to fetch on a Full Content Sync.",
                "value": 30,
                "type": "int",
            },
        }

    async def ping(self) -> bool:
        try:
            await self.client.ping()
            return True
        except Exception:
            raise

    async def close(self) -> None:
        await self.client.close()

    def init_sync_cursor(self) -> Dict[str, Union[str, int]]:
        if not self._sync_cursor:
            self._sync_cursor = {
                CURSOR_SEQUENCE_ID_KEY: 0,
                CURSOR_SYNC_TIMESTAMP: iso_utc(),
            }

        return self._sync_cursor

    def _format_doc(
        self,
        doc_id: str,
        doc_time: str,
        doc_text: str,
        doc_field: str,
        doc_data: Dict[
            str,
            Optional[Union[str, Dict[str, Dict[str, Dict[str, str]]], Dict[str, str]]],
        ],
    ) -> Dict[str, Any]:
        document = {
            "_id": doc_id,
            "_timestamp": doc_time,
            "sandfly_data_type": doc_field,
            "sandfly_key_data": doc_text,
            doc_field: doc_data,
        }
        return document

    def extract_results_data(
        self, result_item: Dict[str, Union[Dict[str, str], str]], get_more_results: bool
    ) -> Tuple[str, str, str, str]:
        last_sequence_id = result_item["sequence_id"]
        external_id = result_item["external_id"]
        timestamp = result_item["header"]["end_time"]
        key_data = result_item["data"]["key_data"]
        status = result_item["data"]["status"]

        if len(key_data) == 0:
            key_data = "- no data -"

        doc_id = hash_id(external_id)

        self._logger.debug(
            f"SANDFLY RESULTS : [{doc_id}] : [{external_id}] - [{status}] [{last_sequence_id}] [{key_data}] [{timestamp}] : [{get_more_results}]"
        )

        return timestamp, key_data, last_sequence_id, doc_id

    def extract_sshkey_data(self, key_item: Dict[str, str]) -> Tuple[str, str]:
        friendly = key_item["friendly_name"]
        key_value = key_item["key_value"]

        doc_id = hash_id(key_value)

        self._logger.debug(f"SANDFLY SSH_KEYS : [{doc_id}] : [{friendly}]")

        return friendly, doc_id

    def extract_host_data(
        self,
        host_item: Dict[
            str, Optional[Union[str, Dict[str, Dict[str, Dict[str, str]]]]]
        ],
    ) -> Tuple[str, str]:
        hostid = host_item["host_id"]
        hostname = host_item["hostname"]

        nodename = "<unknown>"
        if "data" in host_item:
            if host_item["data"] is not None:
                if "os" in host_item["data"]:
                    if "info" in host_item["data"]["os"]:
                        if "node" in host_item["data"]["os"]["info"]:
                            nodename = host_item["data"]["os"]["info"]["node"]

        key_data = f"{nodename} ({hostname})"
        doc_id = hash_id(hostid)

        self._logger.debug(f"SANDFLY HOSTS : [{doc_id}] : [{key_data}]")

        return key_data, doc_id

    def validate_license(
        self, license_data: Dict[str, Union[int, Dict[str, str], Dict[str, List[str]]]]
    ) -> None:
        customer = license_data["customer"]["name"]
        expiry = license_data["date"]["expiry"]

        self._logger.debug(f"SANDFLY VALIDATE_LICENSE : [{customer}] : [{expiry}]")

        now = datetime.utcnow()
        expiry_date = extract_sandfly_date(expiry)
        if expiry_date < now:
            msg = f"Sandfly Server [{self.server_url}] license has expired [{expiry}]"
            raise SandflyLicenseExpired(msg)

        is_licensed = False
        if "limits" in license_data:
            if "features" in license_data["limits"]:
                features_list = license_data["limits"]["features"]
                for feature in features_list:
                    if feature == "elasticsearch_replication":
                        is_licensed = True

        if not is_licensed:
            msg = f"Sandfly Server [{self.server_url}] is not licensed for Elasticsearch Replication"
            raise SandflyNotLicensed(msg)

    async def get_docs(self, filtering: None = None):
        self.init_sync_cursor()

        async for license_data in self.client.get_license():
            try:
                self.validate_license(license_data)
            except Exception as exception:
                self._logger.error(f"SandflyDataSource GET_DOCS : [{exception}]")
                raise

        async for host_item in self.client.get_hosts():
            key_data, doc_id = self.extract_host_data(host_item)

            yield (
                self._format_doc(
                    doc_id=doc_id,
                    doc_time=iso_utc(),
                    doc_text=key_data,
                    doc_field="sandfly_hosts",
                    doc_data=host_item,
                ),
                None,
            )

        async for key_item, _get_more_results in self.client.get_ssh_keys():
            friendly, doc_id = self.extract_sshkey_data(key_item)

            yield (
                self._format_doc(
                    doc_id=doc_id,
                    doc_time=iso_utc(),
                    doc_text=friendly,
                    doc_field="sandfly_ssh_keys",
                    doc_data=key_item,
                ),
                None,
            )

        now = datetime.utcnow()
        then = now + timedelta(days=-self.fetch_days)
        time_since = format_sandfly_date(date=then, flag=True)

        last_sequence_id = None
        get_more_results = False

        async for result_item, get_more_results in self.client.get_results_by_time(
            time_since, self.enable_pass
        ):
            timestamp, key_data, last_sequence_id, doc_id = self.extract_results_data(
                result_item, get_more_results
            )

            yield (
                self._format_doc(
                    doc_id=doc_id,
                    doc_time=timestamp,
                    doc_text=key_data,
                    doc_field="sandfly_results",
                    doc_data=result_item,
                ),
                None,
            )

        self._logger.debug(
            f"SANDFLY GET_MORE_RESULTS-time : [{last_sequence_id}] : [{get_more_results}]"
        )
        if last_sequence_id is not None:
            self._sync_cursor[CURSOR_SEQUENCE_ID_KEY] = last_sequence_id

        while get_more_results:
            get_more_results = False

            async for result_item, get_more_results in self.client.get_results_by_id(
                last_sequence_id, self.enable_pass
            ):
                timestamp, key_data, last_sequence_id, doc_id = (
                    self.extract_results_data(result_item, get_more_results)
                )

                yield (
                    self._format_doc(
                        doc_id=doc_id,
                        doc_time=timestamp,
                        doc_text=key_data,
                        doc_field="sandfly_results",
                        doc_data=result_item,
                    ),
                    None,
                )

            self._logger.debug(
                f"SANDFLY GET_MORE_RESULTS-id : [{last_sequence_id}] : [{get_more_results}]"
            )
            if last_sequence_id is not None:
                self._sync_cursor[CURSOR_SEQUENCE_ID_KEY] = last_sequence_id

    async def get_docs_incrementally(
        self, sync_cursor: Optional[Dict[str, str]], filtering: None = None
    ):
        self._sync_cursor = sync_cursor
        timestamp = iso_utc()

        if not self._sync_cursor:
            msg = "Unable to start incremental sync. Please perform a full sync to re-enable incremental syncs."
            raise SyncCursorEmpty(msg)

        async for license_data in self.client.get_license():
            try:
                self.validate_license(license_data)
            except Exception as exception:
                self._logger.error(f"SandflyDataSource GET_DOCS_INC : [{exception}]")
                raise

        async for host_item in self.client.get_hosts():
            key_data, doc_id = self.extract_host_data(host_item)

            yield (
                self._format_doc(
                    doc_id=doc_id,
                    doc_time=iso_utc(),
                    doc_text=key_data,
                    doc_field="sandfly_hosts",
                    doc_data=host_item,
                ),
                None,
                OP_INDEX,
            )

        get_more_results = False

        async for key_item, _get_more_results in self.client.get_ssh_keys():
            friendly, doc_id = self.extract_sshkey_data(key_item)

            yield (
                self._format_doc(
                    doc_id=doc_id,
                    doc_time=iso_utc(),
                    doc_text=friendly,
                    doc_field="sandfly_ssh_keys",
                    doc_data=key_item,
                ),
                None,
                OP_INDEX,
            )

        last_sequence_id = self._sync_cursor[CURSOR_SEQUENCE_ID_KEY]
        self._logger.debug(
            f"SANDFLY INCREMENTAL last_sequence_id : [{last_sequence_id}]"
        )
        get_more_results = True

        while get_more_results:
            get_more_results = False

            async for result_item, get_more_results in self.client.get_results_by_id(
                last_sequence_id, self.enable_pass
            ):
                timestamp, key_data, last_sequence_id, doc_id = (
                    self.extract_results_data(result_item, get_more_results)
                )

                yield (
                    self._format_doc(
                        doc_id=doc_id,
                        doc_time=timestamp,
                        doc_text=key_data,
                        doc_field="sandfly_results",
                        doc_data=result_item,
                    ),
                    None,
                    OP_INDEX,
                )

            self._logger.debug(
                f"SANDFLY INCREMENTAL GET_MORE_RESULTS-id : [{last_sequence_id}] : [{get_more_results}]"
            )
            self._sync_cursor[CURSOR_SEQUENCE_ID_KEY] = last_sequence_id

        self.update_sync_timestamp_cursor(timestamp)
