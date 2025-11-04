#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from datetime import datetime, timedelta
from functools import cached_property

from connectors_sdk.logger import logger
from connectors_sdk.source import CURSOR_SYNC_TIMESTAMP, BaseDataSource
from connectors_sdk.utils import (
    hash_id,
    iso_utc,
)

from connectors.es.sink import OP_INDEX
from connectors.sources.sandfly.client import SandflyClient

CURSOR_SEQUENCE_ID_KEY = "sequence_id"


def extract_sandfly_date(datestr):
    return datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%SZ")


def format_sandfly_date(date, flag):
    if flag:
        return date.strftime("%Y-%m-%dT00:00:00Z")  # date with time as midnight
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass


class SandflyLicenseExpired(Exception):
    pass


class SandflyNotLicensed(Exception):
    pass


class SandflyDataSource(BaseDataSource):
    """Sandfly Security"""

    name = "Sandfly Security"
    service_type = "sandfly"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self._logger = logger

        self.server_url = self.configuration["server_url"]
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.enable_pass = self.configuration["enable_pass"]
        self.verify_ssl = self.configuration["verify_ssl"]
        self.fetch_days = self.configuration["fetch_days"]

    @cached_property
    def client(self):
        return SandflyClient(configuration=self.configuration)

    @classmethod
    def get_default_configuration(cls):
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

    async def ping(self):
        try:
            await self.client.ping()
            return True
        except Exception:
            raise

    async def close(self):
        await self.client.close()

    def init_sync_cursor(self):
        if not self._sync_cursor:
            self._sync_cursor = {
                CURSOR_SEQUENCE_ID_KEY: 0,
                CURSOR_SYNC_TIMESTAMP: iso_utc(),
            }

        return self._sync_cursor

    def _format_doc(self, doc_id, doc_time, doc_text, doc_field, doc_data):
        document = {
            "_id": doc_id,
            "_timestamp": doc_time,
            "sandfly_data_type": doc_field,
            "sandfly_key_data": doc_text,
            doc_field: doc_data,
        }
        return document

    def extract_results_data(self, result_item, get_more_results):
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

    def extract_sshkey_data(self, key_item):
        friendly = key_item["friendly_name"]
        key_value = key_item["key_value"]

        doc_id = hash_id(key_value)

        self._logger.debug(f"SANDFLY SSH_KEYS : [{doc_id}] : [{friendly}]")

        return friendly, doc_id

    def extract_host_data(self, host_item):
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

    def validate_license(self, license_data):
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

    async def get_docs(self, filtering=None):
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

    async def get_docs_incrementally(self, sync_cursor, filtering=None):
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
