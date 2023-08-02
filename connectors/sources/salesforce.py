#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Salesforce source module responsible to fetch documents from Salesforce."""
from functools import cached_property

import aiohttp

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import retryable

RETRIES = 3
RETRY_INTERVAL = 1

BASE_URL = "https://<domain>.my.salesforce.com"
API_VERSION = "v58.0"
TOKEN_ENDPOINT = "/services/oauth2/token"
QUERY_ENDPOINT = f"/services/data/{API_VERSION}/query"


class SalesforceClient:
    def __init__(self, configuration):
        self._logger = logger

        self.token = None
        self.token_issued_at = None
        self.token_refresh_enabled = False

        self.base_url = BASE_URL.replace("<domain>", configuration["domain"])
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self.token_payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

    async def ping(self):
        # TODO ping something of value (this could be config check instead)
        await self._api_request(self.base_url, "head")

    async def close(self):
        self._token_cleanup()
        if self.session is not None:
            await self.session.close()
            del self.session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def get_token(self, enable_refresh=True):
        # TODO add refresh method
        self._logger.debug("Fetching Salesforce token...")
        resp = await self._api_request(
            f"{self.base_url}{TOKEN_ENDPOINT}", "post", data=self.token_payload
        )
        resp_json = await resp.json()
        self.token = resp_json["access_token"]
        self.token_issued_at = resp_json["issued_at"]
        self.token_refresh_enabled = enable_refresh
        self._logger.debug("Salesforce token retrieved.")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def get_accounts(self):
        # TODO prepare cache

        query_builder = SalesforceSoqlBuilder("Account")
        query_builder.with_id()
        query_builder.with_default_metafields()
        query_builder.with_fields(
            self._select_queryable_fields(
                [
                    "Name",
                    "Description",
                    "BillingAddress",
                    "Type",
                    "Website",
                    "Rating",
                    "Department",
                ]
            )
        )
        query = query_builder.build()

        # TODO handle pagination
        resp = await self._yield_non_bulk_query_pages(query)
        resp_json = await resp.json()
        for record in resp_json.get("records", []):
            record["_id"] = record["Id"]
            yield record

    async def _yield_non_bulk_query_pages(self, soql_query):
        return await self._api_request(
            f"{self.base_url}{QUERY_ENDPOINT}",
            "get",
            headers=self._auth_headers(),
            params={"q": soql_query},
        )

    def _select_queryable_fields(self, fields):
        # TODO check if fields queryable first from cache
        return fields

    def _token_cleanup(self):
        self.token = None
        self.token_issued_at = None
        self.token_refresh_enabled = False

    def _auth_headers(self):
        return {"authorization": f"Bearer {self.token}"}

    async def _api_request(self, url, method, headers=None, data=None, params=None):
        # TODO improve error handling
        return await getattr(self.session, method)(
            url=url, headers=headers, data=data, params=params
        )


class SalesforceSoqlBuilder:
    def __init__(self, table):
        self.table_name = table
        self.fields = []

    def with_id(self):
        self.fields.append("Id")

    def with_default_metafields(self):
        self.fields.extend(["CreatedDate", "LastModifiedDate"])

    def with_fields(self, fields):
        self.fields.extend(fields)

    def build(self):
        select_columns = ",\n".join(set(self.fields))

        query_lines = [f"SELECT {select_columns}", f"FROM {self.table_name}"]
        # TODO expand functionality when needed (where, order_by, limit, etc)

        return "\n".join(query_lines)


class SalesforceDataSource(BaseDataSource):
    """Salesforce"""

    name = "Salesforce"
    service_type = "salesforce"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.salesforce_client = SalesforceClient(configuration=configuration)

    def _set_internal_logger(self):
        self.salesforce_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "client_id": {
                "label": "Client ID",
                "type": "str",
                "value": "",
            },
            "client_secret": {
                "label": "Client Secret",
                "type": "str",
                "value": "",
            },
            "domain": {
                "label": "Domain",
                "type": "str",
                "value": "",
            },
        }

    async def validate_config(self):
        self.configuration.check_valid()

    async def close(self):
        await self.salesforce_client.close()

    async def ping(self):
        try:
            await self.salesforce_client.ping()
            self._logger.debug("Successfully connected to Salesforce.")
        except Exception as e:
            self._logger.exception(f"Error while connecting to Salesforce: {e}")
            raise

    async def get_content(self, attachment, timestamp=None, doit=False):
        # TODO implement
        return

    async def get_docs(self, filtering=None):
        # TODO rename
        await self.salesforce_client.get_token()

        # TODO filtering
        async for account in self.salesforce_client.get_accounts():
            yield account, None
