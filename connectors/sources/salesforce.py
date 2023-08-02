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
DESCRIBE_ENDPOINT = f"/services/data/{API_VERSION}/sobjects"
DESCRIBE_SOBJECT_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/<sobject>/describe"


class SalesforceClient:
    def __init__(self, configuration):
        self._logger = logger

        self.token = None
        self.token_issued_at = None
        self.token_refresh_enabled = False
        self.queryable_sobjects = []

        self.base_url = BASE_URL.replace("<domain>", configuration["domain"])
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self.token_payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        self.doc_builder = SalesforceDocBuilder(self.base_url)

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
        if not await self._is_queryable("Account"):
            return

        # TODO handle pagination
        query = await self._accounts_query()
        resp = await self._yield_non_bulk_query_pages(query)
        resp_json = await resp.json()
        for record in resp_json.get("records", []):
            yield self.doc_builder.build_account(record)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def get_opportunities(self):
        if not await self._is_queryable("Opportunity"):
            return

        # TODO handle pagination
        query = await self._opportunities_query()
        resp = await self._yield_non_bulk_query_pages(query)
        resp_json = await resp.json()
        for record in resp_json.get("records", []):
            yield self.doc_builder.build_oppoortunity(record)

    async def prepare_caches(self):
        await self._cache_queryable_sobjects()

    async def _cache_queryable_sobjects(self):
        resp = await self._api_request(
            f"{self.base_url}{DESCRIBE_ENDPOINT}", "get", headers=self._auth_headers()
        )
        resp_json = await resp.json()
        self.queryable_sobjects = [
            x["name"].lower() for x in resp_json["sobjects"] if x["queryable"] is True
        ]

    async def _is_queryable(self, sobject):
        return sobject.lower() in self.queryable_sobjects

    async def _select_queryable_fields(self, sobject, fields):
        """User settings can cause fields to be non-queryable
        This causes
        """
        endpoint = DESCRIBE_SOBJECT_ENDPOINT.replace("<sobject>", sobject)
        resp = await self._api_request(
            f"{self.base_url}{endpoint}", "get", headers=self._auth_headers()
        )
        resp_json = await resp.json()
        queryable_fields = [f["name"] for f in resp_json["fields"]]
        return [f for f in fields if f in queryable_fields]

    async def _yield_non_bulk_query_pages(self, soql_query):
        return await self._api_request(
            f"{self.base_url}{QUERY_ENDPOINT}",
            "get",
            headers=self._auth_headers(),
            params={"q": soql_query},
        )

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

    async def _accounts_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Account",
            [
                "Name",
                "Description",
                "BillingAddress",
                "Type",
                "Website",
                "Rating",
                "Department",
            ],
        )
        query_builder = SalesforceSoqlBuilder("Account")
        query_builder.with_id()
        query_builder.with_default_metafields()
        query_builder.with_fields(queryable_fields)
        # TODO add uncommon_object_remote_fields
        query_builder.with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])
        query_builder.with_fields(["Parent.Id", "Parent.Name"])

        if await self._is_queryable("Opportunity"):
            queryable_join_fields = await self._select_queryable_fields(
                "Opportunity",
                [
                    "Name",
                    "StageName",
                ],
            )
            join_builder = SalesforceSoqlBuilder("Opportunities")
            join_builder.with_id()
            join_builder.with_fields(queryable_join_fields)
            join_builder.with_order_by("CreatedDate DESC")
            join_builder.with_limit(1)
            query_builder.with_join(join_builder.build())

        return query_builder.build()

    async def _opportunities_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Opportunity",
            [
                "Name",
                "Description",
                "StageName",
            ],
        )
        query_builder = SalesforceSoqlBuilder("Opportunity")
        query_builder.with_id()
        query_builder.with_default_metafields()
        query_builder.with_fields(queryable_fields)
        # TODO add uncommon_object_remote_fields
        query_builder.with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])

        return query_builder.build()


class SalesforceSoqlBuilder:
    def __init__(self, table):
        self.table_name = table
        self.fields = []
        self.where = ""
        self.order_by = ""
        self.limit = ""

    def with_id(self):
        self.fields.append("Id")

    def with_default_metafields(self):
        self.fields.extend(["CreatedDate", "LastModifiedDate"])

    def with_fields(self, fields):
        self.fields.extend(fields)

    def with_where(self, where_string):
        self.where = f"WHERE {where_string}"

    def with_order_by(self, order_by_string):
        self.order_by = f"ORDER BY {order_by_string}"

    def with_limit(self, limit):
        self.limit = f"LIMIT {limit}"

    def with_join(self, join):
        self.fields.append(f"(\n{join})\n")

    def build(self):
        select_columns = ",\n".join(set(self.fields))

        query_lines = []
        query_lines.append(f"SELECT {select_columns}")
        query_lines.append(f"FROM {self.table_name}")
        query_lines.append(self.where)
        query_lines.append(self.order_by)
        query_lines.append(self.limit)

        return "\n".join([line for line in query_lines if line != ""])


class SalesforceDocBuilder:
    def __init__(self, base_url):
        self.base_url = base_url

    def build_account(self, account):
        owner = account.get("Owner", {})

        opportunities = account.get("Opportunities")
        opportunity_records = opportunities.get("records", []) if opportunities else []
        opportunity = opportunity_records[0] if len(opportunity_records) > 0 else {}
        opportunity_url = (
            f"{self.base_url}/{opportunity.get('Id')}" if opportunity else ""
        )
        opportunity_status = opportunity.get("StageName", "")

        return {
            "_id": f"salesforce_connector_{account.get('Id')}",
            "account_type": account.get("Type"),
            "address": self._format_address(account.get("BillingAddress")),
            "body": account.get("Description"),
            "content_source_id": account.get("Id"),
            "created_at": account.get("CreatedDate"),
            "last_updated": account.get("LastModifiedDate"),
            "open_activities": "",  # TODO
            "open_activities_urls": "",  # TODO
            "opportunity_name": opportunity.get("Name"),
            "opportunity_status": opportunity_status,
            "opportunity_url": opportunity_url,
            "owner": owner.get("Name"),
            "owner_email": owner.get("Email"),
            "rating": account.get("Rating"),
            "source": "salesforce",
            "tags": [account.get("Type")],
            "title": account.get("Name"),
            "type": "account",
            "url": f"{self.base_url}/{account.get('Id')}",
            "website_url": account.get("Website"),
        }

    def build_oppoortunity(self, opportunity):
        owner = opportunity.get("Owner", {})

        return {
            "_id": f"salesforce_connector_{opportunity.get('Id')}",
            "body": opportunity.get("Description"),
            "content_source_id": opportunity.get("Id"),
            "created_at": opportunity.get("CreatedDate"),
            "last_updated": opportunity.get("LastModifiedDate"),
            "next_step": opportunity.get("NextStep"),
            "owner": owner.get("Name"),
            "owner_email": owner.get("Email"),
            "source": "salesforce",
            "status": opportunity.get("StageName", ""),
            "title": opportunity.get("Name"),
            "type": "opportunity",
            "url": f"{self.base_url}/{opportunity.get('Id')}",
        }

    def _format_address(self, address):
        if not address:
            return ""

        address_fields = [
            address.get("street"),
            address.get("city"),
            address.get("state"),
            str(address.get("postalCode", "")),
            address.get("country"),
        ]
        return ", ".join([a for a in address_fields if a])


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
        await self.salesforce_client.get_token()
        await self.salesforce_client.prepare_caches()

        # TODO filtering
        async for account in self.salesforce_client.get_accounts():
            yield account, None

        async for opportunity in self.salesforce_client.get_opportunities():
            yield opportunity, None
