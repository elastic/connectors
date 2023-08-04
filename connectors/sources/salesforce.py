#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Salesforce source module responsible to fetch documents from Salesforce."""
from functools import cached_property

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import retryable

RETRIES = 3
RETRY_INTERVAL = 1
QUERY_PAGINATION_LIMIT = 100  # TODO: maybe make the limit configurable

BASE_URL = "https://<domain>.my.salesforce.com"
API_VERSION = "v58.0"
TOKEN_ENDPOINT = "/services/oauth2/token"
QUERY_ENDPOINT = f"/services/data/{API_VERSION}/query"
DESCRIBE_ENDPOINT = f"/services/data/{API_VERSION}/sobjects"
DESCRIBE_SOBJECT_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/<sobject>/describe"

RELEVANT_SOBJECTS = ["Account", "Opportunity"]
RELEVANT_SOBJECT_FIELDS = [
    "Name",
    "Description",
    "BillingAddress",
    "Type",
    "Website",
    "Rating",
    "Department",
    "StageName",
]


class RateLimitedException(Exception):
    """Notifies that Salesforce has begun rate limiting the current accound"""

    pass


class RequestRefusedException(Exception):
    """Notifies that a request to Saleforce was rejected"""

    pass


class InvalidCredentialsException(Exception):
    """Notifies that credentials are invalid for fetching a Salesforce token"""

    pass


class TokenFetchException(Exception):
    """Notifies that an unexpected error occurred when fetching a Salesforce token"""

    pass


class SalesforceClient:
    def __init__(self, configuration):
        self._logger = logger

        self.token = None
        self._fetching_token = False
        self._queryable_sobjects = None
        self._queryable_sobject_fields = None

        self.base_url = BASE_URL.replace("<domain>", configuration["domain"])
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self.token_payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        self.doc_mapper = SalesforceDocMapper(self.base_url)

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None),
        )

    async def ping(self):
        # TODO ping something of value (this could be config check instead)
        await self.session.head(self.base_url)

    async def close(self):
        self._token_cleanup()
        if self.session is not None:
            await self.session.close()
            del self.session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def get_token(self):
        if self._fetching_token is True:
            return

        self._fetching_token = True
        try:
            resp = await self._post(
                f"{self.base_url}{TOKEN_ENDPOINT}", data=self.token_payload
            )
            resp_json = await resp.json()
            resp.raise_for_status()
            self.token = resp_json["access_token"]
        except ClientResponseError as e:
            if 400 <= e.status < 500:
                # 400s have error details in body
                if resp_json.get("error") == "invalid_client":
                    raise InvalidCredentialsException(
                        f"The `client_id` and `client_secret` provided could not be used to generate a token. Status: {e.status}, message: {e.message}, details: {resp_json.get('error_description')}"
                    ) from e
                else:
                    raise TokenFetchException(
                        f"Could not fetch token from Salesforce: Status: {e.status}, message: {e.message}, details: {resp_json.get('error_description')}"
                    ) from e
            else:
                raise TokenFetchException(
                    f"Unexpected error while fetching Salesforce token. Status: {e.status}, message: {e.message}"
                ) from e
        finally:
            self._fetching_token = False

    async def get_accounts(self):
        if not await self._is_queryable("Account"):
            return

        query = await self._accounts_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield self.doc_mapper.map_account(record)

    async def get_opportunities(self):
        if not await self._is_queryable("Opportunity"):
            return

        query = await self._opportunities_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield self.doc_mapper.map_opportunity(record)

    async def queryable_sobjects(self):
        """Cached async property"""
        if self._queryable_sobjects is not None:
            return self._queryable_sobjects

        response = await self._get_json(f"{self.base_url}{DESCRIBE_ENDPOINT}")
        self._queryable_sobjects = []

        for sobject in response["sobjects"]:
            if sobject["queryable"] is True and sobject["name"] in RELEVANT_SOBJECTS:
                self._queryable_sobjects.append(sobject["name"].lower())

        return self._queryable_sobjects

    async def queryable_sobject_fields(self):
        """Cached async property"""
        if self._queryable_sobject_fields is not None:
            return self._queryable_sobject_fields

        self._queryable_sobject_fields = {}

        for sobject in RELEVANT_SOBJECTS:
            endpoint = DESCRIBE_SOBJECT_ENDPOINT.replace("<sobject>", sobject)
            response = await self._get_json(f"{self.base_url}{endpoint}")

            queryable_fields = [
                f["name"].lower()
                for f in response["fields"]
                if f["name"] in RELEVANT_SOBJECT_FIELDS
            ]
            self._queryable_sobject_fields[sobject] = queryable_fields

        return self._queryable_sobject_fields

    async def _is_queryable(self, sobject):
        return sobject.lower() in await self.queryable_sobjects()

    async def _select_queryable_fields(self, sobject, fields):
        """User settings can cause fields to be non-queryable
        This causes
        """
        sobject_fields = await self.queryable_sobject_fields()
        queryable_fields = sobject_fields.get(sobject, [])
        return [f for f in fields if f.lower() in queryable_fields]

    async def _yield_non_bulk_query_pages(self, soql_query):
        """loops through query response pages and yields lists of records"""
        response = await self._get_json(
            f"{self.base_url}{QUERY_ENDPOINT}",
            params={"q": soql_query},
        )
        yield response["records"]

        if response["done"] is False:
            page_count = 1
            next_url = response["nextRecordsUrl"]
            while True:
                next_page = await self._get_json(next_url)
                yield next_page["records"]

                page_count += 1
                if next_page["done"] is True or page_count >= QUERY_PAGINATION_LIMIT:
                    break
                else:
                    next_url = next_page["nextRecordsUrl"]

    def _token_cleanup(self):
        self.token = None

    def _auth_headers(self):
        return {"authorization": f"Bearer {self.token}"}

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def _get_json(self, url, params=None):
        try:
            resp = await self._get(url, params=params)
            resp_json = await resp.json()
            # We get the response body before raising for status as it contains vital error information
            resp.raise_for_status()
            return resp_json
        except ClientResponseError as e:
            await self._handle_client_response_error(resp_json, e)
        except Exception as e:
            raise e

    async def _get(self, url, params=None):
        return await self.session.get(
            url,
            headers=self._auth_headers(),
            params=params,
        )

    async def _post(self, url, data=None):
        return await self.session.post(url, data=data)

    async def _handle_client_response_error(self, resp_json, e):
        match e.status:
            case 401:
                self._logger.warning(
                    f"Token expired, attemping to fetch new token. Status: {e.status}, message: {e.message}"
                )
                # The user can alter the lifetime of issued tokens, so we don't know when they expire
                # Therefore we fetch the token when we encounter an error rather than when it expires
                await self.get_token()
                # raise to continue with retry strategy
                raise e
            case 403:
                # The response error format is an array for some reason.
                # I couldn't find an example of multiple error codes,
                # but just to be safe we concatenate them in the error message
                error_codes = [x["errorCode"] for x in resp_json]
                if "REQUEST_LIMIT_EXCEEDED" in error_codes:
                    raise RateLimitedException(
                        f"Salesforce is rate limiting this account. ErrorCode(s): {', '.join(error_codes)}"
                    ) from e
                else:
                    raise RequestRefusedException(
                        f"Salesforce rejected the request. This can be caused by incorrect permissions. ErrorCode(s): {', '.join(error_codes)}"
                    ) from e
            case _:
                raise e

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


class SalesforceDocMapper:
    def __init__(self, base_url):
        self.base_url = base_url

    def map_account(self, account):
        owner = account.get("Owner", {})

        opportunities = account.get("Opportunities")
        opportunity_records = opportunities.get("records", []) if opportunities else []
        opportunity = opportunity_records[0] if len(opportunity_records) > 0 else {}
        opportunity_url = (
            f"{self.base_url}/{opportunity.get('Id')}" if opportunity else ""
        )
        opportunity_status = opportunity.get("StageName", "")

        return {
            "_id": account.get("Id"),
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

    def map_opportunity(self, opportunity):
        owner = opportunity.get("Owner", {})

        return {
            "_id": opportunity.get("Id"),
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

        # TODO filtering
        async for account in self.salesforce_client.get_accounts():
            yield account, None

        async for opportunity in self.salesforce_client.get_opportunities():
            yield opportunity, None
