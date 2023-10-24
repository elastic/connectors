#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Salesforce source module responsible to fetch documents from Salesforce."""
import os
from functools import cached_property, partial
from itertools import groupby

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    retryable,
)

SALESFORCE_EMULATOR_HOST = os.environ.get("SALESFORCE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

RETRIES = 3
RETRY_INTERVAL = 1

BASE_URL = "https://<domain>.my.salesforce.com"
API_VERSION = "v58.0"
TOKEN_ENDPOINT = "/services/oauth2/token"
QUERY_ENDPOINT = f"/services/data/{API_VERSION}/query"
DESCRIBE_ENDPOINT = f"/services/data/{API_VERSION}/sobjects"
DESCRIBE_SOBJECT_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/<sobject>/describe"
# https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_blob_retrieve.htm
CONTENT_VERSION_DOWNLOAD_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/ContentVersion/<content_version_id>/VersionData"

RELEVANT_SOBJECTS = [
    "Account",
    "Campaign",
    "Case",
    "CaseComment",
    "CaseFeed",
    "Contact",
    "ContentDocument",
    "ContentDocumentLink",
    "ContentVersion",
    "EmailMessage",
    "FeedComment",
    "Lead",
    "Opportunity",
    "User",
]
RELEVANT_SOBJECT_FIELDS = [
    "AccountId",
    "BccAddress",
    "BillingAddress",
    "Body",
    "CaseNumber",
    "CcAddress",
    "CommentBody",
    "CommentCount",
    "Company",
    "ContentSize",
    "ConvertedAccountId",
    "ConvertedContactId",
    "ConvertedDate",
    "ConvertedOpportunityId",
    "Department",
    "Description",
    "Email",
    "EndDate",
    "FileExtension",
    "FirstOpenedDate",
    "FromAddress",
    "FromName",
    "IsActive",
    "IsClosed",
    "IsDeleted",
    "LastEditById",
    "LastEditDate",
    "LastModifiedById",
    "LatestPublishedVersionId",
    "LeadSource",
    "LinkUrl",
    "MessageDate",
    "Name",
    "OwnerId",
    "ParentId",
    "Phone",
    "PhotoUrl",
    "Rating",
    "StageName",
    "StartDate",
    "Status",
    "StatusParentId",
    "Subject",
    "TextBody",
    "Title",
    "ToAddress",
    "Type",
    "VersionDataUrl",
    "VersionNumber",
    "Website",
]


class RateLimitedException(Exception):
    """Notifies that Salesforce has begun rate limiting the current account"""

    pass


class InvalidQueryException(Exception):
    """Notifies that a query was malformed or otherwise incorrect"""

    pass


class InvalidCredentialsException(Exception):
    """Notifies that credentials are invalid for fetching a Salesforce token"""

    pass


class TokenFetchException(Exception):
    """Notifies that an unexpected error occurred when fetching a Salesforce token"""

    pass


class ConnectorRequestError(Exception):
    """Notifies that a general uncaught 400 error occurred during a request, usually this is caused by the connector"""

    pass


class SalesforceServerError(Exception):
    """Notifies that an internal server error occurred in Salesforce"""

    pass


class SalesforceClient:
    def __init__(self, configuration, base_url):
        self._logger = logger
        self._sleeps = CancellableSleeps()

        self._queryable_sobjects = None
        self._queryable_sobject_fields = None
        self._sobjects_cache_by_type = None
        self._content_document_links_join = None

        self.base_url = base_url
        self.api_token = SalesforceAPIToken(
            self.session,
            self.base_url,
            configuration["client_id"],
            configuration["client_secret"],
        )

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None),
        )

    async def ping(self):
        await self.session.head(self.base_url)

    async def close(self):
        self.api_token.clear()
        await self.session.close()
        del self.session

    async def get_accounts(self):
        if not await self._is_queryable("Account"):
            self._logger.warning(
                "Object Account is not queryable, so they won't be ingested."
            )
            return

        query = await self._accounts_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_opportunities(self):
        if not await self._is_queryable("Opportunity"):
            self._logger.warning(
                "Object Opportunity is not queryable, so they won't be ingested."
            )
            return

        query = await self._opportunities_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_contacts(self):
        if not await self._is_queryable("Contact"):
            self._logger.warning(
                "Object Contact is not queryable, so they won't be ingested."
            )
            return

        query = await self._contacts_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                sobjects_by_id = await self.sobjects_cache_by_type()
                record["Account"] = sobjects_by_id["Account"].get(
                    record.get("AccountId"), {}
                )
                record["Owner"] = sobjects_by_id["User"].get(record.get("OwnerId"), {})
                yield record

    async def get_leads(self):
        if not await self._is_queryable("Lead"):
            self._logger.warning(
                "Object Lead is not queryable, so they won't be ingested."
            )
            return

        query = await self._leads_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                sobjects_by_id = await self.sobjects_cache_by_type()
                record["Owner"] = sobjects_by_id["User"].get(record.get("OwnerId"), {})
                record["ConvertedAccount"] = sobjects_by_id["Account"].get(
                    record.get("ConvertedAccountId"), {}
                )
                record["ConvertedContact"] = sobjects_by_id["Contact"].get(
                    record.get("ConvertedContactId"), {}
                )
                record["ConvertedOpportunity"] = sobjects_by_id["Opportunity"].get(
                    record.get("ConvertedOpportunityId"), {}
                )

                yield record

    async def get_campaigns(self):
        if not await self._is_queryable("Campaign"):
            self._logger.warning(
                "Object Campaign is not queryable, so they won't be ingested."
            )
            return

        query = await self._campaigns_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_cases(self):
        if not await self._is_queryable("Case"):
            self._logger.warning(
                "Object Case is not queryable, so they won't be ingested."
            )
            return

        query = await self._cases_query()
        async for records in self._yield_non_bulk_query_pages(query):
            case_feeds_by_case_id = {}
            if await self._is_queryable("CaseFeed") and records:
                case_ids = [x.get("Id") for x in records]
                case_feeds = await self.get_case_feeds(case_ids)

                # groupby requires pre-sorting apparently
                case_feeds.sort(key=lambda x: x.get("ParentId", ""))
                case_feeds_by_case_id = {
                    k: list(feeds)
                    for k, feeds in groupby(
                        case_feeds, key=lambda x: x.get("ParentId", "")
                    )
                }

            for record in records:
                record["Feeds"] = case_feeds_by_case_id.get(record.get("Id"))
                yield record

    async def get_case_feeds(self, case_ids):
        query = await self._case_feeds_query(case_ids)
        all_case_feeds = []
        async for case_feeds in self._yield_non_bulk_query_pages(query):
            all_case_feeds.extend(case_feeds)

        return all_case_feeds

    async def queryable_sobjects(self):
        """Cached async property"""
        if self._queryable_sobjects is not None:
            return self._queryable_sobjects

        response = await self._get_json(f"{self.base_url}{DESCRIBE_ENDPOINT}")
        self._queryable_sobjects = []

        for sobject in response.get("sobjects", []):
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
                for f in response.get("fields", [])
                if f["name"] in RELEVANT_SOBJECT_FIELDS
            ]
            self._queryable_sobject_fields[sobject] = queryable_fields

        return self._queryable_sobject_fields

    async def sobjects_cache_by_type(self):
        """Cached async property

        Many sobjects require extra data that is taxing on the rate limiter
        to repeatedly fetch each request.
        Instead we cache them on the first request for re-use later.
        """
        if self._sobjects_cache_by_type is not None:
            return self._sobjects_cache_by_type

        self._sobjects_cache_by_type = {}
        self._sobjects_cache_by_type["Account"] = await self._prepare_sobject_cache(
            "Account"
        )
        self._sobjects_cache_by_type["Contact"] = await self._prepare_sobject_cache(
            "Contact"
        )
        self._sobjects_cache_by_type["Opportunity"] = await self._prepare_sobject_cache(
            "Opportunity"
        )
        self._sobjects_cache_by_type["User"] = await self._prepare_sobject_cache("User")
        return self._sobjects_cache_by_type

    async def _prepare_sobject_cache(self, sobject):
        if not await self._is_queryable(sobject):
            self._logger.warning(
                f"{sobject} is not queryable, so they won't be cached."
            )
            return {}

        queryable_fields = ["Name"]
        if sobject in ["User", "Contact", "Lead"]:
            queryable_fields.append("Email")

        sobjects = {}
        query = (
            SalesforceSoqlBuilder(sobject)
            .with_id()
            .with_fields(queryable_fields)
            .build()
        )

        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                sobjects[record["Id"]] = record

        return sobjects

    async def _is_queryable(self, sobject):
        """User settings can cause sobjects to be non-queryable
        Querying these causes errors, so we try to filter those out in advance
        """
        return sobject.lower() in await self.queryable_sobjects()

    async def _select_queryable_fields(self, sobject, fields):
        """User settings can cause fields to be non-queryable
        Querying these causes errors, so we try to filter those out in advance
        """
        sobject_fields = await self.queryable_sobject_fields()
        queryable_fields = sobject_fields.get(sobject, [])
        return [f for f in fields if f.lower() in queryable_fields]

    async def _yield_non_bulk_query_pages(self, soql_query):
        """loops through query response pages and yields lists of records"""
        url = f"{self.base_url}{QUERY_ENDPOINT}"
        params = {"q": soql_query}

        while True:
            response = await self._get_json(
                url,
                params=params,
            )
            yield response.get("records")
            if not response.get("nextRecordsUrl"):
                break

            url = f"{self.base_url}{response.get('nextRecordsUrl')}"
            params = None

    async def _execute_non_paginated_query(self, soql_query):
        """For quick queries, ignores pagination"""
        url = f"{self.base_url}{QUERY_ENDPOINT}"
        params = {"q": soql_query}
        response = await self._get_json(
            url,
            params=params,
        )
        return response.get("records")

    async def _auth_headers(self):
        token = await self.api_token.token()
        return {"authorization": f"Bearer {token}"}

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        skipped_exceptions=[RateLimitedException, InvalidQueryException],
    )
    async def _get_json(self, url, params=None):
        response_body = None
        try:
            response = await self._get(url, params=params)
            response_body = await response.json()
            # We get the response body before raising for status as it contains vital error information
            response.raise_for_status()
            return response_body
        except ClientResponseError as e:
            await self._handle_client_response_error(response_body, e)
        except Exception as e:
            raise e

    async def _get(self, url, params=None):
        self._logger.debug(f"Sending request. Url: {url}, params: {params}")
        headers = await self._auth_headers()
        return await self.session.get(
            url,
            headers=headers,
            params=params,
        )

    async def _download(self, content_version_id):
        endpoint = CONTENT_VERSION_DOWNLOAD_ENDPOINT.replace(
            "<content_version_id>", content_version_id
        )
        response = await self._get(f"{self.base_url}{endpoint}")
        yield response

    async def _handle_client_response_error(self, response_body, e):
        exception_details = f"status: {e.status}, message: {e.message}"

        if e.status == 401:
            self._logger.warning(
                f"Token expired, attemping to fetch new token. Status: {e.status}, message: {e.message}"
            )
            # The user can alter the lifetime of issued tokens, so we don't know when they expire
            # By clearing the bearer token, we force the auth headers to fetch a new token in the next request
            self.api_token.clear()
            # raise to continue with retry strategy
            raise e
        elif 400 <= e.status < 500:
            errors = self._handle_response_body_error(response_body)
            # response format is an array for some reason so we check all of the error codes
            # errorCode and message are generally identical, except if the query is invalid
            error_codes = [x["errorCode"] for x in errors]

            if "REQUEST_LIMIT_EXCEEDED" in error_codes:
                raise RateLimitedException(
                    f"Salesforce is rate limiting this account. {exception_details}, details: {', '.join(error_codes)}"
                ) from e
            elif any(
                error in error_codes
                for error in [
                    "INVALID_FIELD",
                    "INVALID_TERM",
                    "MALFORMED_QUERY",
                ]
            ):
                raise InvalidQueryException(
                    f"The query was rejected by Salesforce. {exception_details}, details: {', '.join(error_codes)}, query: {', '.join([x['message'] for x in errors])}"
                ) from e
            else:
                raise ConnectorRequestError(
                    f"The request to Salesforce failed. {exception_details}, details: {', '.join(error_codes)}"
                ) from e
        else:
            raise SalesforceServerError(
                f"Salesforce experienced an internal server error. {exception_details}."
            )

    def _handle_response_body_error(self, error_list):
        if error_list is None or len(error_list) < 1:
            return [{"errorCode": "unknown"}]

        return error_list

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

        doc_links_join = await self.content_document_links_join()
        opportunities_join = None
        if await self._is_queryable("Opportunity"):
            queryable_join_fields = await self._select_queryable_fields(
                "Opportunity",
                [
                    "Name",
                    "StageName",
                ],
            )
            opportunities_join = (
                SalesforceSoqlBuilder("Opportunities")
                .with_id()
                .with_fields(queryable_join_fields)
                .with_order_by("CreatedDate DESC")
                .with_limit(1)
                .build()
            )

        return (
            SalesforceSoqlBuilder("Account")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])
            .with_fields(["Parent.Id", "Parent.Name"])
            .with_join(opportunities_join)
            .with_join(doc_links_join)
            .build()
        )

    async def _opportunities_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Opportunity",
            [
                "Name",
                "Description",
                "StageName",
            ],
        )

        doc_links_join = await self.content_document_links_join()
        return (
            SalesforceSoqlBuilder("Opportunity")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])
            .with_join(doc_links_join)
            .build()
        )

    async def _contacts_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Contact",
            [
                "Name",
                "Description",
                "Email",
                "Phone",
                "Title",
                "PhotoUrl",
                "LeadSource",
                "AccountId",
                "OwnerId",
            ],
        )
        doc_links_join = await self.content_document_links_join()
        return (
            SalesforceSoqlBuilder("Contact")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_join(doc_links_join)
            .build()
        )

    async def _leads_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Lead",
            [
                "Company",
                "ConvertedAccountId",
                "ConvertedContactId",
                "ConvertedDate",
                "ConvertedOpportunityId",
                "Description",
                "Email",
                "LeadSource",
                "Name",
                "OwnerId",
                "Phone",
                "PhotoUrl",
                "Rating",
                "Status",
                "Title",
            ],
        )
        doc_links_join = await self.content_document_links_join()
        return (
            SalesforceSoqlBuilder("Lead")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_join(doc_links_join)
            .build()
        )

    async def _campaigns_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Campaign",
            [
                "Name",
                "IsActive",
                "Type",
                "Description",
                "Status",
                "StartDate",
                "EndDate",
            ],
        )
        doc_links_join = await self.content_document_links_join()
        return (
            SalesforceSoqlBuilder("Campaign")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])
            .with_fields(["Parent.Id", "Parent.Name"])
            .with_join(doc_links_join)
            .build()
        )

    async def _cases_query(self):
        queryable_fields = await self._select_queryable_fields(
            "Case",
            [
                "Subject",
                "Description",
                "CaseNumber",
                "Status",
                "AccountId",
                "ParentId",
                "IsClosed",
                "IsDeleted",
            ],
        )

        email_mesasges_join = await self._email_messages_join_query()
        case_comments_join = await self._case_comments_join_query()
        doc_links_join = await self.content_document_links_join()

        return (
            SalesforceSoqlBuilder("Case")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["Owner.Id", "Owner.Name", "Owner.Email"])
            .with_fields(["CreatedBy.Id", "CreatedBy.Name", "CreatedBy.Email"])
            .with_join(email_mesasges_join)
            .with_join(case_comments_join)
            .with_join(doc_links_join)
            .build()
        )

    async def _email_messages_join_query(self):
        """For join with Case"""
        queryable_fields = await self._select_queryable_fields(
            "EmailMessage",
            [
                "ParentId",
                "MessageDate",
                "LastModifiedById",
                "TextBody",
                "Subject",
                "FromName",
                "FromAddress",
                "ToAddress",
                "CcAddress",
                "BccAddress",
                "Status",
                "IsDeleted",
                "FirstOpenedDate",
            ],
        )

        return (
            SalesforceSoqlBuilder("EmailMessages")
            .with_id()
            .with_fields(queryable_fields)
            .with_fields(["CreatedBy.Id", "CreatedBy.Name", "CreatedBy.Email"])
            .with_limit(500)
            .build()
        )

    async def _case_comments_join_query(self):
        """For join with Case"""
        queryable_fields = await self._select_queryable_fields(
            "CaseComment",
            [
                "ParentId",
                "CommentBody",
                "LastModifiedById",
            ],
        )

        return (
            SalesforceSoqlBuilder("CaseComments")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["CreatedBy.Id", "CreatedBy.Name", "CreatedBy.Email"])
            .with_limit(500)
            .build()
        )

    async def _case_feeds_query(self, case_ids):
        queryable_fields = await self._select_queryable_fields(
            "CaseFeed",
            [
                "ParentId",
                "Type",
                "IsDeleted",
                "CommentCount",
                "Title",
                "Body",
                "LinkUrl",
            ],
        )
        where_in_clause = ",".join(f"'{x}'" for x in case_ids)
        join_clause = await self._case_feed_comments_join()

        return (
            SalesforceSoqlBuilder("CaseFeed")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .with_fields(["CreatedBy.Id", "CreatedBy.Name", "CreatedBy.Email"])
            .with_join(join_clause)
            .with_where(f"ParentId IN ({where_in_clause})")
            .build()
        )

    async def _case_feed_comments_join(self):
        queryable_fields = await self._select_queryable_fields(
            "FeedComment",
            [
                "ParentId",
                "CreatedDate",
                "LastEditById",
                "LastEditDate",
                "CommentBody",
                "IsDeleted",
                "StatusParentId",
            ],
        )
        return (
            SalesforceSoqlBuilder("FeedComments")
            .with_id()
            .with_fields(queryable_fields)
            .with_fields(["CreatedBy.Id", "CreatedBy.Name", "CreatedBy.Email"])
            .with_limit(500)
            .build()
        )

    async def content_document_links_join(self):
        """Cached async property for getting downloadable attached files
        This join is identical for all SObject queries"""
        if self._content_document_links_join is not None:
            return self._content_document_links_join

        links_queryable = await self._is_queryable("ContentDocumentLink")
        docs_queryable = await self._is_queryable("ContentDocument")
        versions_queryable = await self._is_queryable("ContentVersion")
        if not all([links_queryable, docs_queryable, versions_queryable]):
            self._logger.warning(
                "ContentDocuments, ContentVersions, or ContentDocumentLinks were not queryable, so not including in any queries."
            )
            self._content_document_links_join = ""
            return self._content_document_links_join

        queryable_docs_fields = await self._select_queryable_fields(
            "ContentDocument",
            [
                "Title",
                "FileExtension",
                "ContentSize",
                "Description",
            ],
        )
        queryable_version_fields = await self._select_queryable_fields(
            "ContentVersion",
            [
                "VersionDataUrl",
                "VersionNumber",
            ],
        )
        queryable_docs_fields = [f"ContentDocument.{x}" for x in queryable_docs_fields]
        queryable_version_fields = [
            f"ContentDocument.LatestPublishedVersion.{x}"
            for x in queryable_version_fields
        ]
        where_in_clause = ",".join([f"'{x[1:]}'" for x in TIKA_SUPPORTED_FILETYPES])

        self._content_document_links_join = (
            SalesforceSoqlBuilder("ContentDocumentLinks")
            .with_fields(queryable_docs_fields)
            .with_fields(queryable_version_fields)
            .with_fields(
                [
                    "ContentDocument.Id",
                    "ContentDocument.LatestPublishedVersion.Id",
                    "ContentDocument.CreatedDate",
                    "ContentDocument.LastModifiedDate",
                    "ContentDocument.LatestPublishedVersion.CreatedDate",
                ]
            )
            .with_fields(
                [
                    "ContentDocument.Owner.Id",
                    "ContentDocument.Owner.Name",
                    "ContentDocument.Owner.Email",
                ]
            )
            .with_fields(
                [
                    "ContentDocument.CreatedBy.Id",
                    "ContentDocument.CreatedBy.Name",
                    "ContentDocument.CreatedBy.Email",
                ]
            )
            .with_where(f"ContentDocument.FileExtension IN ({where_in_clause})")
            .build()
        )

        return self._content_document_links_join


class SalesforceAPIToken:
    def __init__(self, session, base_url, client_id, client_secret):
        self._token = None
        self.session = session
        self.url = f"{base_url}{TOKEN_ENDPOINT}"
        self.token_payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        skipped_exceptions=[InvalidCredentialsException],
    )
    async def token(self):
        if self._token:
            return self._token

        response_body = {}
        try:
            response = await self.session.post(self.url, data=self.token_payload)
            response_body = await response.json()
            response.raise_for_status()
            self._token = response_body["access_token"]
            return self._token
        except ClientResponseError as e:
            if 400 <= e.status < 500:
                # 400s have detailed error messages in body
                error_message = response_body.get("error", "No error dscription found.")
                if error_message == "invalid_client":
                    raise InvalidCredentialsException(
                        f"The `client_id` and `client_secret` provided could not be used to generate a token. Status: {e.status}, message: {e.message}, details: {error_message}"
                    ) from e
                else:
                    raise TokenFetchException(
                        f"Could not fetch token from Salesforce: Status: {e.status}, message: {e.message}, details: {error_message}"
                    ) from e
            else:
                raise TokenFetchException(
                    f"Unexpected error while fetching Salesforce token. Status: {e.status}, message: {e.message}"
                ) from e

    def clear(self):
        self._token = None


class SalesforceSoqlBuilder:
    def __init__(self, table):
        self.table_name = table
        self.fields = []
        self.where = ""
        self.order_by = ""
        self.limit = ""

    def with_id(self):
        self.fields.append("Id")
        return self

    def with_default_metafields(self):
        self.fields.extend(["CreatedDate", "LastModifiedDate"])
        return self

    def with_fields(self, fields):
        self.fields.extend(fields)
        return self

    def with_where(self, where_string):
        self.where = f"WHERE {where_string}"
        return self

    def with_order_by(self, order_by_string):
        self.order_by = f"ORDER BY {order_by_string}"
        return self

    def with_limit(self, limit):
        self.limit = f"LIMIT {limit}"
        return self

    def with_join(self, join):
        if join:
            self.fields.append(f"(\n{join})\n")

        return self

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
        owner = account.get("Owner", {}) or {}

        opportunities = account.get("Opportunities", {}) or {}
        opportunity_records = opportunities.get("records", []) if opportunities else []
        opportunity = opportunity_records[0] if len(opportunity_records) > 0 else {}
        opportunity_url = (
            f"{self.base_url}/{opportunity.get('Id')}" if opportunity else None
        )
        opportunity_status = opportunity.get("StageName", None)

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
        owner = opportunity.get("Owner", {}) or {}

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

    def map_contact(self, contact):
        account = contact.get("Account", {}) or {}
        account_id = account.get("Id")
        account_url = f"{self.base_url}/{account_id}" if account_id else None

        owner = contact.get("Owner", {}) or {}
        owner_id = owner.get("Id")
        owner_url = f"{self.base_url}/{owner_id}" if owner_id else None

        photo_url = contact.get("PhotoUrl")
        thumbnail = f"{self.base_url}{photo_url}" if photo_url else None

        return {
            "_id": contact.get("Id"),
            "account": account.get("Name"),
            "account_url": account_url,
            "body": contact.get("Description"),
            "created_at": contact.get("CreatedDate"),
            "email": contact.get("Email"),
            "job_title": contact.get("Title"),
            "last_updated": contact.get("LastModifiedDate"),
            "lead_source": contact.get("LeadSource"),
            "owner": owner.get("Name"),
            "owner_url": owner_url,
            "phone": contact.get("Phone"),
            "source": "salesforce",
            "thumbnail": thumbnail,
            "title": contact.get("Name"),
            "type": "contact",
            "url": f"{self.base_url}/{contact.get('Id')}",
        }

    def map_lead(self, lead):
        owner = lead.get("Owner", {}) or {}
        owner_id = owner.get("Id")
        owner_url = f"{self.base_url}/{owner_id}" if owner_id else ""

        converted_account = lead.get("ConvertedAccount", {}) or {}
        converted_account_id = converted_account.get("Id")
        converted_account_url = (
            f"{self.base_url}/{converted_account_id}" if converted_account_id else None
        )

        converted_contact = lead.get("ConvertedContact", {}) or {}
        converted_contact_id = converted_account.get("Id")
        converted_contact_url = (
            f"{self.base_url}/{converted_contact_id}" if converted_contact_id else None
        )

        converted_opportunity = lead.get("ConvertedOpportunity", {}) or {}
        converted_opportunity_id = converted_opportunity.get("Id")
        converted_opportunity_url = (
            f"{self.base_url}/{converted_opportunity_id}"
            if converted_opportunity_id
            else None
        )

        photo_url = lead.get("PhotoUrl")
        thumbnail = f"{self.base_url}{photo_url}" if photo_url else None

        return {
            "_id": lead.get("Id"),
            "body": lead.get("Description"),
            "company": lead.get("Company"),
            "converted_account": converted_account.get("Name"),
            "converted_account_url": converted_account_url,
            "converted_at": lead.get("ConvertedDate"),  # TODO convert
            "converted_contact": converted_contact.get("Name"),
            "converted_contact_url": converted_contact_url,
            "converted_opportunity": converted_opportunity.get("Name"),
            "converted_opportunity_url": converted_opportunity_url,
            "created_at": lead.get("CreatedDate"),
            "email": lead.get("Email"),
            "job_title": lead.get("Title"),
            "last_updated": lead.get("LastModifiedDate"),
            "lead_source": lead.get("LeadSource"),
            "owner": owner.get("Name"),
            "owner_url": owner_url,
            "phone": lead.get("Phone"),
            "rating": lead.get("Rating"),
            "source": "salesforce",
            "status": lead.get("Status"),
            "title": lead.get("Name"),
            "thumbnail": thumbnail,
            "type": "lead",
            "url": f"{self.base_url}/{lead.get('Id')}",
        }

    def map_campaign(self, campaign):
        owner = campaign.get("Owner", {}) or {}

        parent = campaign.get("Parent", {}) or {}
        parent_id = parent.get("Id")
        parent_url = f"{self.base_url}/{parent_id}" if parent_id else None

        is_active = campaign.get("IsActive")
        state = (
            ("active" if is_active else "archived") if is_active is not None else None
        )

        return {
            "_id": campaign.get("Id"),
            "body": campaign.get("Description"),
            "campaign_type": campaign.get("Type"),
            "created_at": campaign.get("CreatedDate"),
            "end_date": campaign.get("EndDate"),
            "last_updated": campaign.get("LastModifiedDate"),
            "owner": owner.get("Name"),
            "owner_email": owner.get("Email"),
            "parent": parent.get("Name"),
            "parent_url": parent_url,
            "source": "salesforce",
            "start_date": campaign.get("StartDate"),
            "status": campaign.get("Status"),
            "state": state,
            "title": campaign.get("Name"),
            "type": "campaign",
            "url": f"{self.base_url}/{campaign.get('Id')}",
        }

    def map_case(self, case):
        owner = case.get("Owner", {}) or {}

        created_by = case.get("CreatedBy", {}) or {}

        (
            participant_ids,
            participant_emails,
            participant_names,
        ) = self._collect_case_participant_ids_emails_and_names(case)

        return {
            "_id": case.get("Id"),
            "account_id": case.get("AccountId"),
            "created_at": case.get("CreatedDate"),
            "created_by": created_by.get("Name"),
            "created_by_email": created_by.get("Email"),
            "body": self._format_case_body(case),
            "case_number": case.get("CaseNumber"),
            "is_closed": case.get("IsClosed"),
            "last_updated": case.get("LastModifiedDate"),
            "owner": owner.get("Name"),
            "owner_email": owner.get("Email"),
            "participant_emails": participant_emails,
            "participant_ids": participant_ids,
            "participants": participant_names,
            "source": "salesforce",
            "status": case.get("Status"),
            "title": case.get("Subject"),
            "type": "case",
            "url": f"{self.base_url}/{case.get('Id')}",
        }

    def map_content_document(self, content_document):
        content_version = content_document.get("LatestPublishedVersion", {}) or {}
        owner = content_document.get("Owner", {}) or {}
        created_by = content_document.get("CreatedBy", {}) or {}

        return {
            "_id": content_document.get("Id"),
            "content_size": content_document.get("ContentSize"),
            "created_at": content_document.get("CreatedDate"),
            "created_by": created_by.get("Name"),
            "created_by_email": created_by.get("Email"),
            "description": content_document.get("Description"),
            "file_extension": content_document.get("FileExtension"),
            "last_updated": content_document.get("LastModifiedDate"),
            "linked_ids": sorted(content_document.get("linked_ids")),
            "owner": owner.get("Name"),
            "owner_email": owner.get("Email"),
            "title": f"{content_document.get('Title')}.{content_document.get('FileExtension')}",
            "type": "content_document",
            "url": f"{self.base_url}/{content_document.get('Id')}",
            "version_number": content_version.get("VersionNumber"),
            "version_url": f"{self.base_url}/{content_version.get('Id')}",
        }

    def _format_address(self, address):
        if not address:
            return None

        address_fields = [
            address.get("street"),
            address.get("city"),
            address.get("state"),
            str(address.get("postalCode", "")),
            address.get("country"),
        ]
        return ", ".join([a for a in address_fields if a])

    def _format_case_body(self, case):
        time_body_pairs = []
        time_body_pairs.append([case.get("CreatedDate"), case.get("Description")])

        case_comments = case.get("CaseComments", {}) or {}
        for comment in case_comments.get("records", []):
            time_body_pairs.append(
                [comment.get("CreatedDate"), comment.get("CommentBody")]
            )

        email_messages = case.get("EmailMessages", {}) or {}
        for email in email_messages.get("records", []):
            subject = email.get("Subject", "") or ""
            text_body = email.get("TextBody", "") or ""
            time_body_pairs.append(
                [email.get("MessageDate"), f"{subject}\n{text_body}"]
            )

        case_feeds = case.get("Feeds", []) or []
        for feed in case_feeds:
            title = feed.get("Title", "") or ""
            body = feed.get("Body", "") or ""
            time_body_pairs.append([feed.get("CreatedDate"), f"{title}\n{body}"])

            feed_comments = feed.get("FeedComments", {}) or {}
            for comment in feed_comments.get("records", []):
                time_body_pairs.append(
                    [comment.get("CreatedDate"), comment.get("CommentBody")]
                )

        # sort the body values by their associated timestamp
        time_body_pairs = sorted(
            time_body_pairs, key=lambda x: "" if x[0] is None else x[0]
        )

        # ensure string, remove Nones, and remove whitespace
        bodies = [str(x[-1]).strip() for x in time_body_pairs if x[-1] is not None]

        # finally filter out any empty strings, join and return
        return "\n\n".join(filter(None, bodies))

    def _collect_case_participant_ids_emails_and_names(self, case):
        ids = []
        emails = []
        names = []

        participants = [
            case.get("Owner", {}) or {},
            case.get("CreatedBy", {}) or {},
        ]

        case_comments = case.get("CaseComments", {}) or {}
        participants.extend(self._collect_created_by(case_comments))

        email_messages = case.get("EmailMessages", {}) or {}
        participants.extend(self._collect_created_by(email_messages))

        for email in email_messages.get("records", []):
            names.append(email.get("FromName"))
            emails.append(email.get("FromAddress"))
            emails.extend(str(email.get("ToAddress", "")).split(";"))
            emails.extend(str(email.get("CcAddress", "")).split(";"))
            emails.extend(str(email.get("BccAddress", "")).split(";"))

        feeds = case.get("Feeds", []) or []
        for feed in feeds:
            participants.append(feed.get("CreatedBy", {}))

            feed_comments = feed.get("FeedComments", {}) or {}
            participants.extend(self._collect_created_by(feed_comments))

        for participant in participants:
            ids.append(participant.get("Id"))
            emails.append(participant.get("Email"))
            names.append(participant.get("Name"))

        ids = self._format_list(ids)
        emails = self._format_list(emails)
        names = self._format_list(names)

        return ids, emails, names

    def _collect_created_by(self, sobject):
        created_by_list = []

        records = sobject.get("records", [])
        for record in records:
            created_by_list.append(record.get("CreatedBy"))

        return created_by_list

    def _format_list(self, unformatted):
        return sorted(
            set(filter(None, [str(x).strip() for x in unformatted if x is not None]))
        )


class SalesforceDataSource(BaseDataSource):
    """Salesforce"""

    name = "Salesforce"
    service_type = "salesforce"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        base_url = (
            SALESFORCE_EMULATOR_HOST
            if (RUNNING_FTEST and SALESFORCE_EMULATOR_HOST)
            else BASE_URL.replace("<domain>", configuration["domain"])
        )
        self.salesforce_client = SalesforceClient(
            configuration=configuration, base_url=base_url
        )
        self.doc_mapper = SalesforceDocMapper(base_url)

    def _set_internal_logger(self):
        self.salesforce_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "domain": {
                "label": "Domain",
                "order": 1,
                "tooltip": "The domain for your Salesforce instance. If your Salesforce URL is 'foo.my.salesforce.com', the domain would be 'foo'.",
                "type": "str",
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "sensitive": True,
                "tooltip": "The client id for your OAuth2-enabled connected app. Also called 'consumer key'",
                "type": "str",
            },
            "client_secret": {
                "label": "Client Secret",
                "order": 3,
                "sensitive": True,
                "tooltip": "The client secret for your OAuth2-enabled connected app. Also called 'consumer secret'",
                "type": "str",
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
        }

    async def validate_config(self):
        await super().validate_config()

    async def close(self):
        await self.salesforce_client.close()

    async def ping(self):
        try:
            await self.salesforce_client.ping()
            self._logger.debug("Successfully connected to Salesforce.")
        except Exception as e:
            self._logger.exception(f"Error while connecting to Salesforce: {e}")
            raise

    async def get_docs(self, filtering=None):
        # We collect all content documents and de-duplicate them before downloading and yielding
        content_docs = []

        async for account in self.salesforce_client.get_accounts():
            content_docs.extend(self._parse_content_documents(account))
            yield self.doc_mapper.map_account(account), None

        async for opportunity in self.salesforce_client.get_opportunities():
            content_docs.extend(self._parse_content_documents(opportunity))
            yield self.doc_mapper.map_opportunity(opportunity), None

        async for contact in self.salesforce_client.get_contacts():
            content_docs.extend(self._parse_content_documents(contact))
            yield self.doc_mapper.map_contact(contact), None

        async for lead in self.salesforce_client.get_leads():
            content_docs.extend(self._parse_content_documents(lead))
            yield self.doc_mapper.map_lead(lead), None

        async for campaign in self.salesforce_client.get_campaigns():
            content_docs.extend(self._parse_content_documents(campaign))
            yield self.doc_mapper.map_campaign(campaign), None

        async for case in self.salesforce_client.get_cases():
            content_docs.extend(self._parse_content_documents(case))
            yield self.doc_mapper.map_case(case), None

        # Note: this could possibly be done on the fly if memory becomes an issue
        content_docs = self._combine_duplicate_content_docs(content_docs)
        for content_doc in content_docs:
            content_version_id = (
                content_doc.get("LatestPublishedVersion", {}) or {}
            ).get("Id")
            if not content_version_id:
                self._logger.debug(
                    f"Couldn't find the latest content version for {content_doc.get('Title')}, skipping."
                )
                continue

            doc = self.doc_mapper.map_content_document(content_doc)
            doc = await self.get_content(doc, content_version_id)

            yield doc, None

    async def get_content(self, doc, content_version_id):
        file_size = doc["content_size"]
        filename = doc["title"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        return await self.download_and_extract_file(
            doc,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.salesforce_client._download,
                    content_version_id,
                ),
            ),
            return_doc_if_failed=True,  # we still ingest on download failure for Salesforce
        )

    def _parse_content_documents(self, record):
        content_docs = []
        content_links = record.get("ContentDocumentLinks", {}) or {}
        content_links = content_links.get("records", []) or []
        for content_link in content_links:
            content_doc = content_link.get("ContentDocument")
            if not content_doc:
                continue

            content_doc["linked_sobject_id"] = record.get("Id")
            content_docs.append(content_doc)

        return content_docs

    def _combine_duplicate_content_docs(self, content_docs):
        """Duplicate ContentDocuments may appear linked to multiple SObjects
        Here we ensure that we don't download any duplicates while retaining links"""
        grouped = {}

        for content_doc in content_docs:
            content_doc_id = content_doc["Id"]
            if content_doc_id in grouped:
                linked_id = content_doc["linked_sobject_id"]
                linked_ids = grouped[content_doc_id]["linked_ids"]
                if linked_id not in linked_ids:
                    linked_ids.append(linked_id)
            else:
                grouped[content_doc_id] = content_doc
                grouped[content_doc_id]["linked_ids"] = [
                    content_doc["linked_sobject_id"]
                ]
                # the id is now in the list of linked_ids so we can delete it
                del grouped[content_doc_id]["linked_sobject_id"]

        grouped_objects = list(grouped.values())
        return grouped_objects
