#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Salesforce source module responsible to fetch documents from Salesforce."""
import os
import re
from datetime import datetime
from functools import cached_property, partial
from itertools import groupby

import aiohttp
import fastjsonschema
from aiohttp.client_exceptions import ClientResponseError

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
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    RetryStrategy,
    iso_utc,
    retryable,
)

SALESFORCE_EMULATOR_HOST = os.environ.get("SALESFORCE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

RETRIES = 3
RETRY_INTERVAL = 1

BASE_URL = "https://<domain>.my.salesforce.com"
API_VERSION = "v59.0"
TOKEN_ENDPOINT = "/services/oauth2/token"  # noqa S105
QUERY_ENDPOINT = f"/services/data/{API_VERSION}/query"
SOSL_SEARCH_ENDPOINT = f"/services/data/{API_VERSION}/search"
DESCRIBE_ENDPOINT = f"/services/data/{API_VERSION}/sobjects"
DESCRIBE_SOBJECT_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/<sobject>/describe"
# https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_blob_retrieve.htm
CONTENT_VERSION_DOWNLOAD_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/ContentVersion/<content_version_id>/VersionData"
OFFSET = 200

OBJECT_READ_PERMISSION_USERS = "SELECT AssigneeId FROM PermissionSetAssignment WHERE PermissionSetId IN (SELECT ParentId FROM ObjectPermissions WHERE PermissionsRead = true AND SObjectType = '{sobject}')"
USERNAME_FROM_IDS = "SELECT Name, Email FROM User WHERE Id IN {user_list}"
FILE_ACCESS = "SELECT ContentDocumentId, LinkedEntityId, LinkedEntity.Name FROM ContentDocumentLink WHERE ContentDocumentId = '{document_id}'"

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
    "UserType",
]


def _prefix_user(user):
    if user:
        return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


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
        self._queryable_sobject_fields = {}
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

    def modify_soql_query(self, query):
        lowered_query = query.lower()
        match_limit = re.search(r"(?i)(.*)FROM\s+(.*?)(?:LIMIT)(.*)", lowered_query)
        match_offset = re.search(r"(?i)(.*)FROM\s+(.*?)(?:OFFSET)(.*)", lowered_query)

        if "fields" in lowered_query and not match_limit:
            query += " LIMIT 200 OFFSET 0"

        elif "fields" in lowered_query and match_limit and not match_offset:
            query += " OFFSET 0"

        elif "fields" in lowered_query and match_limit and match_offset:
            return query

        return query

    def _add_last_modified_date(self, query):
        lowered_query = query.lower()
        if (
            not ("fields(all)" in lowered_query or "fields(standard)" in lowered_query)
            and "lastmodifieddate" not in lowered_query
        ):
            query = re.sub(
                r"(?i)SELECT (.*) FROM", r"SELECT \1, LastModifiedDate FROM", query
            )

        return query

    def _add_id(self, query):
        lowered_query = query.lower()
        if not (
            "fields(all)" in lowered_query or "fields(standard)" in lowered_query
        ) and not re.search(r"\bid\b", lowered_query):
            query = re.sub(r"(?i)SELECT (.*) FROM", r"SELECT \1, Id FROM", query)

        return query

    async def get_sync_rules_results(self, rule):
        if rule["language"] == "SOQL":
            query_with_id = self._add_id(query=rule["query"])
            query = self._add_last_modified_date(query=query_with_id)

            if "fields" not in query.lower():
                async for records in self._yield_non_bulk_query_pages(soql_query=query):
                    for record in records:
                        yield record
            # If FIELDS function is present in SOQL query, LIMIT/OFFSET is used for pagination
            else:
                soql_query = self.modify_soql_query(query=query)
                async for records in self._yield_soql_query_pages_with_fields_function(
                    soql_query=soql_query
                ):
                    for record in records:
                        yield record

        else:
            async for records in self._yield_sosl_query_pages(sosl_query=rule["query"]):
                for record in records:
                    yield record

    async def _custom_objects(self):
        response = await self._get_json(f"{self.base_url}{DESCRIBE_ENDPOINT}")
        custom_objects = []

        for sobject in response.get("sobjects", []):
            if sobject.get("custom") and sobject.get("name")[-3:] == "__c":
                custom_objects.append(sobject.get("name"))
        return custom_objects

    async def get_custom_objects(self):
        for custom_object in await self._custom_objects():
            query = await self._custom_object_query(custom_object=custom_object)
            async for records in self._yield_non_bulk_query_pages(query):
                for record in records:
                    yield record

    async def get_salesforce_users(self):
        if not await self._is_queryable("User"):
            self._logger.warning(
                "Object User is not queryable, so they won't be ingested."
            )
            return

        query = await self._user_query()
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_users_with_read_access(self, sobject):
        query = OBJECT_READ_PERMISSION_USERS.format(sobject=sobject)
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_username_by_id(self, user_list):
        query = USERNAME_FROM_IDS.format(user_list=user_list)
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

    async def get_file_access(self, document_id):
        query = FILE_ACCESS.format(document_id=document_id)
        async for records in self._yield_non_bulk_query_pages(query):
            for record in records:
                yield record

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
                all_case_ids = [x.get("Id") for x in records]
                case_ids_list = [
                    all_case_ids[i : i + 800] for i in range(0, len(all_case_ids), 800)
                ]

                all_case_feeds = []
                for case_ids in case_ids_list:
                    case_feeds = await self.get_case_feeds(case_ids)
                    all_case_feeds.extend(case_feeds)

                # groupby requires pre-sorting apparently
                all_case_feeds.sort(key=lambda x: x.get("ParentId", ""))
                case_feeds_by_case_id = {
                    k: list(feeds)
                    for k, feeds in groupby(
                        all_case_feeds, key=lambda x: x.get("ParentId", "")
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

    async def queryable_sobject_fields(
        self,
        relevant_objects,
        relevant_sobject_fields,
    ):
        """Cached async property"""
        for sobject in relevant_objects:
            endpoint = DESCRIBE_SOBJECT_ENDPOINT.replace("<sobject>", sobject)
            response = await self._get_json(f"{self.base_url}{endpoint}")

            if relevant_sobject_fields is None:
                queryable_fields = [
                    f["name"].lower() for f in response.get("fields", [])
                ]
            else:
                queryable_fields = [
                    f["name"].lower()
                    for f in response.get("fields", [])
                    if f["name"] in relevant_sobject_fields
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
        if sobject not in RELEVANT_SOBJECTS:
            sobject_fields = await self.queryable_sobject_fields(
                relevant_objects=[sobject], relevant_sobject_fields=None
            )
        else:
            sobject_fields = await self.queryable_sobject_fields(
                relevant_objects=RELEVANT_SOBJECTS,
                relevant_sobject_fields=RELEVANT_SOBJECT_FIELDS,
            )
        queryable_fields = sobject_fields.get(sobject, [])
        if fields == []:
            return queryable_fields
        return [f for f in fields if f.lower() in queryable_fields]

    async def _yield_non_bulk_query_pages(self, soql_query, endpoint=QUERY_ENDPOINT):
        """loops through query response pages and yields lists of records"""
        url = f"{self.base_url}{endpoint}"
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

    async def _yield_soql_query_pages_with_fields_function(self, soql_query):
        """loops through SOQL query response pages and yields lists of records"""

        def modify_offset(query, new_offset):
            offset_pattern = r"OFFSET (\d+)"
            new_query = re.sub(offset_pattern, f"OFFSET {new_offset}", query)

            return new_query

        url = f"{self.base_url}{QUERY_ENDPOINT}"
        offset = OFFSET

        while True:
            response = await self._get_json(
                url,
                params={"q": soql_query},
            )
            yield response.get("records", [])

            # Note: we can't set offset more than 2000 if SOQL query contains `FIELDS` function
            if not response.get("records") or offset > 2000:
                break

            soql_query = modify_offset(soql_query, offset)
            offset += OFFSET

    async def _yield_sosl_query_pages(self, sosl_query):
        """loops through SOSL query response pages and yields lists of records"""

        url = f"{self.base_url}{SOSL_SEARCH_ENDPOINT}"
        params = {"q": sosl_query}

        response = await self._get_json(
            url,
            params=params,
        )
        yield response.get("searchRecords", [])

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
                f"Token expired, attempting to fetch new token. Status: {e.status}, message: {e.message}"
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
                msg = f"Salesforce is rate limiting this account. {exception_details}, details: {', '.join(error_codes)}"
                raise RateLimitedException(msg) from e
            elif any(
                error in error_codes
                for error in [
                    "INVALID_FIELD",
                    "INVALID_TERM",
                    "MALFORMED_QUERY",
                    "INVALID_TYPE",
                ]
            ):
                msg = f"The query was rejected by Salesforce. {exception_details}, details: {', '.join(error_codes)}, query: {', '.join([x['message'] for x in errors])}"
                raise InvalidQueryException(msg) from e
            else:
                msg = f"The request to Salesforce failed. {exception_details}, details: {', '.join(error_codes)}"
                raise ConnectorRequestError(msg) from e
        else:
            msg = (
                f"Salesforce experienced an internal server error. {exception_details}."
            )
            raise SalesforceServerError(msg)

    def _handle_response_body_error(self, error_list):
        if error_list is None or len(error_list) < 1:
            return [{"errorCode": "unknown"}]

        return error_list

    async def _custom_object_query(self, custom_object):
        queryable_fields = await self._select_queryable_fields(
            custom_object,
            [],
        )
        doc_links_join = await self.content_document_links_join()
        return (
            SalesforceSoqlBuilder(custom_object)
            .with_fields(queryable_fields)
            .with_join(doc_links_join)
            .build()
        )

    async def _user_query(self):
        queryable_fields = await self._select_queryable_fields(
            "User",
            ["Name", "Email", "UserType"],
        )

        return (
            SalesforceSoqlBuilder("User")
            .with_id()
            .with_default_metafields()
            .with_fields(queryable_fields)
            .build()
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
                error_message = response_body.get("error", "No error message found.")
                error_description = response_body.get("error_description", "No error description found.")
                if error_message == "invalid_client":
                    msg = f"The `client_id` and `client_secret` provided could not be used to generate a token. Status: {e.status}, message: {e.message}, details: {error_message}, description: {error_description}"
                    raise InvalidCredentialsException(msg) from e
                else:
                    msg = f"Could not fetch token from Salesforce: Status: {e.status}, message: {e.message}, details: {error_message}, description: {error_description}"
                    raise TokenFetchException(msg) from e
            else:
                msg = f"Unexpected error while fetching Salesforce token. Status: {e.status}, message: {e.message}"
                raise TokenFetchException(msg) from e

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

    def map_salesforce_objects(self, _object):
        def _format_datetime(datetime_):
            datetime_ = datetime_ or iso_utc()
            return datetime.strptime(datetime_, "%Y-%m-%dT%H:%M:%S.%f%z").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        return {
            "_id": _object.get("Id"),
            "_timestamp": _format_datetime(datetime_=_object.get("LastModifiedDate")),
            "url": f"{self.base_url}/{_object.get('Id')}",
        } | _object


class SalesforceAdvancedRulesValidator(AdvancedRulesValidator):
    OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1},
            "language": {
                "type": "string",
                "minLength": 1,
                "enum": ["SOSL", "SOQL"],
            },
        },
        "required": ["query", "language"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=3,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            SalesforceAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        invalid_queries = []
        for rule in advanced_rules:
            language = rule["language"]
            query = (
                self.source.salesforce_client.modify_soql_query(rule["query"])
                if language == "SOQL"
                else rule["query"]
            )
            try:
                url = (
                    f"{self.source.base_url_}{SOSL_SEARCH_ENDPOINT}"
                    if language == "SOSL"
                    else f"{self.source.base_url_}{QUERY_ENDPOINT}"
                )
                params = {"q": query}
                await self.source.salesforce_client._get_json(
                    url,
                    params=params,
                )
            except InvalidQueryException:
                invalid_queries.append(query)

        if len(invalid_queries) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Found invalid queries: '{' & '.join(invalid_queries)}'. Either object or fields might be invalid.",
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class SalesforceDataSource(BaseDataSource):
    """Salesforce"""

    name = "Salesforce"
    service_type = "salesforce"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        base_url = (
            SALESFORCE_EMULATOR_HOST
            if (RUNNING_FTEST and SALESFORCE_EMULATOR_HOST)
            else BASE_URL.replace("<domain>", configuration["domain"])
        )
        self.base_url_ = base_url
        self.salesforce_client = SalesforceClient(
            configuration=configuration, base_url=base_url
        )
        self.doc_mapper = SalesforceDocMapper(base_url)
        self.permissions = {}

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
                "order": 4,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 5,
                "tooltip": "Document level security ensures identities and permissions set in Salesforce are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _user_access_control_doc(self, user):
        email = user.get("Email")
        username = user.get("Name")

        prefixed_email = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(user.get("Id"))

        access_control = [prefixed_email, prefixed_username, prefixed_user_id]
        return {
            "_id": user.get("Id"),
            "identity": {
                "email": prefixed_email,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "created_at": user.get("CreatedDate", iso_utc()),
            "_timestamp": user.get("LastModifiedDate", iso_utc()),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        """Get access control documents for Salesforce users.

        This method fetches access control documents for Salesforce users when document level security (DLS)
        is enabled. It starts by checking if DLS is enabled, and if not, it logs a warning message and skips further processing.
        If DLS is enabled, the method fetches all users from the Salesforce API, filters out Salesforce users,
        and fetches additional information for each user using the _fetch_user method. After gathering the user information,
        it generates an access control document for each user using the user_access_control_doc method and yields the results.

        Yields:
            dict: An access control document for each Salesforce user.
        """
        if not self._dls_enabled():
            self._logger.debug("DLS is not enabled. Skipping")
            return

        self._logger.debug("Fetching Salesforce users")
        async for user in self.salesforce_client.get_salesforce_users():
            if user.get("UserType") in ["CloudIntegrationUser", "AutomatedProcess"]:
                continue
            user_doc = await self._user_access_control_doc(user=user)
            yield user_doc

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

    def advanced_rules_validators(self):
        return [SalesforceAdvancedRulesValidator(self)]

    async def _get_advanced_sync_rules_result(self, rule):
        async for doc in self.salesforce_client.get_sync_rules_results(rule=rule):
            if sobject := doc.get("attributes", {}).get("type"):
                await self._fetch_users_with_read_access(sobject=sobject)
            yield doc

    async def _fetch_users_with_read_access(self, sobject):
        if not self._dls_enabled():
            self._logger.debug("DLS is not enabled. Skipping")
            return

        self._logger.debug(
            f"Fetching users who have Read access for Salesforce object: {sobject}"
        )

        if sobject in self.permissions:
            return

        user_list = set()
        access_control = set()
        async for assignee in self.salesforce_client.get_users_with_read_access(
            sobject=sobject
        ):
            user_list.add(assignee.get("AssigneeId"))
            access_control.add(_prefix_user_id(assignee.get("AssigneeId")))

        if user_list == set():
            return

        user_sub_list = [
            list(user_list)[i : i + 800] for i in range(0, len(user_list), 800)
        ]
        for _user_list in user_sub_list:
            async for user in self.salesforce_client.get_username_by_id(
                user_list=tuple(_user_list)
            ):
                access_control.add(_prefix_user(user.get("Name")))
                access_control.add(_prefix_email(user.get("Email")))

        self.permissions[sobject] = list(access_control)

    async def get_docs(self, filtering=None):
        # We collect all content documents and de-duplicate them before downloading and yielding
        content_docs = []

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            for rule in advanced_rules:
                async for doc in self._get_advanced_sync_rules_result(rule=rule):
                    content_docs.extend(self._parse_content_documents(doc))
                    access_control = self.permissions.get(
                        doc.get("attributes", {}).get("type"), []
                    )
                    yield self.doc_mapper.map_salesforce_objects(
                        self._decorate_with_access_control(doc, access_control)
                    ), None

        else:
            for sobject in [
                "Account",
                "Opportunity",
                "Contact",
                "Lead",
                "Campaign",
                "Case",
            ]:
                await self._fetch_users_with_read_access(sobject=sobject)

            for custom_object in await self.salesforce_client._custom_objects():
                await self._fetch_users_with_read_access(sobject=custom_object)

            async for account in self.salesforce_client.get_accounts():
                content_docs.extend(self._parse_content_documents(account))
                access_control = self.permissions.get("Account", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(account, access_control)
                ), None

            async for opportunity in self.salesforce_client.get_opportunities():
                content_docs.extend(self._parse_content_documents(opportunity))
                access_control = self.permissions.get("Opportunity", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(opportunity, access_control)
                ), None

            async for contact in self.salesforce_client.get_contacts():
                content_docs.extend(self._parse_content_documents(contact))
                access_control = self.permissions.get("Contact", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(contact, access_control)
                ), None

            async for lead in self.salesforce_client.get_leads():
                content_docs.extend(self._parse_content_documents(lead))
                access_control = self.permissions.get("Lead", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(lead, access_control)
                ), None

            async for campaign in self.salesforce_client.get_campaigns():
                content_docs.extend(self._parse_content_documents(campaign))
                access_control = self.permissions.get("Campaign", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(campaign, access_control)
                ), None

            async for case in self.salesforce_client.get_cases():
                content_docs.extend(self._parse_content_documents(case))
                access_control = self.permissions.get("Case", [])
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(case, access_control)
                ), None

            async for custom_object in self.salesforce_client.get_custom_objects():
                content_docs.extend(self._parse_content_documents(custom_object))
                access_control = self.permissions.get(
                    custom_object.get("attributes", {}).get("type"), []
                )
                yield self.doc_mapper.map_salesforce_objects(
                    self._decorate_with_access_control(custom_object, access_control)
                ), None

        # Note: this could possibly be done on the fly if memory becomes an issue
        content_docs = self._combine_duplicate_content_docs(content_docs)
        for content_doc in content_docs:
            access_control = []
            async for permission in self.salesforce_client.get_file_access(
                document_id=content_doc["Id"]
            ):
                access_control.append(_prefix_user_id(permission.get("LinkedEntityId")))
                access_control.append(
                    _prefix_user(permission.get("LinkedEntity", {}).get("Name"))
                )

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

            yield self._decorate_with_access_control(doc, access_control), None

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
