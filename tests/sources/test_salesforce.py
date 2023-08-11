#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Salesforce source class methods"""
import re
from contextlib import asynccontextmanager
from unittest import TestCase, mock

import pytest
from aiohttp.client_exceptions import ClientConnectionError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.salesforce import (
    RELEVANT_SOBJECT_FIELDS,
    ConnectorRequestError,
    InvalidCredentialsException,
    InvalidQueryException,
    RateLimitedException,
    SalesforceDataSource,
    SalesforceServerError,
    SalesforceSoqlBuilder,
    TokenFetchException,
)
from tests.sources.support import create_source

TEST_DOMAIN = "fake"
TEST_BASE_URL = f"https://{TEST_DOMAIN}.my.salesforce.com"
TEST_CLIENT_ID = "1234"
TEST_CLIENT_SECRET = "9876"

ACCOUNT_RESPONSE_PAYLOAD = {
    "totalSize": 1,
    "done": True,
    "records": [
        {
            "attributes": {
                "type": "Account",
                "url": "/services/data/v58.0/sobjects/Account/account_id",
            },
            "Type": "Customer - Direct",
            "Owner": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Id": "user_id",
                "Name": "Frodo",
                "Email": "frodo@tlotr.com",
            },
            "Id": "account_id",
            "Rating": "Hot",
            "Website": "www.tlotr.com",
            "LastModifiedDate": "",
            "CreatedDate": "",
            "Opportunities": {
                "totalSize": 1,
                "done": True,
                "records": [
                    {
                        "attributes": {
                            "type": "Opportunity",
                            "url": "/services/data/v58.0/sobjects/Opportunity/opportunity_id",
                        },
                        "Id": "opportunity_id",
                        "Name": "The Fellowship",
                        "StageName": "Closed Won",
                    }
                ],
            },
            "Name": "TLOTR",
            "BillingAddress": {
                "city": "The Shire",
                "country": "Middle Earth",
                "postalCode": 111,
                "state": "Eriador",
                "street": "The Burrow under the Hill, Bag End, Hobbiton",
            },
            "Description": "A story about the One Ring.",
        }
    ],
}

OPPORTUNITY_RESPONSE_PAYLOAD = {
    "totalSize": 1,
    "done": True,
    "records": [
        {
            "attributes": {
                "type": "Opportunity",
                "url": "/services/data/v58.0/sobjects/Opportunity/opportunity_id",
            },
            "Description": "A fellowship of the races of Middle Earth",
            "Owner": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Id": "user_id",
                "Email": "frodo@tlotr.com",
                "Name": "Frodo",
            },
            "LastModifiedDate": "",
            "Name": "The Fellowship",
            "StageName": "Closed Won",
            "CreatedDate": "",
            "Id": "opportunity_id",
        },
    ],
}

CONTACT_RESPONSE_PAYLOAD = {
    "records": [
        {
            "attributes": {
                "type": "Contact",
                "url": "/services/data/v58.0/sobjects/Contact/contact_id",
            },
            "OwnerId": "user_id",
            "Phone": "12345678",
            "Name": "Gandalf",
            "AccountId": "account_id",
            "LastModifiedDate": "",
            "Description": "The White",
            "Title": "Wizard",
            "CreatedDate": "",
            "LeadSource": "Partner Referral",
            "PhotoUrl": "/services/images/photo/photo_id",
            "Id": "contact_id",
            "Email": "gandalf@tlotr.com",
        },
    ],
}

LEAD_PAYLOAD = {
    "records": [
        {
            "attributes": {
                "type": "Lead",
                "url": "/services/data/v58.0/sobjects/Lead/lead_id",
            },
            "Name": "Sauron",
            "Status": "Working - Contacted",
            "Company": "Mordor Inc.",
            "Description": "Forger of the One Ring",
            "Email": "sauron@tlotr.com",
            "Phone": "09876543",
            "Title": "Dark Lord",
            "PhotoUrl": "/services/images/photo/photo_id",
            "Rating": "Hot",
            "LastModifiedDate": "",
            "LeadSource": "Partner Referral",
            "OwnerId": "user_id",
            "ConvertedAccountId": None,
            "ConvertedContactId": None,
            "ConvertedOpportunityId": None,
            "ConvertedDate": None,
            "Id": "lead_id",
        }
    ]
}

CAMPAIGN_PAYLOAD = {
    "records": [
        {
            "attributes": {
                "type": "Campaign",
                "url": "/services/data/v58.0/sobjects/Campaign/campaign_id",
            },
            "Name": "Defend the Gap",
            "IsActive": True,
            "Type": "War",
            "Description": "Orcs are raiding the Gap of Rohan",
            "Status": "planned",
            "Id": "campaign_id",
            "Parent": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Id": "user_id",
                "Name": "Théoden",
            },
            "Owner": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Id": "user_id",
                "Name": "Saruman",
                "Email": "saruman@tlotr.com",
            },
            "StartDate": "",
            "EndDate": "",
        }
    ]
}

CASE_PAYLOAD = {
    "records": [
        {
            "attributes": {
                "type": "Case",
                "url": "/services/data/v58.0/sobjects/Case/case_id",
            },
            "Status": "New",
            "AccountId": "account_id",
            "Description": "The One Ring",
            "Subject": "It needs to be destroyed",
            "Owner": {
                "attributes": {
                    "type": "Name",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Email": "frodo@tlotr.com",
                "Name": "Frodo",
                "Id": "user_id",
            },
            "CreatedBy": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id_2",
                },
                "Id": "user_id_2",
                "Email": "gandalf@tlotr.com",
                "Name": "Gandalf",
            },
            "Id": "case_id",
            "EmailMessages": {
                "records": [
                    {
                        "attributes": {
                            "type": "EmailMessage",
                            "url": "/services/data/v58.0/sobjects/EmailMessage/email_message_id",
                        },
                        "CreatedDate": "2023-08-11T00:00:00.000+0000",
                        "LastModifiedById": "user_id",
                        "ParentId": "case_id",
                        "MessageDate": "2023-08-01T00:00:00.000+0000",
                        "TextBody": "Maybe I should do something?",
                        "Subject": "Ring?!",
                        "FromName": "Frodo",
                        "FromAddress": "frodo@tlotr.com",
                        "ToAddress": "gandalf@tlotr.com",
                        "CcAddress": "elrond@tlotr.com",
                        "BccAddress": "samwise@tlotr.com",
                        "Status": "",
                        "IsDeleted": False,
                        "FirstOpenedDate": "2023-08-02T00:00:00.000+0000",
                        "CreatedBy": {
                            "attributes": {
                                "type": "Name",
                                "url": "/services/data/v58.0/sobjects/User/user_id",
                            },
                            "Name": "Frodo",
                            "Id": "user_id",
                            "Email": "frodo@tlotr.com",
                        },
                    }
                ]
            },
            "CaseComments": {
                "records": [
                    {
                        "attributes": {
                            "type": "CaseComment",
                            "url": "/services/data/v58.0/sobjects/CaseComment/case_comment_id",
                        },
                        "CreatedDate": "2023-08-03T00:00:00.000+0000",
                        "LastModifiedById": "user_id_3",
                        "CommentBody": "You have my axe",
                        "LastModifiedDate": "2023-08-03T00:00:00.000+0000",
                        "CreatedBy": {
                            "attributes": {
                                "type": "Name",
                                "url": "/services/data/v58.0/sobjects/User/user_id_3",
                            },
                            "Name": "Gimli",
                            "Id": "user_id_3",
                            "Email": "gimli@tlotr.com",
                        },
                        "ParentId": "case_id",
                        "Id": "case_comment_id",
                    }
                ]
            },
            "CaseNumber": "00001234",
            "ParentId": "",
            "CreatedDate": "2023-08-01T00:00:00.000+0000",
            "IsDeleted": False,
            "IsClosed": False,
            "LastModifiedDate": "2023-08-11T00:00:00.000+0000",
        }
    ]
}

CASE_FEED_PAYLOAD = {
    "records": [
        {
            "attributes": {
                "type": "CaseFeed",
                "url": "/services/data/v58.0/sobjects/CaseFeed/case_feed_id",
            },
            "CreatedBy": {
                "attributes": {
                    "type": "Name",
                    "url": "/services/data/v58.0/sobjects/User/user_id_4",
                },
                "Id": "user_id_4",
                "Email": "galadriel@tlotr.com",
                "Name": "Galadriel",
            },
            "CommentCount": 2,
            "LastModifiedDate": "2023-08-09T00:00:00.000+0000",
            "Type": "TextPost",
            "Title": None,
            "IsDeleted": False,
            "LinkUrl": f"{TEST_BASE_URL}/case_feed_id",
            "CreatedDate": "2023-08-08T00:00:00.000+0000",
            "Id": "case_feed_id",
            "FeedComments": {
                "records": [
                    {
                        "attributes": {
                            "type": "FeedComment",
                            "url": "/services/data/v58.0/sobjects/FeedComment/feed_comment_id",
                        },
                        "CreatedBy": {
                            "attributes": {
                                "type": "Name",
                                "url": "/services/data/v58.0/sobjects/User/user_id_4",
                            },
                            "Id": "user_id_4",
                            "Email": "galadriel@tlotr.com",
                            "Name": "Galadriel",
                        },
                        "IsDeleted": False,
                        "Id": "feed_comment_id",
                        "ParentId": "case_feed_id",
                        "LastEditById": "user_id_4",
                        "LastEditDate": "2023-08-08T00:00:00.000+0000",
                        "CommentBody": "I know what it is you saw",
                    }
                ]
            },
            "ParentId": "case_id",
        }
    ]
}

CACHED_SOBJECTS = {
    "Account": {"account_id": {"Id": "account_id", "Name": "TLOTR"}},
    "User": {
        "user_id": {
            "Id": "user_id",
            "Name": "Frodo",
            "Email": "frodo@tlotr.com",
        }
    },
    "Opportunity": {},
    "Contact": {},
}


@asynccontextmanager
async def create_salesforce_source():
    async with create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    ) as source:
        yield source


def generate_account_doc(identifier):
    return {
        "_id": identifier,
        "account_type": "An account type",
        "address": "Somewhere, Someplace, 1234",
        "body": "A body",
        "content_source_id": identifier,
        "created_at": "",
        "last_updated": "",
        "owner": "Frodo",
        "owner_email": "frodo@tlotr.com",
        "open_activities": "",
        "open_activities_urls": "",
        "opportunity_name": "An opportunity name",
        "opportunity_status": "An opportunity status",
        "opportunity_url": f"{TEST_BASE_URL}/{identifier}",
        "rating": "Hot",
        "source": "salesforce",
        "tags": ["A tag"],
        "title": {identifier},
        "type": "account",
        "url": f"{TEST_BASE_URL}/{identifier}",
        "website_url": "www.tlotr.com",
    }


def test_get_default_configuration():
    config = DataSourceConfiguration(SalesforceDataSource.get_default_configuration())
    expected_fields = ["client_id", "client_secret", "domain"]

    assert all(field in config.to_dict() for field in expected_fields)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "domain, client_id, client_secret",
    [
        ("", TEST_CLIENT_ID, TEST_CLIENT_SECRET),
        (TEST_DOMAIN, "", TEST_CLIENT_SECRET),
        (TEST_DOMAIN, TEST_CLIENT_ID, ""),
    ],
)
async def test_validate_config_missing_fields_then_raise(
    domain, client_id, client_secret
):
    async with create_source(
        SalesforceDataSource,
        domain=domain,
        client_id=client_id,
        client_secret=client_secret,
    ) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection(mock_responses):
    async with create_salesforce_source() as source:
        mock_responses.head(TEST_BASE_URL, status=200)

        await source.ping()


@pytest.mark.asyncio
async def test_generate_token_with_successful_connection(mock_responses):
    async with create_salesforce_source() as source:
        response_payload = {
            "access_token": "foo",
            "signature": "bar",
            "instance_url": "https://fake.my.salesforce.com",
            "id": "https://login.salesforce.com/id/1234",
            "token_type": "Bearer",
        }

        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token",
            status=200,
            payload=response_payload,
        )
        await source.salesforce_client.get_token()

        assert source.salesforce_client.api_token.token() == "foo"


@pytest.mark.asyncio
async def test_generate_token_with_bad_domain_raises_error(
    patch_sleep, mock_responses, patch_cancellable_sleeps
):
    async with create_salesforce_source() as source:
        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token", status=500, repeat=True
        )
        with pytest.raises(TokenFetchException):
            await source.salesforce_client.get_token()


@pytest.mark.asyncio
async def test_generate_token_with_bad_credentials_raises_error(
    patch_sleep, mock_responses, patch_cancellable_sleeps
):
    async with create_salesforce_source() as source:
        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token",
            status=400,
            payload={
                "error": "invalid_client",
                "error_description": "Invalid client credentials",
            },
        )
        with pytest.raises(InvalidCredentialsException):
            await source.salesforce_client.get_token()


@pytest.mark.asyncio
async def test_generate_token_with_unexpected_error_retries(
    patch_sleep, mock_responses, patch_cancellable_sleeps
):
    async with create_salesforce_source() as source:
        response_payload = {
            "access_token": "foo",
            "signature": "bar",
            "instance_url": "https://fake.my.salesforce.com",
            "id": "https://login.salesforce.com/id/1234",
            "token_type": "Bearer",
        }

        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token",
            status=500,
        )
        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token",
            status=200,
            payload=response_payload,
        )

        await source.salesforce_client.get_token()

        assert source.salesforce_client.api_token.token() == "foo"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sobject, expected_result",
    [
        (
            "FooField",
            True,
        ),
        ("ArghField", False),
    ],
)
@mock.patch(
    "connectors.sources.salesforce.RELEVANT_SOBJECTS",
    ["FooField", "BarField", "ArghField"],
)
async def test_get_queryable_sobjects(mock_responses, sobject, expected_result):
    async with create_salesforce_source() as source:
        response_payload = {
            "sobjects": [
                {
                    "queryable": True,
                    "name": "FooField",
                },
                {
                    "queryable": False,
                    "name": "BarField",
                },
            ],
        }

        mock_responses.get(
            f"{TEST_BASE_URL}/services/data/v58.0/sobjects",
            status=200,
            payload=response_payload,
        )

        queryable = await source.salesforce_client._is_queryable(sobject)
        assert queryable == expected_result


@pytest.mark.asyncio
@mock.patch("connectors.sources.salesforce.RELEVANT_SOBJECTS", ["Account"])
@mock.patch(
    "connectors.sources.salesforce.RELEVANT_SOBJECT_FIELDS",
    ["FooField", "BarField", "ArghField"],
)
async def test_get_queryable_fields(mock_responses):
    async with create_salesforce_source() as source:
        expected_fields = [
            {
                "name": "FooField",
            },
            {
                "name": "BarField",
            },
            {"name": "ArghField"},
        ]
        response_payload = {
            "fields": expected_fields,
        }
        mock_responses.get(
            f"{TEST_BASE_URL}/services/data/v58.0/sobjects/Account/describe",
            status=200,
            payload=response_payload,
        )

        queryable_fields = await source.salesforce_client._select_queryable_fields(
            "Account", ["FooField", "BarField", "NarghField"]
        )
        TestCase().assertCountEqual(queryable_fields, ["FooField", "BarField"])


@pytest.mark.asyncio
async def test_get_accounts_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "account_id",
            "account_type": "Customer - Direct",
            "address": "The Burrow under the Hill, Bag End, Hobbiton, The Shire, Eriador, 111, Middle Earth",
            "body": "A story about the One Ring.",
            "content_source_id": "account_id",
            "created_at": "",
            "last_updated": "",
            "owner": "Frodo",
            "owner_email": "frodo@tlotr.com",
            "open_activities": "",
            "open_activities_urls": "",
            "opportunity_name": "The Fellowship",
            "opportunity_status": "Closed Won",
            "opportunity_url": f"{TEST_BASE_URL}/opportunity_id",
            "rating": "Hot",
            "source": "salesforce",
            "tags": ["Customer - Direct"],
            "title": "TLOTR",
            "type": "account",
            "url": f"{TEST_BASE_URL}/account_id",
            "website_url": "www.tlotr.com",
        }

        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=[
                "Name",
                "Description",
                "BillingAddress",
                "Type",
                "Website",
                "Rating",
                "Department",
            ]
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=ACCOUNT_RESPONSE_PAYLOAD,
        )
        async for account in source.salesforce_client.get_accounts():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_get_accounts_when_paginated_yields_all_pages(mock_responses):
    async with create_salesforce_source() as source:
        response_page_1 = {
            "done": False,
            "nextRecordsUrl": f"{TEST_BASE_URL}/barbar",
            "records": [
                {
                    "Id": 1234,
                }
            ],
        }
        response_page_2 = {
            "done": True,
            "records": [
                {
                    "Id": 5678,
                }
            ],
        }

        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=response_page_1,
        )
        mock_responses.get(
            f"{TEST_BASE_URL}/barbar",
            status=200,
            payload=response_page_2,
        )

        yielded_account_ids = []
        async for account in source.salesforce_client.get_accounts():
            yielded_account_ids.append(account["_id"])

        assert sorted(yielded_account_ids) == [1234, 5678]


@pytest.mark.asyncio
async def test_get_accounts_when_invalid_request(patch_sleep, mock_responses):
    async with create_salesforce_source() as source:
        response_payload = [
            {"message": "Unable to process query.", "errorCode": "INVALID_FIELD"}
        ]

        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=405,
            payload=response_payload,
        )
        with pytest.raises(ClientConnectionError):
            async for _ in source.salesforce_client.get_accounts():
                # TODO confirm error message when error handling is improved
                pass


@pytest.mark.asyncio
async def test_get_accounts_when_not_queryable_yields_nothing(mock_responses):
    async with create_salesforce_source() as source:
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=False)
        async for account in source.salesforce_client.get_accounts():
            assert account is None


@pytest.mark.asyncio
async def test_get_opportunities_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "opportunity_id",
            "body": "A fellowship of the races of Middle Earth",
            "content_source_id": "opportunity_id",
            "created_at": "",
            "last_updated": "",
            "next_step": None,
            "owner": "Frodo",
            "owner_email": "frodo@tlotr.com",
            "source": "salesforce",
            "status": "Closed Won",
            "title": "The Fellowship",
            "type": "opportunity",
            "url": f"{TEST_BASE_URL}/opportunity_id",
        }

        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=[
                "Name",
                "Description",
                "StageName",
            ]
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=OPPORTUNITY_RESPONSE_PAYLOAD,
        )
        async for account in source.salesforce_client.get_opportunities():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_get_contacts_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "contact_id",
            "account": "TLOTR",
            "account_url": f"{TEST_BASE_URL}/account_id",
            "body": "The White",
            "created_at": "",
            "email": "gandalf@tlotr.com",
            "job_title": "Wizard",
            "last_updated": "",
            "lead_source": "Partner Referral",
            "owner": "Frodo",
            "owner_url": f"{TEST_BASE_URL}/user_id",
            "phone": "12345678",
            "source": "salesforce",
            "thumbnail": f"{TEST_BASE_URL}/services/images/photo/photo_id",
            "title": "Gandalf",
            "type": "contact",
            "url": f"{TEST_BASE_URL}/contact_id",
        }

        source.salesforce_client.sobjects_cache_by_type = mock.AsyncMock(
            return_value=CACHED_SOBJECTS
        )
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=[
                "Name",
                "Description",
                "Email",
                "Phone",
                "Title",
                "PhotoUrl",
                "LastModifiedDate" "LeadSource",
                "AccountId",
                "OwnerId",
            ]
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=CONTACT_RESPONSE_PAYLOAD,
        )
        async for account in source.salesforce_client.get_contacts():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_get_leads_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "lead_id",
            "body": "Forger of the One Ring",
            "company": "Mordor Inc.",
            "converted_account": None,
            "converted_account_url": None,
            "converted_at": None,
            "converted_contact": None,
            "converted_contact_url": None,
            "converted_opportunity": None,
            "converted_opportunity_url": None,
            "created_at": None,
            "email": "sauron@tlotr.com",
            "job_title": "Dark Lord",
            "last_updated": "",
            "lead_source": "Partner Referral",
            "owner": "Frodo",
            "owner_url": f"{TEST_BASE_URL}/user_id",
            "phone": "09876543",
            "rating": "Hot",
            "source": "salesforce",
            "status": "Working - Contacted",
            "title": "Sauron",
            "thumbnail": f"{TEST_BASE_URL}/services/images/photo/photo_id",
            "type": "lead",
            "url": f"{TEST_BASE_URL}/lead_id",
        }

        source.salesforce_client.sobjects_cache_by_type = mock.AsyncMock(
            return_value=CACHED_SOBJECTS
        )
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=[
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
            ]
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=LEAD_PAYLOAD,
        )
        async for account in source.salesforce_client.get_leads():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_get_campaigns_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "campaign_id",
            "body": "Orcs are raiding the Gap of Rohan",
            "campaign_type": "War",
            "created_at": None,
            "end_date": "",
            "last_updated": None,
            "owner": "Saruman",
            "owner_email": "saruman@tlotr.com",
            "parent": "Théoden",
            "parent_url": f"{TEST_BASE_URL}/user_id",
            "source": "salesforce",
            "start_date": "",
            "status": "planned",
            "state": "active",
            "title": "Defend the Gap",
            "type": "campaign",
            "url": f"{TEST_BASE_URL}/campaign_id",
        }

        source.salesforce_client.sobjects_cache_by_type = mock.AsyncMock(
            return_value=CACHED_SOBJECTS
        )
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=[
                "Name",
                "IsActive",
                "Type",
                "Description",
                "Status",
                "StartDate",
                "EndDate",
            ]
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=CAMPAIGN_PAYLOAD,
        )
        async for account in source.salesforce_client.get_campaigns():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_get_casess_when_success(mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "case_id",
            "account_id": "account_id",
            "body": "I know what it is you saw\n\nThe One Ring\n\nRing?!\nMaybe I should do something?\n\nYou have my axe",
            "created_at": "2023-08-01T00:00:00.000+0000",
            "created_by": "Gandalf",
            "created_by_email": "gandalf@tlotr.com",
            "case_number": "00001234",
            "is_closed": False,
            "last_updated": "2023-08-11T00:00:00.000+0000",
            "owner": "Frodo",
            "owner_email": "frodo@tlotr.com",
            "participant_emails": [
                "elrond@tlotr.com",
                "frodo@tlotr.com",
                "galadriel@tlotr.com",
                "gandalf@tlotr.com",
                "gimli@tlotr.com",
                "samwise@tlotr.com",
            ],
            "participant_ids": ["user_id", "user_id_2", "user_id_3", "user_id_4"],
            "participants": ["Frodo", "Galadriel", "Gandalf", "Gimli"],
            "source": "salesforce",
            "status": "New",
            "title": "It needs to be destroyed",
            "type": "case",
            "url": f"{TEST_BASE_URL}/case_id",
        }

        source.salesforce_client.sobjects_cache_by_type = mock.AsyncMock(
            return_value=CACHED_SOBJECTS
        )
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock(
            return_value=RELEVANT_SOBJECT_FIELDS
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=CASE_PAYLOAD,
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=CASE_FEED_PAYLOAD,
        )
        async for account in source.salesforce_client.get_cases():
            assert account == expected_doc


@pytest.mark.asyncio
async def test_prepare_sobject_cache(mock_responses):
    async with create_salesforce_source() as source:
        sobjects = {
            "records": [
                {"Id": "id_1", "Name": "Foo", "Type": "Account"},
                {"Id": "id_2", "Name": "Bar", "Type": "Account"},
            ]
        }
        expected = {
            "id_1": {"Id": "id_1", "Name": "Foo", "Type": "Account"},
            "id_2": {"Id": "id_2", "Name": "Bar", "Type": "Account"},
        }
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=sobjects,
        )
        sobjects = await source.salesforce_client._prepare_sobject_cache("Account")
        assert sobjects == expected


@pytest.mark.asyncio
async def test_request_when_token_invalid_refetches_token(patch_sleep, mock_responses):
    async with create_salesforce_source() as source:
        expected_doc = {
            "_id": "account_id",
            "account_type": "Customer - Direct",
            "address": "The Burrow under the Hill, Bag End, Hobbiton, The Shire, Eriador, 111, Middle Earth",
            "body": "A story about the One Ring.",
            "content_source_id": "account_id",
            "created_at": "",
            "last_updated": "",
            "owner": "Frodo",
            "owner_email": "frodo@tlotr.com",
            "open_activities": "",
            "open_activities_urls": "",
            "opportunity_name": "The Fellowship",
            "opportunity_status": "Closed Won",
            "opportunity_url": f"{TEST_BASE_URL}/opportunity_id",
            "rating": "Hot",
            "source": "salesforce",
            "tags": ["Customer - Direct"],
            "title": "TLOTR",
            "type": "account",
            "url": f"{TEST_BASE_URL}/account_id",
            "website_url": "www.tlotr.com",
        }

        invalid_token_payload = [
            {
                "message": "Session expired or invalid",
                "errorCode": "INVALID_SESSION_ID",
            }
        ]
        token_response_payload = {"access_token": "foo"}
        mock_responses.post(
            f"{TEST_BASE_URL}/services/oauth2/token",
            status=200,
            payload=token_response_payload,
        )
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()

        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=401,
            payload=invalid_token_payload,
        )
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=200,
            payload=ACCOUNT_RESPONSE_PAYLOAD,
        )

        with mock.patch.object(
            source.salesforce_client.api_token,
            "generate",
            wraps=source.salesforce_client.api_token.generate,
        ) as mock_get_token:
            async for account in source.salesforce_client.get_accounts():
                assert account == expected_doc
                mock_get_token.assert_called_once()


@pytest.mark.asyncio
async def test_request_when_rate_limited_raises_error_no_retries(mock_responses):
    async with create_salesforce_source() as source:
        response_payload = [
            {
                "message": "Request limit has been exceeded.",
                "errorCode": "REQUEST_LIMIT_EXCEEDED",
            }
        ]
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=403,
            payload=response_payload,
        )

        with pytest.raises(RateLimitedException):
            async for _ in source.salesforce_client.get_accounts():
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    [
        "INVALID_FIELD",
        "INVALID_TERM",
        "MALFORMED_QUERY",
    ],
)
async def test_request_when_invalid_query_raises_error_no_retries(
    mock_responses, error_code
):
    async with create_salesforce_source() as source:
        response_payload = [
            {
                "message": "Invalid query.",
                "errorCode": error_code,
            }
        ]
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=400,
            payload=response_payload,
        )

        with pytest.raises(InvalidQueryException):
            async for _ in source.salesforce_client.get_accounts():
                pass


@pytest.mark.asyncio
async def test_request_when_generic_400_raises_error_with_retries(
    patch_sleep, mock_responses
):
    async with create_salesforce_source() as source:
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=400,
            repeat=True,
        )

        with pytest.raises(ConnectorRequestError):
            async for _ in source.salesforce_client.get_accounts():
                pass


@pytest.mark.asyncio
async def test_request_when_generic_500_raises_error_with_retries(
    patch_sleep, mock_responses
):
    async with create_salesforce_source() as source:
        source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
        source.salesforce_client._select_queryable_fields = mock.AsyncMock()
        mock_responses.get(
            re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
            status=500,
            repeat=True,
        )

        with pytest.raises(SalesforceServerError):
            async for _ in source.salesforce_client.get_accounts():
                pass


@pytest.mark.asyncio
async def test_build_soql_query_with_fields():
    expected_columns = [
        "Id",
        "CreatedDate",
        "LastModifiedDate",
        "FooField",
        "BarField",
    ]

    builder = SalesforceSoqlBuilder("Test")
    builder.with_id()
    builder.with_default_metafields()
    builder.with_fields(["FooField", "BarField"])
    builder.with_where("FooField = 'FOO'")
    builder.with_order_by("CreatedDate DESC")
    builder.with_limit(2)
    # builder.with_join()
    query = builder.build()

    # SELECT Id,
    # CreatedDate,
    # LastModifiedDate,
    # FooField,
    # BarField,
    # FROM Test
    # WHERE FooField = 'FOO'
    # ORDER BY CreatedDate DESC
    # LIMIT 2

    query_columns_str = re.search("SELECT (.*)\nFROM", query, re.DOTALL).group(1)
    query_columns = query_columns_str.split(",\n")

    TestCase().assertCountEqual(query_columns, expected_columns)
    assert query.startswith("SELECT ")
    assert query.endswith(
        "FROM Test\nWHERE FooField = 'FOO'\nORDER BY CreatedDate DESC\nLIMIT 2"
    )
