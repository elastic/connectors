#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Salesforce source class methods"""
import re
import time
from unittest import TestCase, mock

import pytest
from aiohttp.client_exceptions import ClientConnectionError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.salesforce import SalesforceDataSource, SalesforceSoqlBuilder
from tests.sources.support import create_source

TEST_DOMAIN = "fake"
TEST_BASE_URL = f"https://{TEST_DOMAIN}.my.salesforce.com"
TEST_CLIENT_ID = "1234"
TEST_CLIENT_SECRET = "9876"
SECONDS_SINCE_EPOCH = int(time.time())

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
                "Name": "Owner's Name",
                "Email": "email@fake.com",
            },
            "Id": "account_id",
            "Rating": "Cold",
            "Website": "www.fake.com",
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
                        "Name": "Opportunity Generator",
                        "StageName": "Closed Won",
                    }
                ],
            },
            "Name": "Salesforce Account 1",
            "BillingAddress": {
                "city": "The Shire",
                "country": "Middle Earth",
                "postalCode": 111,
                "state": "Eriador",
                "street": "The Burrow under the Hill, Bag End, Hobbiton",
            },
            "Description": "A fantastic opportunity!",
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
            "Description": "An Opportunity!",
            "Owner": {
                "attributes": {
                    "type": "User",
                    "url": "/services/data/v58.0/sobjects/User/user_id",
                },
                "Id": "user_id",
                "Email": "email@fake.com",
                "Name": "User's Name",
            },
            "LastModifiedDate": "",
            "Name": "Another Opportunity Generator",
            "StageName": "Qualification",
            "CreatedDate": "",
            "Id": "opportunity_id",
        },
    ],
}


def create_salesforce_source():
    return create_source(
        SalesforceDataSource,
        domain=TEST_DOMAIN,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )


async def create_fake_coroutine(data):
    """create a method for returning fake coroutine value"""
    return data


def test_get_default_configuration():
    config = DataSourceConfiguration(SalesforceDataSource.get_default_configuration())
    expected_fields = ["client_id", "client_secret", "domain"]

    assert all(field in config.to_dict() for field in expected_fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["client_id", "client_secret", "domain"])
async def test_validate_config_missing_fields_then_raise(field):
    source = create_salesforce_source()
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection(mock_responses):
    source = create_salesforce_source()
    mock_responses.head(TEST_BASE_URL, status=200)

    await source.ping()
    await source.close()


@pytest.mark.asyncio
async def test_get_token_with_successful_connection(mock_responses):
    source = create_salesforce_source()
    response_payload = {
        "access_token": "foo",
        "signature": "bar",
        "instance_url": "https://fake.my.salesforce.com",
        "id": "https://login.salesforce.com/id/1234",
        "token_type": "Bearer",
        "issued_at": SECONDS_SINCE_EPOCH,
    }

    mock_responses.post(
        f"{TEST_BASE_URL}/services/oauth2/token", status=200, payload=response_payload
    )
    await source.salesforce_client.get_token()

    assert source.salesforce_client.token == "foo"
    assert source.salesforce_client.token_issued_at == SECONDS_SINCE_EPOCH

    await source.close()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_token_with_bad_domain_raises_error(
    apply_retry_strategy, mock_responses
):
    source = create_salesforce_source()
    apply_retry_strategy.return_value = mock.Mock()

    response_payload = {
        "access_token": "foo",
        "signature": "bar",
        "instance_url": "https://fake.my.salesforce.com",
        "id": "https://login.salesforce.com/id/1234",
        "token_type": "Bearer",
        "issued_at": SECONDS_SINCE_EPOCH,
    }

    mock_responses.post(
        f"{TEST_BASE_URL}/services/oauth2/token", status=400, payload=response_payload
    )
    with pytest.raises(ClientConnectionError):
        await source.salesforce_client.get_token()
    await source.close()


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
    source = create_salesforce_source()
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

    await source.close()


@pytest.mark.asyncio
@mock.patch("connectors.sources.salesforce.RELEVANT_SOBJECTS", ["Account"])
@mock.patch(
    "connectors.sources.salesforce.RELEVANT_SOBJECT_FIELDS",
    ["FooField", "BarField", "ArghField"],
)
async def test_get_queryable_fields(mock_responses):
    source = create_salesforce_source()
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

    await source.close()


@pytest.mark.asyncio
async def test_get_accounts_when_success(mock_responses):
    source = create_salesforce_source()
    expected_doc = {
        "_id": "account_id",
        "account_type": "Customer - Direct",
        "address": "The Burrow under the Hill, Bag End, Hobbiton, The Shire, Eriador, 111, Middle Earth",
        "body": "A fantastic opportunity!",
        "content_source_id": "account_id",
        "created_at": "",
        "last_updated": "",
        "owner": "Owner's Name",
        "owner_email": "email@fake.com",
        "open_activities": "",
        "open_activities_urls": "",
        "opportunity_name": "Opportunity Generator",
        "opportunity_status": "Closed Won",
        "opportunity_url": f"{TEST_BASE_URL}/opportunity_id",
        "rating": "Cold",
        "source": "salesforce",
        "tags": ["Customer - Direct"],
        "title": "Salesforce Account 1",
        "type": "account",
        "url": f"{TEST_BASE_URL}/account_id",
        "website_url": "www.fake.com",
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

    await source.close()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_accounts_when_invalid_request(apply_retry_strategy, mock_responses):
    source = create_salesforce_source()

    response_payload = [
        {"message": "Unable to process query.", "errorCode": "INVALID_FIELD"}
    ]

    source.salesforce_client._is_queryable = mock.AsyncMock(return_value=True)
    mock_responses.get(
        re.compile(f"{TEST_BASE_URL}/services/data/v58.0/query*"),
        status=400,
        payload=response_payload,
    )
    with pytest.raises(ClientConnectionError):
        async for _ in source.salesforce_client.get_accounts():
            # TODO confirm error message when error handling is improved
            pass

    await source.close()


@pytest.mark.asyncio
async def test_get_accounts_when_not_queryable_yields_nothing(mock_responses):
    source = create_salesforce_source()

    source.salesforce_client._is_queryable = mock.AsyncMock(return_value=False)
    async for account in source.salesforce_client.get_accounts():
        assert account is None

    await source.close()


@pytest.mark.asyncio
async def test_get_opportunities_when_success(mock_responses):
    source = create_salesforce_source()
    expected_doc = {
        "_id": "opportunity_id",
        "body": "An Opportunity!",
        "content_source_id": "opportunity_id",
        "created_at": "",
        "last_updated": "",
        "next_step": None,
        "owner": "User's Name",
        "owner_email": "email@fake.com",
        "source": "salesforce",
        "status": "Qualification",
        "title": "Another Opportunity Generator",
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

    await source.close()


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
    query = builder.build()

    assert query.startswith("SELECT ")
    assert all(col in query for col in expected_columns)
    assert query.endswith("FROM Test")
