#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from freezegun import freeze_time

from connectors.source import ConfigurableFieldValueError
from connectors.sources.gmail import (
    GMailAdvancedRulesValidator,
    GMailDataSource,
    _message_doc,
)
from tests.sources.support import create_source

CUSTOMER_ID = "customer_id"

DATE = "2023-01-24T04:07:19+00:00"

JSON_CREDENTIALS = {"key": "value"}


class TestGMailAdvancedRulesValidator:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "advanced_rules, is_valid",
        [
            (
                # empty advanced rules (dictionary)
                {},
                True,
            ),
            (
                # empty advanced rules (array)
                [],
                True,
            ),
            (
                # string in array
                {
                    "messages": ["sent"],
                },
                True,
            ),
            (
                # multiple strings in array
                {
                    "messages": ["sent", "some other query"],
                },
                True,
            ),
            (
                # wrong type
                {
                    "messages": [1],
                },
                False,
            ),
        ],
    )
    async def test_advanced_rules_validator(self, advanced_rules, is_valid):
        validation_result = await GMailAdvancedRulesValidator().validate(advanced_rules)
        assert validation_result.is_valid == is_valid


def setup_source():
    source = create_source(GMailDataSource)
    source._service_account_credentials = MagicMock()

    return source


MESSAGE_ID = 1
FULL_MESSAGE = "some message"
CREATION_DATE = "2023-01-01T13:37:00"


@pytest.mark.parametrize(
    "message, expected_doc",
    [
        (
            {"id": MESSAGE_ID, "raw": FULL_MESSAGE, "internalDate": CREATION_DATE},
            {
                "_id": MESSAGE_ID,
                "_attachment": FULL_MESSAGE,
                "_timestamp": CREATION_DATE,
            },
        ),
        (
            {"id": None, "raw": FULL_MESSAGE, "internalDate": CREATION_DATE},
            {"_id": None, "_attachment": FULL_MESSAGE, "_timestamp": CREATION_DATE},
        ),
        (
            {"id": MESSAGE_ID, "raw": None, "internalDate": CREATION_DATE},
            {"_id": MESSAGE_ID, "_attachment": None, "_timestamp": CREATION_DATE},
        ),
        (
            # timestamp is added, if it's `None`
            {"id": MESSAGE_ID, "raw": FULL_MESSAGE, "internalDate": None},
            {"_id": MESSAGE_ID, "_attachment": FULL_MESSAGE, "_timestamp": DATE},
        ),
    ],
)
@freeze_time(DATE)
def test_message_doc(message, expected_doc):
    assert _message_doc(message) == expected_doc


class TestGMailDataSource:
    @pytest_asyncio.fixture
    async def patch_gmail_client(self):
        with patch(
            "connectors.sources.gmail.GMailClient", return_value=AsyncMock()
        ) as mock:
            client = mock.return_value
            yield client

    @pytest_asyncio.fixture
    async def patch_google_directory_client(self):
        with patch(
            "connectors.sources.gmail.GoogleDirectoryClient", return_value=AsyncMock()
        ) as mock:
            client = mock.return_value
            yield client

    @pytest.mark.asyncio
    async def test_ping_successful(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with setup_source() as source:
            patch_gmail_client.ping = AsyncMock()
            patch_google_directory_client.ping = AsyncMock()

            try:
                await source.ping()
            except Exception as e:
                raise AssertionError("Ping should've been successful") from e

    @pytest.mark.asyncio
    async def test_ping_gmail_client_fails(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with setup_source() as source:
            patch_gmail_client.ping = AsyncMock(
                side_effect=Exception("Something went wrong")
            )
            patch_google_directory_client.ping = AsyncMock()

            with pytest.raises(Exception):
                await source.ping()

    @pytest.mark.asyncio
    async def test_ping_google_directory_client_fails(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with setup_source() as source:
            patch_gmail_client.ping = AsyncMock()
            patch_google_directory_client.ping = AsyncMock(side_effect=Exception)

            with pytest.raises(Exception):
                await source.ping()

    @pytest.mark.asyncio
    async def test_validate_config_valid(self):
        valid_json = '{"key": "value"}'

        async with setup_source() as source:
            source.configuration.set_field(
                "service_account_credentials", value=valid_json
            )
            source.configuration.set_field("customer_id", value=CUSTOMER_ID)

            try:
                await source.validate_config()
            except ConfigurableFieldValueError:
                raise AssertionError("Should've been a valid config") from None

    @pytest.mark.asyncio
    async def test_validate_config_invalid(self):
        async with setup_source() as source:
            source.configuration.set_field(
                "service_account_credentials", value="invalid json"
            )

            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()
