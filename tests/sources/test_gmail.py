#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from freezegun import freeze_time

from connectors.protocol import Features, Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.gmail import (
    ACCESS_CONTROL,
    GMailAdvancedRulesValidator,
    GMailDataSource,
    _message_doc,
)
from connectors.sources.google import MessageFields, UserFields
from connectors.utils import iso_utc
from tests.commons import AsyncIterator
from tests.sources.support import create_source

TIME = "2023-01-24T04:07:19"

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


def setup_source():
    source = create_source(GMailDataSource)
    source._service_account_credentials = MagicMock()

    return source


def set_dls_enabled(source, dls_enabled):
    source.set_features(Features({"document_level_security": {"enabled": dls_enabled}}))
    source.configuration.set_field("use_document_level_security", value=dls_enabled)


def set_include_spam_and_trash(source, include_spam_and_trash):
    source.configuration.set_field(
        "include_spam_and_trash", value=include_spam_and_trash
    )


async def setup_messages_and_users_apis(
    patch_gmail_client, patch_google_directory_client, messages, users
):
    patch_google_directory_client.users = AsyncIterator(users)
    patch_gmail_client.messages = AsyncIterator(messages)
    patch_gmail_client.message = AsyncMock(side_effect=messages)


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

    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_disabled(
        self, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        patch_google_directory_client.users = AsyncIterator(users)

        async with setup_source() as source:
            set_dls_enabled(source, False)
            actual_users = []

            async for user in source.get_access_control():
                actual_users.append(user)

            assert len(actual_users) == 0
            patch_google_directory_client.users.assert_not_called()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_enabled(
        self, patch_google_directory_client
    ):
        email = "user@google.com"
        creation_date = iso_utc()
        users = [
            {
                UserFields.EMAIL.value: email,
                UserFields.CREATION_DATE.value: creation_date,
            }
        ]
        patch_google_directory_client.users = AsyncIterator(users)

        async with setup_source() as source:
            set_dls_enabled(source, True)
            actual_users = []

            async for user in source.get_access_control():
                actual_users.append(user)

            actual_user = actual_users[0]

            assert len(actual_users) == len(users)
            assert actual_user["_id"] == email
            assert actual_user["identity"]["email"] == email
            assert actual_user["created_at"] == creation_date

            patch_google_directory_client.users.assert_called_once()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_dls_without_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_dls_enabled(source, False)

            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL not in actual_message

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_dls_with_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_dls_enabled(source, False)
            actual_messages = []
            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL not in actual_message

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=ANY
            )

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_dls_without_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_dls_enabled(source, True)

            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL in actual_message
            assert email in actual_message[ACCESS_CONTROL]

            patch_gmail_client.messages.assert_called_once()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_dls_with_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_dls_enabled(source, True)
            actual_messages = []
            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL in actual_message
            assert email in actual_message[ACCESS_CONTROL]

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=ANY
            )

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_filtering_and_include_spam_and_trash(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_include_spam_and_trash(source, True)
            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"

            patch_gmail_client.messages.assert_called_once_with(includeSpamTrash=True)

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_filtering_and_include_spam_and_trash(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: "abcd",
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with setup_source() as source:
            set_include_spam_and_trash(source, True)
            actual_messages = []

            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=True
            )
