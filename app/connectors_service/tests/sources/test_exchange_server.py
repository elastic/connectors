#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Exchange Server source class methods"""

from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from connectors_sdk.source import ConfigurableFieldValueError
from exchangelib import UTC
from exchangelib.errors import (
    ErrorAccessDenied,
    ErrorNonExistentMailbox,
    ErrorNonPrimarySmtpAddress,
    TransportError,
)
from exchangelib.items import CalendarItem, Contact, Message, Task

from connectors.access_control import ACCESS_CONTROL
from connectors.config import _default_config
from connectors.sources.exchange_server import (
    ExchangeServerClient,
    ExchangeServerDataSource,
)
from connectors.sources.exchange_server.client import (
    ExchangeUsers,
    UsersFetchFailed,
)
from connectors.sources.exchange_server.constants import INBOX_MAIL_OBJECT
from connectors.sources.outlook import OutlookDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source

TIMESTAMP = "2023-12-12T01:01:01Z"
MAILBOX = "dummy@es.local"
LDAP_USER = {
    "attributes": {"mail": [MAILBOX]},
    "dn": "CN=Dummy,CN=Users,DC=es,DC=local",
}

# Real self-signed certificate in the single-line form the connector receives.
# load_verify_locations ignores validity dates, so it never expires for tests.
VALID_SSL_CERTIFICATE = (
    "-----BEGIN CERTIFICATE----- "
    "MIICsjCCAZqgAwIBAgIUDznyN9v5Tk8muCxnL/Z2EFwtcBwwDQYJKoZIhvcNAQELBQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAgFw0wMDAxMDEwMDAwMDBaGA8yMDk5MDEwMTAwMDAwMFowEjEQMA4GA1UEAwwHdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKmQAqm3+hDpc9+OzTjhY4W/AASWa41qyeuKNL+K8kA6oh9TmT20YhPikxPzQCUxp/prm9pi9eym5VLh2GhNCCE8LR+TsrwZr2MpYGZph1Y/y4U5PVNZCOboCee44F/6f8huYtHRPSrOC1OHehvMwdfAC63MueN6oBtxIIOwktxlkuBbK5wY97QlY/utxMa72APdUh3TAyzA6GWum7rLvEafj1v7WRpJWkTpklFXhaGVm4u/SWeFiMfgIK+ciJgT04k0qbk8APwuPmLR5VmUNyMDOgLMtSLu9sbntVv+eLoAAiOFLk5ZpHs0Q8UPANdNMV03tgxaDnvtgzh7W0Qgvo8CAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAGPI/7KUIjHsNuRHUALIRtNVlhD80gdzKN27IEFLTu/jbiNEIGY59oV0qvx+iCPrLTLDnJkxHlnwApwB2WulXNg7+nYGHPP03jSLXKA+61GAN/ghPULl1DcA5Q+gunhPA4ITyqOr70i/3fphSXWjWfcX8hcym3pDcKzPIY3wV+dVeVdRdi9C1cTRuZ7zh2Chm7e4vM1SagLybMA4F8yckPJsRdVV5hZ+W6cI1H9fhjq/G1N0TyH4wG3FffRVniYVgAxY9m9RgMiQ5qCuc2PdktO7ovmNybijVG1aLVcHcYAS285f4JnZPIAJJMvCvW0NXDDphBNQPG5Nt1PHVgzUiNA== "
    "-----END CERTIFICATE-----"
)


@asynccontextmanager
async def create_exchange_server_source(
    exchange_server="127.0.0.1",
    active_directory_server="127.0.0.1",
    username="fee",
    password="fuu",
    domain="es.local",
    ssl_enabled=False,
    ssl_ca="",
    use_text_extraction_service=False,
    sync_all_mail_folders=False,
):
    async with create_source(
        ExchangeServerDataSource,
        exchange_server=exchange_server,
        active_directory_server=active_directory_server,
        username=username,
        password=password,
        domain=domain,
        ssl_enabled=ssl_enabled,
        ssl_ca=ssl_ca,
        use_text_extraction_service=use_text_extraction_service,
        sync_all_mail_folders=sync_all_mail_folders,
    ) as source:
        yield source


def build_account(smtp=MAILBOX, default_timezone="UTC"):
    account = MagicMock()
    account.primary_smtp_address = smtp
    account.default_timezone = default_timezone
    return account


def build_mail():
    sender = MagicMock()
    sender.email_address = MAILBOX
    mail = MagicMock(spec=Message)
    mail.id = "mail_1"
    mail.last_modified_time = TIMESTAMP
    mail.subject = "Dummy Subject"
    mail.sender = sender
    mail.to_recipients = [sender]
    mail.cc_recipients = []
    mail.bcc_recipients = []
    mail.importance = "Normal"
    mail.categories = []
    mail.body = "This is a dummy mail"
    mail.text_body = None
    mail.mime_content = None
    mail.reply_to = None
    mail.message_id = None
    mail.datetime_received = None
    mail.has_attachments = False
    return mail


def build_contact():
    contact = MagicMock(spec=Contact)
    contact.id = "contact_1"
    contact.last_modified_time = TIMESTAMP
    contact.display_name = "Dummy User"
    contact.email_addresses = []
    contact.phone_numbers = []
    contact.company_name = "ABC"
    contact.birthday = None
    return contact


def build_task():
    task = MagicMock(spec=Task)
    task.id = "task_1"
    task.last_modified_time = TIMESTAMP
    task.subject = "Dummy task"
    task.owner = "Dummy User"
    task.start_date = None
    task.due_date = None
    task.complete_date = None
    task.categories = []
    task.importance = "Normal"
    task.text_body = "This is a dummy task"
    task.status = "NotStarted"
    task.has_attachments = False
    return task


def build_calendar():
    calendar = MagicMock(spec=CalendarItem)
    calendar.id = "calendar_1"
    calendar.last_modified_time = TIMESTAMP
    calendar.subject = "Dummy meeting"
    calendar.type = "Single"
    calendar.organizer = None
    calendar.required_attendees = []
    calendar.start = TIMESTAMP
    calendar.end = TIMESTAMP
    calendar.location = "Office"
    calendar.body = "This is a dummy meeting"
    calendar.has_attachments = False
    return calendar


def mock_mailbox_content(source):
    source.client.get_mails = AsyncIterator(
        [(build_mail(), {"constant": INBOX_MAIL_OBJECT})]
    )
    source.client.get_contacts = AsyncIterator([build_contact()])
    source.client.get_tasks = AsyncIterator([build_task()])
    source.client.get_calendars = AsyncIterator([build_calendar()])
    source.client.get_child_calendars = AsyncIterator([])


EXPECTED_IDS = {"mail_1", "contact_1", "task_1", "calendar_1"}


def test_exchange_server_is_registered():
    assert (
        _default_config()["sources"]["exchange_server"]
        == "connectors.sources.exchange_server:ExchangeServerDataSource"
    )


def test_exchange_server_identity():
    assert ExchangeServerDataSource.name == "Exchange Server"
    assert ExchangeServerDataSource.service_type == "exchange_server"
    assert ExchangeServerDataSource.incremental_sync_enabled is True
    assert ExchangeServerDataSource.dls_enabled is True


def test_default_configuration_is_server_only():
    configuration = ExchangeServerDataSource.get_default_configuration()

    assert "data_source" not in configuration
    for cloud_field in ("tenant_id", "client_id", "client_secret"):
        assert cloud_field not in configuration
    assert configuration["ssl_ca"]["depends_on"] == [
        {"field": "ssl_enabled", "value": True}
    ]
    assert [key for key, field in configuration.items() if "depends_on" in field] == [
        "ssl_ca"
    ]
    orders = [field["order"] for field in configuration.values()]
    assert len(orders) == len(set(orders))


def test_default_configuration_keys_match_legacy_server_fields():
    # Keeping the legacy key names lets an outlook_server configuration carry
    # over field by field.
    legacy_configuration = OutlookDataSource.get_default_configuration()

    assert set(ExchangeServerDataSource.get_default_configuration()) <= set(
        legacy_configuration
    )


@pytest.mark.asyncio
async def test_client_uses_exchange_users():
    async with create_exchange_server_source() as source:
        assert type(source.client) is ExchangeServerClient
        assert isinstance(source.client._get_user_instance, ExchangeUsers)
        assert not hasattr(source.client, "is_cloud")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["exchange_server", "active_directory_server", "domain"]
)
async def test_validate_config_rejects_blank_required_field(field):
    async with create_exchange_server_source(**{field: ""}) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_passes_with_valid_fields():
    async with create_exchange_server_source() as source:
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_ignores_certificate_when_ssl_disabled():
    async with create_exchange_server_source(
        ssl_enabled=False, ssl_ca="this is not a certificate"
    ) as source:
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_raises_when_ssl_enabled_without_certificate():
    async with create_exchange_server_source(ssl_enabled=True, ssl_ca="") as source:
        with pytest.raises(ConfigurableFieldValueError) as exc_info:
            await source.validate_config()

        assert "SSL certificate" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_config_raises_when_certificate_is_invalid():
    async with create_exchange_server_source(
        ssl_enabled=True, ssl_ca="this is not a certificate"
    ) as source:
        with pytest.raises(ConfigurableFieldValueError) as exc_info:
            await source.validate_config()

        assert "SSL certificate is not valid" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_config_passes_when_ssl_enabled_with_valid_certificate():
    async with create_exchange_server_source(
        ssl_enabled=True, ssl_ca=VALID_SSL_CERTIFICATE
    ) as source:
        await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.sources.exchange_server.client.Connection")
async def test_ping(mock_connection):
    mock_connection.return_value.search.return_value = (
        True,
        None,
        [{"mail": MAILBOX}],
        None,
    )

    async with create_exchange_server_source() as source:
        await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", return_value=0)
@patch("connectors.sources.exchange_server.client.Connection")
async def test_ping_raises_when_users_cannot_be_fetched(mock_connection, _mock_sleep):
    mock_connection.return_value.search.return_value = (False, None, [], None)

    async with create_exchange_server_source() as source:
        with pytest.raises(UsersFetchFailed):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_closes_the_users_generator():
    closed = []

    async def get_users():
        try:
            yield LDAP_USER
            yield LDAP_USER
        finally:
            closed.append(True)

    async with create_exchange_server_source() as source:
        source.client._get_user_instance.get_users = get_users

        await source.ping()

    assert closed == [True]


@pytest.mark.asyncio
async def test_get_access_control_skips_when_dls_disabled():
    async with create_exchange_server_source() as source:
        source.client._get_user_instance.get_users = AsyncIterator([LDAP_USER])

        assert [doc async for doc in source.get_access_control()] == []


@pytest.mark.asyncio
async def test_get_access_control_yields_server_identities():
    async with create_exchange_server_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.client._get_user_instance.get_users = AsyncIterator(
            [LDAP_USER, {"attributes": {"mail": []}, "dn": "CN=NoMail"}]
        )

        documents = [doc async for doc in source.get_access_control()]

    assert len(documents) == 1
    assert documents[0]["identity"]["email"] == f"email:{MAILBOX}"
    assert documents[0]["identity"]["display_name"] == "name:Dummy"


@pytest.mark.asyncio
async def test_get_docs_yields_mailbox_content():
    async with create_exchange_server_source() as source:
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [build_account()]
        )
        mock_mailbox_content(source)

        documents = [document async for document, _ in source.get_docs()]

    assert {document["_id"] for document in documents} == EXPECTED_IDS


@pytest.mark.asyncio
async def test_content_documents_are_reachable_by_their_owner():
    async with create_exchange_server_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.client._get_user_instance.get_users = AsyncIterator([LDAP_USER])
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [build_account()]
        )
        mock_mailbox_content(source)

        access_control_documents = [doc async for doc in source.get_access_control()]
        content_documents = [doc async for doc, _ in source.get_docs()]

    granted = set(
        access_control_documents[0]["query"]["template"]["params"]["access_control"]
    )
    assert content_documents
    for document in content_documents:
        assert granted & set(document[ACCESS_CONTROL])


@pytest.mark.asyncio
async def test_get_docs_falls_back_to_utc_timezone():
    async with create_exchange_server_source() as source:
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [build_account(default_timezone=None)]
        )
        captured = {}

        def fake_fetch_mails(account, timezone):
            captured["timezone"] = timezone
            return AsyncIterator([])

        source._fetch_mails = fake_fetch_mails
        mock_mailbox_content(source)

        _ = [document async for document, _ in source.get_docs()]

    assert captured["timezone"] is UTC


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        ErrorNonExistentMailbox("no mailbox"),
        ErrorNonPrimarySmtpAddress("non-primary smtp"),
        ErrorAccessDenied("access denied"),
    ],
)
async def test_get_docs_skips_account_specific_failure_and_continues(exception):
    async with create_exchange_server_source() as source:
        bad_account = build_account(smtp="broken@es.local")
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [bad_account, build_account()]
        )
        mock_mailbox_content(source)
        healthy_get_mails = source.client.get_mails

        def get_mails(account):
            if account is bad_account:
                raise exception
            return healthy_get_mails(account)

        source.client.get_mails = get_mails
        source._logger = MagicMock()

        documents = [document async for document, _ in source.get_docs()]

    assert {document["_id"] for document in documents} == EXPECTED_IDS
    source._logger.warning.assert_called_once()
    warning_message = source._logger.warning.call_args.args[0]
    assert "broken@es.local" in warning_message
    assert type(exception).__name__ in warning_message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception", [TransportError("TLS verification failed"), RuntimeError("boom")]
)
async def test_get_docs_reraises_connection_wide_error(exception):
    async with create_exchange_server_source() as source:
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [build_account(), build_account()]
        )
        mock_mailbox_content(source)
        source.client.get_mails = mock.Mock(side_effect=exception)

        with pytest.raises(type(exception)):
            async for _document, _ in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_close_closes_user_instance():
    async with create_exchange_server_source() as source:
        user_instance = source.client._get_user_instance
        with patch.object(user_instance, "close") as close:
            await source.close()

        close.assert_awaited_once()
