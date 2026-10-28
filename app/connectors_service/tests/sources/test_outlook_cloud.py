#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Outlook Cloud (Microsoft Graph) source class methods"""

import ast
import asyncio
import re
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import unquote, unquote_plus

import pytest
from aiohttp.client_exceptions import (
    ClientOSError,
    ClientPayloadError,
    ClientResponseError,
    ServerDisconnectedError,
)
from aioresponses.core import normalize_url
from connectors_sdk.source import DataSourceConfiguration

from connectors.config import _default_config
from connectors.sources import outlook_cloud
from connectors.sources.outlook_cloud import OutlookCloudDataSource
from connectors.sources.outlook_cloud import client as outlook_cloud_client
from connectors.sources.outlook_cloud.client import NoAccessibleMailboxes
from connectors.sources.outlook_cloud.constants import (
    DEFAULT_MAIL_FOLDERS,
    DEFAULT_RETRY_SECONDS,
    EXCLUDED_WELL_KNOWN_FOLDERS,
    FOLDER_FIELDS,
    GRAPH_BASE_URL,
    INBOX_MAIL_OBJECT,
    MAILBOX_ACCESSIBLE,
    MAILBOX_MISSING,
    MAILBOX_OUT_OF_SCOPE,
    MESSAGE_FIELDS,
    NETWORK_RETRY_SECONDS,
    RETRIES,
)
from connectors.sources.shared.microsoft.graph import (
    PermissionsMissing,
    TokenFetchFailed,
)
from tests.sources.outlook_contract import assert_conforms
from tests.sources.support import create_source

TENANT_ID = "tenant-id"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
PING_URL = f"{GRAPH_BASE_URL}/users?$top=1&$select=id"
USERS_URL = re.compile(rf"^{re.escape(GRAPH_BASE_URL)}/users\?.*filter.*$")
NEXT_PAGE_URL = f"{GRAPH_BASE_URL}/users?$skiptoken=page-2"


def mailbox_url(user_id):
    return f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/inbox?$select=id"


def user(user_id, mail="user@example.com"):
    return {"id": user_id, "displayName": user_id, "mail": mail, "jobTitle": None}


def create_outlook_cloud_source(**extras):
    return create_source(
        OutlookCloudDataSource,
        tenant_id=TENANT_ID,
        client_id="client-id",
        client_secret="client-secret",
        **extras,
    )


@pytest.fixture
def graph(mock_responses):
    """Mock the token endpoint; tests add the Graph endpoints they need."""
    mock_responses.post(
        TOKEN_URL,
        payload={"access_token": "fake-token", "expires_in": 3600},
        repeat=True,
    )
    return mock_responses


def requests_to(mock_responses, url):
    return [
        call
        for (method, request_url), calls in mock_responses.requests.items()
        if method == "GET" and request_url == normalize_url(url)
        for call in calls
    ]


def test_get_configuration():
    config = DataSourceConfiguration(OutlookCloudDataSource.get_default_configuration())

    assert list(config.to_dict()) == [
        "tenant_id",
        "client_id",
        "client_secret",
        "sync_all_mail_folders",
        "include_full_raw_message",
    ]
    assert config.to_dict()["client_secret"]["sensitive"] is True


def test_outlook_cloud_is_not_registered_in_config():
    assert "outlook_cloud" not in _default_config()["sources"]


def test_outlook_cloud_does_not_use_ews():
    forbidden = ("exchangelib", "connectors.sources.exchange_server")
    for path in Path(outlook_cloud.__file__).parent.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            else:
                continue
            assert not [m for m in modules if m.startswith(forbidden)], path.name


@pytest.mark.asyncio
async def test_ping(graph):
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()

    headers = requests_to(graph, PING_URL)[0].kwargs["headers"]
    assert headers["Authorization"] == "Bearer fake-token"
    assert headers["Prefer"] == 'IdType="ImmutableId"'


@pytest.mark.asyncio
async def test_ping_raises_when_users_cannot_be_listed(graph):
    graph.get(PING_URL, status=403)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(PermissionsMissing, match="User.Read.All"):
            await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", return_value=0)
async def test_ping_raises_when_token_is_refused(_sleep, mock_responses):
    mock_responses.post(TOKEN_URL, status=401, repeat=True)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(TokenFetchFailed):
            await source.ping()


@pytest.mark.asyncio
async def test_get_users_follows_pagination(graph):
    graph.get(
        USERS_URL,
        payload={"value": [user("1"), user("2")], "@odata.nextLink": NEXT_PAGE_URL},
    )
    graph.get(NEXT_PAGE_URL, payload={"value": [user("3")]})

    async with create_outlook_cloud_source() as source:
        users = [u async for u in source.client.get_users()]

    assert [u["id"] for u in users] == ["1", "2", "3"]
    # aioresponses stores the URL encoded twice; decode it back to read the query.
    first_url = next(
        unquote_plus(unquote(str(url)))
        for (method, url) in graph.requests
        if method == "GET" and "filter" in str(url)
    )
    assert "$filter=accountEnabled eq true" in first_url
    assert "$select=id,displayName,mail,jobTitle" in first_url
    assert "$top=999" in first_url


@pytest.mark.asyncio
async def test_paging_link_to_another_host_is_not_followed(graph):
    other_host_url = "https://other.example.com/users?$skiptoken=page-2"
    graph.get(
        USERS_URL,
        payload={"value": [user("1")], "@odata.nextLink": other_host_url},
    )
    graph.get(other_host_url, payload={"value": [user("2")]})

    async with create_outlook_cloud_source() as source:
        with pytest.raises(ValueError, match="other.example.com"):
            [u async for u in source.client.get_users()]

    assert not requests_to(graph, other_host_url)


@pytest.mark.asyncio
async def test_get_users_raises_when_users_cannot_be_listed(graph):
    graph.get(USERS_URL, status=403)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(PermissionsMissing):
            [u async for u in source.client.get_users()]


@pytest.mark.asyncio
async def test_throttled_request_waits_for_retry_after(graph, patch_cancellable_sleeps):
    graph.get(PING_URL, status=429, headers={"Retry-After": "7"})
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()

    patch_cancellable_sleeps.assert_awaited_once_with(7)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 502, 503, 504])
async def test_retryable_status_without_retry_after_uses_doubling_default(
    graph, patch_cancellable_sleeps, status
):
    graph.get(PING_URL, status=status)
    graph.get(PING_URL, status=status)
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()

    delays = [call.args[0] for call in patch_cancellable_sleeps.await_args_list]
    assert delays == [DEFAULT_RETRY_SECONDS, DEFAULT_RETRY_SECONDS * 2]


@pytest.mark.asyncio
async def test_retries_are_limited_then_the_error_is_raised(
    graph, patch_cancellable_sleeps
):
    graph.get(PING_URL, status=503, repeat=True)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(ClientResponseError) as error:
            await source.ping()

    assert error.value.status == 503
    assert len(requests_to(graph, PING_URL)) == RETRIES
    assert patch_cancellable_sleeps.await_count == RETRIES - 1


@pytest.mark.asyncio
async def test_other_errors_are_not_retried(graph, patch_cancellable_sleeps):
    graph.get(PING_URL, status=500)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(ClientResponseError):
            await source.ping()

    assert len(requests_to(graph, PING_URL)) == 1
    patch_cancellable_sleeps.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ClientOSError(),
        ServerDisconnectedError(),
        ClientPayloadError(),
        asyncio.TimeoutError(),
    ],
)
async def test_network_faults_are_retried(graph, patch_cancellable_sleeps, error):
    graph.get(PING_URL, exception=error)
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()

    patch_cancellable_sleeps.assert_awaited_once_with(NETWORK_RETRY_SECONDS)


@pytest.mark.asyncio
async def test_network_faults_are_raised_when_retries_run_out(
    graph, patch_cancellable_sleeps
):
    graph.get(PING_URL, exception=ClientOSError(), repeat=True)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(ClientOSError):
            await source.ping()

    assert len(requests_to(graph, PING_URL)) == RETRIES
    assert patch_cancellable_sleeps.await_count == RETRIES - 1


@pytest.mark.asyncio
async def test_unauthorized_response_is_reported_as_missing_permissions(
    graph, patch_cancellable_sleeps
):
    graph.get(PING_URL, status=401)
    graph.get(mailbox_url("1"), status=401)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(PermissionsMissing, match="client secret"):
            await source.ping()
        with pytest.raises(PermissionsMissing, match="client secret"):
            await source.client.check_mailbox_access("1")

    patch_cancellable_sleeps.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, expected",
    [
        (200, MAILBOX_ACCESSIBLE),
        (403, MAILBOX_OUT_OF_SCOPE),
        (404, MAILBOX_MISSING),
    ],
)
async def test_check_mailbox_access(graph, status, expected):
    graph.get(mailbox_url("1"), status=status, payload={})

    async with create_outlook_cloud_source() as source:
        assert await source.client.check_mailbox_access("1") == expected

    assert len(requests_to(graph, mailbox_url("1"))) == 1


@pytest.mark.asyncio
async def test_check_mailbox_access_raises_on_other_errors(graph):
    graph.get(mailbox_url("1"), status=500)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(ClientResponseError):
            await source.client.check_mailbox_access("1")


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_skips_unreadable_users(graph):
    graph.get(
        USERS_URL,
        payload={
            "value": [
                user("ok"),
                user("denied"),
                user("no-mailbox"),
                user("no-mail", mail=None),
            ]
        },
    )
    graph.get(mailbox_url("ok"), payload={})
    graph.get(mailbox_url("denied"), status=403)
    graph.get(mailbox_url("no-mailbox"), status=404)

    async with create_outlook_cloud_source() as source:
        source.client.set_logger(MagicMock())
        mailboxes = [u async for u in source.client.get_accessible_mailboxes()]
        summary = source.client._logger.info.call_args.args[0]

    assert [u["id"] for u in mailboxes] == ["ok"]
    assert summary == (
        "Mailboxes: 1 readable, 1 outside the granted scope, 2 without a mailbox."
    )
    assert not requests_to(graph, mailbox_url("no-mail"))


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_fails_when_none_is_readable(graph):
    graph.get(USERS_URL, payload={"value": [user("1"), user("2")]})
    graph.get(mailbox_url("1"), status=403)
    graph.get(mailbox_url("2"), status=403)

    async with create_outlook_cloud_source() as source:
        with pytest.raises(NoAccessibleMailboxes, match="2 users"):
            [u async for u in source.client.get_accessible_mailboxes()]


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_fails_when_there_are_no_users(graph):
    graph.get(USERS_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        with pytest.raises(NoAccessibleMailboxes, match="no enabled users"):
            [u async for u in source.client.get_accessible_mailboxes()]


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_fails_when_no_user_has_a_mailbox(graph):
    graph.get(
        USERS_URL, payload={"value": [user("1", mail=None), user("2", mail=None)]}
    )

    async with create_outlook_cloud_source() as source:
        with pytest.raises(NoAccessibleMailboxes) as error:
            [u async for u in source.client.get_accessible_mailboxes()]

    assert "0 outside the granted scope, 2 without a mailbox" in str(error.value)
    assert "Mail.Read" not in str(error.value)


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_waits_for_all_probes_before_raising(graph):
    graph.get(USERS_URL, payload={"value": [user(str(i)) for i in range(5)]})
    finished = []

    async def check_mailbox_access(user_id):
        await asyncio.sleep(0)
        if user_id == "0":
            msg = "boom"
            raise RuntimeError(msg)
        for _ in range(5):  # slower than the failing probe
            await asyncio.sleep(0)
        finished.append(user_id)
        return MAILBOX_ACCESSIBLE

    async with create_outlook_cloud_source() as source:
        source.client.check_mailbox_access = check_mailbox_access
        with pytest.raises(RuntimeError, match="boom"):
            [u async for u in source.client.get_accessible_mailboxes()]
        # Checked before the source closes, while a leftover probe could still be running.
        finished_when_raised = sorted(finished)

    assert finished_when_raised == ["1", "2", "3", "4"]


@pytest.mark.asyncio
async def test_get_accessible_mailboxes_limits_concurrent_probes(graph):
    graph.get(USERS_URL, payload={"value": [user(str(i)) for i in range(8)]})
    running = 0
    peak = 0

    async def check_mailbox_access(_user_id):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0)
        running -= 1
        return MAILBOX_ACCESSIBLE

    async with create_outlook_cloud_source() as source:
        source.client.check_mailbox_access = check_mailbox_access
        with patch.object(outlook_cloud_client, "MAX_CONCURRENT_MAILBOXES", 3):
            mailboxes = [u async for u in source.client.get_accessible_mailboxes()]

    assert len(mailboxes) == 8
    assert peak == 3


@pytest.mark.asyncio
async def test_close_closes_the_session(graph):
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()
        session = source.client.session

        await source.close()

        assert session.closed


@pytest.mark.asyncio
async def test_close_does_not_create_a_client():
    async with create_outlook_cloud_source() as source:
        await source.close()

        assert "client" not in source.__dict__


def messages_url(user_id, folder_segment):
    return (
        f"{GRAPH_BASE_URL}/users/{user_id}/mailFolders/{folder_segment}/messages"
        f"?$select={MESSAGE_FIELDS}&$top=100"
    )


def graph_message(**overrides):
    message = {
        "id": "msg-1",
        "subject": "Dummy Subject",
        "from": {"emailAddress": {"address": "from@example.com"}},
        "sender": {"emailAddress": {"address": "sender@example.com"}},
        "replyTo": [],
        "toRecipients": [{"emailAddress": {"address": "to@example.com"}}],
        "ccRecipients": [],
        "bccRecipients": [],
        "sentDateTime": "2023-12-12T01:01:01Z",
        "lastModifiedDateTime": "2023-12-12T01:01:01Z",
        "internetMessageId": "<id@example.com>",
        "importance": "high",
        "categories": ["Outlook"],
        "body": {"contentType": "html", "content": "<p>This is a dummy mail</p>"},
    }
    message.update(overrides)
    return message


def register_accessible_user(graph, user_id="user-1", mail="user@example.com"):
    graph.get(USERS_URL, payload={"value": [user(user_id, mail=mail)]})
    graph.get(mailbox_url(user_id), payload={"id": "inbox-id"})


def register_default_folders_empty(graph, user_id="user-1"):
    for folder in ("inbox", "sentitems", "junkemail", "archive"):
        graph.get(messages_url(user_id, folder), payload={"value": []})


@pytest.mark.asyncio
async def test_get_docs_yields_inbox_mail_document(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    graph.get(
        messages_url("user-1", "inbox"),
        payload={"value": [graph_message()]},
    )

    async with create_outlook_cloud_source() as source:
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1
    assert documents[0]["type"] == INBOX_MAIL_OBJECT
    assert documents[0]["sender"] == "sender@example.com"
    assert documents[0]["importance"] == "High"
    assert documents[0]["message"] == "This is a dummy mail"
    assert isinstance(documents[0]["_attachment"], str)
    assert_conforms(documents[0])


@pytest.mark.asyncio
async def test_get_docs_skips_junk_when_folder_missing(graph):
    register_accessible_user(graph)
    graph.get(messages_url("user-1", "inbox"), payload={"value": []})
    graph.get(messages_url("user-1", "sentitems"), payload={"value": []})
    graph.get(messages_url("user-1", "junkemail"), status=404)
    graph.get(messages_url("user-1", "archive"), payload={"value": []})

    async with create_outlook_cloud_source() as source:
        source.client.set_logger(MagicMock())
        documents = [document async for document, _ in source.get_docs()]

    assert documents == []
    source.client._logger.warning.assert_any_call(
        "Could not read mail folder 'Junk Mails' for user user-1, skipping: ClientResponseError (404)."
    )


@pytest.mark.asyncio
async def test_get_docs_include_full_raw_message_fetches_value(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    graph.get(messages_url("user-1", "inbox"), payload={"value": [graph_message()]})
    raw_url = f"{GRAPH_BASE_URL}/users/user-1/messages/msg-1/$value"
    graph.get(raw_url, body=b"Subject: test\r\n\r\nbody")

    async with create_outlook_cloud_source(
        include_full_raw_message=True,
    ) as source:
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1
    assert requests_to(graph, raw_url)
    headers = requests_to(graph, raw_url)[0].kwargs["headers"]
    assert headers["Prefer"] == 'IdType="ImmutableId"'


@pytest.mark.asyncio
async def test_get_docs_raw_message_404_falls_back_to_minimal_eml(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    graph.get(messages_url("user-1", "inbox"), payload={"value": [graph_message()]})
    raw_url = f"{GRAPH_BASE_URL}/users/user-1/messages/msg-1/$value"
    graph.get(raw_url, status=404)

    async with create_outlook_cloud_source(
        include_full_raw_message=True,
    ) as source:
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1
    assert documents[0]["_attachment"]


@pytest.mark.asyncio
async def test_get_docs_raw_message_500_raises(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    graph.get(messages_url("user-1", "inbox"), payload={"value": [graph_message()]})
    raw_url = f"{GRAPH_BASE_URL}/users/user-1/messages/msg-1/$value"
    graph.get(raw_url, status=500)

    async with create_outlook_cloud_source(
        include_full_raw_message=True,
    ) as source:
        with pytest.raises(ClientResponseError):
            [document async for document, _ in source.get_docs()]


@pytest.mark.asyncio
async def test_graph_datetime_to_iso_utc_handles_fractional_seconds():
    from connectors.sources.outlook_cloud.utils import graph_datetime_to_iso_utc

    assert (
        graph_datetime_to_iso_utc("2023-12-12T01:01:01.1234567Z")
        == "2023-12-12T01:01:01Z"
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2023-12-12T01:01:01.1Z", "2023-12-12T01:01:01Z"),
        ("2023-12-12T01:01:01.12Z", "2023-12-12T01:01:01Z"),
        ("2023-12-12T01:01:01.123Z", "2023-12-12T01:01:01Z"),
        ("2023-12-12T01:01:01Z", "2023-12-12T01:01:01Z"),
        ("2023-12-12T03:01:01+02:00", "2023-12-12T01:01:01Z"),
        ("2023-12-12T03:01:01.5+0200", "2023-12-12T01:01:01Z"),
        ("", ""),
        (None, None),
    ],
)
def test_graph_datetime_to_iso_utc_accepts_graph_shapes(value, expected):
    from connectors.sources.outlook_cloud.utils import graph_datetime_to_iso_utc

    assert graph_datetime_to_iso_utc(value) == expected


def test_graph_datetime_to_iso_utc_rejects_garbage():
    from connectors.sources.outlook_cloud.utils import graph_datetime_to_iso_utc

    with pytest.raises(ValueError):
        graph_datetime_to_iso_utc("yesterday")


def test_minimal_eml_survives_newlines_and_bad_values():
    from email import message_from_bytes

    from connectors.sources.outlook_cloud.mail_eml import (
        build_minimal_eml_from_graph_message,
    )

    eml = build_minimal_eml_from_graph_message(
        graph_message(
            subject="line one\r\nBcc: evil@example.com",
            sentDateTime="not a date",
            **{"from": None},
        )
    )
    parsed = message_from_bytes(eml)

    assert parsed["Subject"] == "line one Bcc: evil@example.com"
    assert parsed["Bcc"] is None
    assert parsed["From"] == "sender@example.com"
    assert parsed["Date"] is None


@pytest.mark.asyncio
async def test_get_docs_handles_null_fields(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    graph.get(
        messages_url("user-1", "inbox"),
        payload={
            "value": [
                graph_message(
                    subject=None,
                    importance=None,
                    categories=None,
                    body=None,
                    toRecipients=None,
                    **{"from": None, "sender": None},
                )
            ]
        },
    )

    async with create_outlook_cloud_source() as source:
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1
    assert_conforms(documents[0])


@pytest.mark.asyncio
async def test_get_docs_follows_message_pages_with_headers(graph):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    next_url = f"{GRAPH_BASE_URL}/users/user-1/mailFolders/inbox/messages?$skiptoken=2"
    graph.get(
        messages_url("user-1", "inbox"),
        payload={"value": [graph_message()], "@odata.nextLink": next_url},
    )
    graph.get(next_url, payload={"value": [graph_message(id="msg-2")]})

    async with create_outlook_cloud_source() as source:
        documents = [document async for document, _ in source.get_docs()]

    assert [document["_id"] for document in documents] == ["msg-1", "msg-2"]
    for call in requests_to(graph, next_url):
        assert call.kwargs["headers"]["Prefer"] == 'IdType="ImmutableId"'


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 404])
async def test_get_docs_keeps_pages_read_when_a_later_page_is_refused(graph, status):
    register_accessible_user(graph)
    for folder in ("sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), payload={"value": []})
    next_url = f"{GRAPH_BASE_URL}/users/user-1/mailFolders/inbox/messages?$skiptoken=2"
    graph.get(
        messages_url("user-1", "inbox"),
        payload={"value": [graph_message()], "@odata.nextLink": next_url},
    )
    graph.get(next_url, status=status)

    async with create_outlook_cloud_source() as source:
        source.client.set_logger(MagicMock())
        documents = [document async for document, _ in source.get_docs()]

    assert [document["_id"] for document in documents] == ["msg-1"]
    source.client._logger.warning.assert_any_call(
        f"Could not read mail folder 'Inbox Mails' for user user-1, skipping: ClientResponseError ({status})."
    )


@pytest.mark.asyncio
async def test_get_docs_fails_when_no_folder_can_be_read(graph):
    register_accessible_user(graph)
    for folder in ("inbox", "sentitems", "junkemail", "archive"):
        graph.get(messages_url("user-1", folder), status=403)

    async with create_outlook_cloud_source() as source:
        source.client.set_logger(MagicMock())
        with pytest.raises(NoAccessibleMailboxes):
            [document async for document, _ in source.get_docs()]


@pytest.mark.asyncio
async def test_get_docs_does_not_list_folders_by_default(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)

    async with create_outlook_cloud_source() as source:
        [document async for document, _ in source.get_docs()]

    assert not [url for (_, url) in graph.requests if "childFolders" in str(url)]


# --- sync_all_mail_folders ---

ROOTS_URL = (
    f"{GRAPH_BASE_URL}/users/user-1/mailFolders?$select={FOLDER_FIELDS}&$top=100"
)


def children_url(folder_id):
    return (
        f"{GRAPH_BASE_URL}/users/user-1/mailFolders/{folder_id}"
        f"/childFolders?$select={FOLDER_FIELDS}&$top=100"
    )


def folder(folder_id, name=None, children=1, **extra):
    return {
        "id": folder_id,
        "displayName": name or folder_id,
        "childFolderCount": children,
        **extra,
    }


def register_well_known_ids(graph, overrides=None):
    """Every well-known folder resolves to `id-<name>`, unless overridden by a status."""
    overrides = overrides or {}
    names = [e["well_known"] for e in DEFAULT_MAIL_FOLDERS] + list(
        EXCLUDED_WELL_KNOWN_FOLDERS
    )
    for name in names:
        url = f"{GRAPH_BASE_URL}/users/user-1/mailFolders/{name}?$select=id"
        if name in overrides:
            graph.get(url, status=overrides[name])
        else:
            graph.get(url, payload={"id": f"id-{name}"})


@pytest.mark.asyncio
async def test_sync_all_mail_folders_indexes_only_user_folders(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)
    register_well_known_ids(graph)
    graph.get(
        ROOTS_URL,
        payload={
            "value": [
                folder("id-inbox"),
                folder("id-deleteditems", children=0),
                folder("id-drafts", children=1),
                folder("id-searchfolders", children=1),
                folder("proj", "Projects"),
                folder(
                    "saved",
                    "Saved",
                    children=0,
                    **{"@odata.type": "#microsoft.graph.mailSearchFolder"},
                ),
            ]
        },
    )
    # Subfolder of a default folder is user mail; the default itself is not repeated.
    graph.get(children_url("id-inbox"), payload={"value": [folder("sub", "Sub", 0)]})
    graph.get(children_url("proj"), payload={"value": [folder("deep", "Deep", 0)]})
    for folder_id in ("sub", "proj", "deep"):
        graph.get(
            messages_url("user-1", folder_id),
            payload={"value": [graph_message(id=f"msg-{folder_id}")]},
        )

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        documents = [document async for document, _ in source.get_docs()]

    assert {d["_id"]: d["folder_name"] for d in documents} == {
        "msg-sub": "Sub",
        "msg-proj": "Projects",
        "msg-deep": "Deep",
    }
    assert {d["type"] for d in documents} == {"Mail"}
    for document in documents:
        assert_conforms(document)
    # Excluded subtrees are never entered, and folders without children are not listed.
    for skipped in ("id-drafts", "id-searchfolders", "id-deleteditems", "sub", "deep"):
        assert not requests_to(graph, children_url(skipped))


@pytest.mark.asyncio
async def test_sync_all_mail_folders_follows_folder_pages(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)
    register_well_known_ids(graph)
    next_url = f"{GRAPH_BASE_URL}/users/user-1/mailFolders?$skiptoken=2"
    graph.get(
        ROOTS_URL,
        payload={"value": [folder("a", children=0)], "@odata.nextLink": next_url},
    )
    graph.get(next_url, payload={"value": [folder("b", children=0)]})
    for folder_id in ("a", "b"):
        graph.get(
            messages_url("user-1", folder_id),
            payload={"value": [graph_message(id=f"msg-{folder_id}")]},
        )

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        documents = [document async for document, _ in source.get_docs()]

    assert [d["_id"] for d in documents] == ["msg-a", "msg-b"]


@pytest.mark.asyncio
async def test_sync_all_mail_folders_skips_unreadable_folder(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)
    register_well_known_ids(graph)
    graph.get(
        ROOTS_URL,
        payload={"value": [folder("bad", children=0), folder("good", children=0)]},
    )
    graph.get(messages_url("user-1", "bad"), status=403)
    graph.get(messages_url("user-1", "good"), payload={"value": [graph_message()]})

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        source.client.set_logger(MagicMock())
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1
    source.client._logger.warning.assert_any_call(
        "Could not read mail folder 'bad' for user user-1, skipping: ClientResponseError (403)."
    )


@pytest.mark.asyncio
async def test_sync_all_mail_folders_tolerates_missing_system_folder(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)
    register_well_known_ids(graph, overrides={"archive": 404, "outbox": 404})
    graph.get(ROOTS_URL, payload={"value": [folder("a", children=0)]})
    graph.get(messages_url("user-1", "a"), payload={"value": [graph_message()]})

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 1


@pytest.mark.asyncio
async def test_sync_all_mail_folders_skips_extras_when_system_folders_are_unknown(
    graph,
):
    """A refused lookup must not turn Deleted Items or Drafts into indexed mail."""
    register_accessible_user(graph)
    for folder_name in ("inbox", "sentitems", "junkemail"):
        graph.get(
            messages_url("user-1", folder_name), payload={"value": [graph_message()]}
        )
    graph.get(messages_url("user-1", "archive"), payload={"value": []})
    register_well_known_ids(graph, overrides={"deleteditems": 403})

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        source.client.set_logger(MagicMock())
        documents = [document async for document, _ in source.get_docs()]

    assert len(documents) == 3  # the default folders are still synced
    assert not requests_to(graph, ROOTS_URL)


@pytest.mark.asyncio
async def test_sync_all_mail_folders_raises_on_server_errors(graph):
    register_accessible_user(graph)
    register_default_folders_empty(graph)
    register_well_known_ids(graph)
    graph.get(ROOTS_URL, status=500)

    async with create_outlook_cloud_source(sync_all_mail_folders=True) as source:
        with pytest.raises(ClientResponseError):
            [document async for document, _ in source.get_docs()]
