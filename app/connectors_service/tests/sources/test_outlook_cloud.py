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
    DEFAULT_RETRY_SECONDS,
    GRAPH_BASE_URL,
    MAILBOX_ACCESSIBLE,
    MAILBOX_MISSING,
    MAILBOX_OUT_OF_SCOPE,
    RETRIES,
)
from connectors.sources.shared.microsoft.graph import (
    PermissionsMissing,
    TokenFetchFailed,
)
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


def create_outlook_cloud_source():
    return create_source(
        OutlookCloudDataSource,
        tenant_id=TENANT_ID,
        client_id="client-id",
        client_secret="client-secret",
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

    assert list(config.to_dict()) == ["tenant_id", "client_id", "client_secret"]
    assert config.to_dict()["client_secret"]["sensitive"] is True


def test_outlook_cloud_is_registered():
    assert (
        _default_config()["sources"]["outlook_cloud"]
        == "connectors.sources.outlook_cloud:OutlookCloudDataSource"
    )


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
@pytest.mark.parametrize("status", [429, 503, 504])
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
    [ClientOSError(), ServerDisconnectedError(), ClientPayloadError(), TimeoutError()],
)
async def test_network_faults_are_retried(graph, patch_cancellable_sleeps, error):
    graph.get(PING_URL, exception=error)
    graph.get(PING_URL, payload={"value": []})

    async with create_outlook_cloud_source() as source:
        await source.ping()

    patch_cancellable_sleeps.assert_awaited_once_with(DEFAULT_RETRY_SECONDS)


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


@pytest.mark.asyncio
async def test_get_docs_is_not_implemented_yet():
    async with create_outlook_cloud_source() as source:
        with pytest.raises(NotImplementedError, match="does not sync content yet"):
            async for _ in source.get_docs():
                pass
