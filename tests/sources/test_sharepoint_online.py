#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import re
from datetime import datetime, timedelta, timezone
from functools import partial
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientResponseError

from connectors.protocol import Features
from connectors.sources.sharepoint_online import (
    ACCESS_CONTROL,
    DEFAULT_GROUPS,
    WILDCARD,
    GraphAPIToken,
    InternalServerError,
    InvalidSharepointTenant,
    MicrosoftAPISession,
    MicrosoftSecurityToken,
    NotFound,
    SharepointOnlineAdvancedRulesValidator,
    SharepointOnlineClient,
    SharepointOnlineDataSource,
    TokenFetchFailed,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

GROUP_2 = "Group 2"

GROUP_1 = "Group 1"

USER_2 = "User 2"

USER_1 = "User 1"

NUMBER_OF_DEFAULT_GROUPS = 3

ALLOW_ACCESS_CONTROL_PATCHED = "access_control"
DEFAULT_GROUPS_PATCHED = ["some default group"]


def set_dls_enabled(source, dls_enabled):
    source.set_features(Features({"document_level_security": {"enabled": dls_enabled}}))


class TestMicrosoftSecurityToken:
    class StubMicrosoftSecurityToken(MicrosoftSecurityToken):
        def __init__(self, bearer, expires_in):
            super().__init__(None, None, None, None, None)
            self.bearer = bearer
            self.expires_in = expires_in

        async def _fetch_token(self):
            return (self.bearer, self.expires_in)

    class StubMicrosoftSecurityTokenWrongConfig(MicrosoftSecurityToken):
        def __init__(self, error_code, message=None):
            super().__init__(None, None, None, None, None)
            self.error_code = error_code
            self.message = message

        async def _fetch_token(self):
            error = ClientResponseError(None, None)
            error.status = self.error_code
            error.message = self.message

            raise error

    @pytest.mark.asyncio
    async def test_fetch_token_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError) as e:
            mst = MicrosoftSecurityToken(None, None, None, None, None)

            await mst._fetch_token()

        assert e is not None

    @pytest.mark.asyncio
    async def test_get_returns_results_from_fetch_token(self):
        bearer = "something"
        expires_in = 0.1

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            bearer, expires_in
        )

        actual = await token.get()

        assert actual == bearer

    @pytest.mark.asyncio
    async def test_get_returns_cached_value_when_token_did_not_expire(self):
        original_bearer = "something"
        updated_bearer = "another"
        expires_in = 1

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            original_bearer, expires_in
        )

        first_bearer = await token.get()
        token.bearer = updated_bearer

        second_bearer = await token.get()

        assert first_bearer == second_bearer
        assert second_bearer == original_bearer

    @pytest.mark.asyncio
    async def test_get_returns_new_value_when_token_expired(self):
        original_bearer = "something"
        updated_bearer = "another"
        expires_in = 0.01

        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(
            original_bearer, expires_in
        )

        first_bearer = await token.get()
        token.bearer = updated_bearer

        await asyncio.sleep(expires_in + 0.01)

        second_bearer = await token.get()

        assert first_bearer != second_bearer
        assert second_bearer == updated_bearer

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_400(self):
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(400)

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match("Tenant Id")
        assert e.match("Tenant Name")
        assert e.match("Client ID")

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_401(self):
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(401)

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match("Secret Value")

    @pytest.mark.asyncio
    async def test_get_raises_correct_exception_when_any_other_status(self):
        message = "Internal server error"
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityTokenWrongConfig(
            500, message
        )

        with pytest.raises(TokenFetchFailed) as e:
            await token.get()

        # Assert that message has field names in UI
        assert e.match(message)


class TestGraphAPIToken:
    @pytest_asyncio.fixture
    async def token(self):
        session = aiohttp.ClientSession()

        yield GraphAPIToken(session, None, None, None, None)

        await session.close()

    @pytest.mark.asyncio
    async def test_fetch_token(self, token, mock_responses):
        bearer = "hello"
        expires_in = 15

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in

    @pytest.mark.asyncio
    async def test_fetch_token_retries(self, token, mock_responses, patch_sleep):
        bearer = "hello"
        expires_in = 15

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        mock_responses.post(re.compile(".*"), exception=first_request_error)

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in


class TestSharepointRestAPIToken:
    @pytest_asyncio.fixture
    async def token(self):
        session = aiohttp.ClientSession()

        yield GraphAPIToken(session, None, None, None, None)

        await session.close()

    @pytest.mark.asyncio
    async def test_fetch_token(self, token, mock_responses):
        bearer = "hello"
        expires_in = 15

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in

    # This test is a duplicate of test for TestGraphAPIToken.
    # When we introduce reusable retryable function instead of a wrapper
    # Then this test can be removed
    @pytest.mark.asyncio
    async def test_fetch_token_retries(self, token, mock_responses, patch_sleep):
        bearer = "hello"
        expires_in = 15

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        mock_responses.post(re.compile(".*"), exception=first_request_error)

        mock_responses.post(
            re.compile(".*"),
            payload={"access_token": bearer, "expires_in": str(expires_in)},
        )

        actual_token, actual_expires_in = await token._fetch_token()

        assert actual_token == bearer
        assert actual_expires_in == expires_in


class TestMicrosoftAPISession:
    class StubAPIToken:
        async def get(self):
            return "something"

    @pytest_asyncio.fixture
    async def microsoft_api_session(self):
        session = aiohttp.ClientSession()
        yield MicrosoftAPISession(
            session,
            TestMicrosoftAPISession.StubAPIToken(),
            self.scroll_field,
        )
        await session.close()

    @property
    def scroll_field(self):
        return "next_link"

    @pytest.mark.asyncio
    async def test_fetch(self, microsoft_api_session, mock_responses):
        url = "http://localhost:1234/url"
        payload = {"test": "hello world"}

        mock_responses.get(url, payload=payload)

        response = await microsoft_api_session.fetch(url)

        assert response == payload

    @pytest.mark.asyncio
    async def test_fetch_with_retry(
        self, microsoft_api_session, mock_responses, patch_sleep
    ):
        url = "http://localhost:1234/url"
        payload = {"test": "hello world"}

        first_request_error = ClientResponseError(None, None)
        first_request_error.status = 500
        first_request_error.message = "Something went wrong"

        # First error out, then on request to same resource return good payload
        mock_responses.get(url, exception=first_request_error)
        mock_responses.get(url, payload=payload)

        response = await microsoft_api_session.fetch(url)

        assert response == payload

    @pytest.mark.asyncio
    async def test_scroll(self, microsoft_api_session, mock_responses):
        url = "http://localhost:1234/url"
        first_page = ["1", "2", "3"]

        next_url = "http://localhost:1234/url/page-two"
        second_page = ["4", "5", "6"]

        first_payload = {"value": first_page, self.scroll_field: next_url}
        second_payload = {"value": second_page}

        mock_responses.get(url, payload=first_payload)
        mock_responses.get(next_url, payload=second_payload)

        pages = []

        async for page in microsoft_api_session.scroll(url):
            pages.append(page)

        assert first_page in pages
        assert second_page in pages


class TestSharepointOnlineClient:
    @property
    def tenant_id(self):
        return "tid"

    @property
    def tenant_name(self):
        return "tname"

    @property
    def client_id(self):
        return "cid"

    @property
    def client_secret(self):
        return "csecret"

    @pytest_asyncio.fixture
    async def client(self):
        # Patch close is passed here to also not do actual closing logic but instead
        # Do nothing when MicrosoftAPISession.close is called
        client = SharepointOnlineClient(
            self.tenant_id, self.tenant_name, self.client_id, self.client_secret
        )

        yield client
        await client.close()

    @pytest_asyncio.fixture
    def patch_fetch(self):
        with patch.object(
            MicrosoftAPISession, "fetch", return_value=AsyncMock()
        ) as fetch:
            yield fetch

    @pytest_asyncio.fixture
    async def patch_scroll(self):
        with patch.object(
            MicrosoftAPISession, "scroll", return_value=AsyncMock()
        ) as scroll:
            yield scroll

    @pytest_asyncio.fixture
    async def patch_pipe(self):
        with patch.object(
            MicrosoftAPISession, "pipe", return_value=AsyncMock()
        ) as pipe:
            yield pipe

    async def _execute_scrolling_method(self, method, patch_scroll, setup_items):
        half = len(setup_items) // 2
        patch_scroll.return_value = AsyncIterator(
            [setup_items[:half], setup_items[half:]]
        )  # simulate 2 pages

        returned_items = []
        async for item in method():
            returned_items.append(item)

        return returned_items

    async def _test_scrolling_method_not_found(self, method, patch_scroll):
        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for item in method():
            returned_items.append(item)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_groups(self, client, patch_scroll):
        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            client.groups, patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites(self, client, patch_scroll):
        group_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.group_sites, group_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites_not_found(self, client, patch_scroll):
        group_id = "12345"

        await self._test_scrolling_method_not_found(
            partial(client.group_sites, group_id), patch_scroll
        )

    @pytest.mark.asyncio
    async def test_site_collections(self, client, patch_scroll):
        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            client.site_collections, patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_sites_wildcard(self, client, patch_scroll):
        root_site = "root"
        actual_items = [
            {"name": "First"},
            {"name": "Second"},
            {"name": "Third"},
            {"name": "Fourth"},
        ]

        returned_items = await self._execute_scrolling_method(
            partial(client.sites, root_site, WILDCARD), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_sites_filter(self, client, patch_scroll):
        root_site = "root"
        actual_items = [
            {"name": "First"},
            {"name": "Second"},
            {"name": "Third"},
            {"name": "Fourth"},
        ]
        filter_ = ["First", "Third"]

        returned_items = await self._execute_scrolling_method(
            partial(client.sites, root_site, filter_), patch_scroll, actual_items
        )

        assert len(returned_items) == len(filter_)
        assert actual_items[0] in returned_items
        assert actual_items[2] in returned_items

    @pytest.mark.asyncio
    async def test_site_drives(self, client, patch_scroll):
        site_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_drives, site_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_drive_items_non_recursive(self, client, patch_fetch, patch_scroll):
        """Basic setup for the test - no recursion through directories"""
        pass

    @pytest.mark.asyncio
    async def test_download_drive_item(self, client, patch_pipe):
        """Basic setup for the test - no recursion through directories"""
        drive_id = "1"
        item_id = "2"
        async_buffer = MagicMock()

        await client.download_drive_item(drive_id, item_id, async_buffer)

        patch_pipe.assert_awaited_once_with(ANY, async_buffer)

    @pytest.mark.asyncio
    async def test_site_lists(self, client, patch_scroll):
        site_id = "12345"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_lists, site_id), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_site_list_items(self, client, patch_scroll):
        site_id = "12345"
        list_id = "54321"

        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_list_items, site_id, list_id),
            patch_scroll,
            actual_items,
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_site_list_item_attachments(self, client, patch_fetch):
        site_web_url = f"https://{self.tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        actual_attachments = ["file.txt", "o-file.txt", "third.txt", "hll.wrd"]

        patch_fetch.return_value = {"AttachmentFiles": actual_attachments}

        returned_items = []
        async for attachment in client.site_list_item_attachments(
            site_web_url, list_title, list_item_id
        ):
            returned_items.append(attachment)

        assert len(returned_items) == len(actual_attachments)
        assert returned_items == actual_attachments

    @pytest.mark.asyncio
    async def test_site_list_item_attachments_not_found(self, client, patch_fetch):
        site_web_url = f"https://{self.tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        patch_fetch.side_effect = NotFound()

        returned_items = []
        async for attachment in client.site_list_item_attachments(
            site_web_url, list_title, list_item_id
        ):
            returned_items.append(attachment)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_site_list_item_attachments_wrong_tenant(self, client):
        invalid_tenant_name = "something"
        site_web_url = f"https://{invalid_tenant_name}.sharepoint.com"
        list_title = "Summer Vacation Notes"
        list_item_id = "1"

        with pytest.raises(InvalidSharepointTenant) as e:
            async for _ in client.site_list_item_attachments(
                site_web_url, list_title, list_item_id
            ):
                pass

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_download_attachment(self, client, patch_pipe):
        attachment_path = f"https://{self.tenant_name}.sharepoint.com/thats/a/made/up/attachment/path.jpg"
        async_buffer = MagicMock()

        await client.download_attachment(attachment_path, async_buffer)

        patch_pipe.assert_awaited_once_with(ANY, async_buffer)

    @pytest.mark.asyncio
    async def test_download_attachment_wrong_tenant(self, client, patch_pipe):
        invalid_tenant_name = "something"
        attachment_path = f"https://{invalid_tenant_name}.sharepoint.com/thats/a/made/up/attachment/path.jpg"
        async_buffer = MagicMock()

        with pytest.raises(InvalidSharepointTenant) as e:
            await client.download_attachment(attachment_path, async_buffer)

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_site_pages(self, client, patch_scroll):
        page_url_path = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"
        actual_items = ["1", "2", "3", "4"]

        returned_items = await self._execute_scrolling_method(
            partial(client.site_pages, page_url_path), patch_scroll, actual_items
        )

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_site_pages_not_found(self, client, patch_scroll):
        page_url_path = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"

        patch_scroll.side_effect = NotFound()

        returned_items = []
        async for site_page in client.site_pages(page_url_path):
            returned_items.append(site_page)

        assert len(returned_items) == 0

    @pytest.mark.asyncio
    async def test_site_pages_wrong_tenant(self, client, patch_scroll):
        invalid_tenant_name = "something"
        page_url_path = f"https://{invalid_tenant_name}.sharepoint.com/random/totally/made/up/page.aspx"

        with pytest.raises(InvalidSharepointTenant) as e:
            async for _ in client.site_pages(page_url_path):
                pass

        # Assert error message contains both invalid and valid tenant name
        # cause that's what's important
        assert e.match(invalid_tenant_name)
        assert e.match(self.tenant_name)

    @pytest.mark.asyncio
    async def test_tenant_details(self, client, patch_fetch):
        http_call_result = {"hello": "world"}

        patch_fetch.return_value = http_call_result

        actual_result = await client.tenant_details()

        assert http_call_result == actual_result

    @pytest.mark.asyncio
    async def test_site_groups(self, client, patch_fetch):
        site_groups_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/sitegroups"
        groups = ["group"]

        patch_fetch.return_value = groups

        actual_groups = await client.site_groups(site_groups_url)

        assert actual_groups == groups

    @pytest.mark.asyncio
    async def test_site_groups_not_found(self, client, patch_fetch):
        site_groups_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/sitegroups"

        patch_fetch.side_effect = NotFound

        groups = await client.site_groups(site_groups_url)

        assert len(groups) == 0

    @pytest.mark.asyncio
    async def test_site_users(self, client, patch_fetch):
        site_users_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/siteusers"
        users = ["user"]

        patch_fetch.return_value = users

        actual_users = await client.site_users(site_users_url)

        assert actual_users == users

    @pytest.mark.asyncio
    async def test_site_users_not_found(self, client, patch_fetch):
        site_users_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/siteusers"

        patch_fetch.side_effect = NotFound

        users = await client.site_users(site_users_url)

        assert len(users) == 0

    @pytest.mark.asyncio
    async def test_drive_item_permissions(self, client, patch_fetch):
        drive_id = 1
        item_id = 2

        permissions = ["permission"]
        patch_fetch.return_value = permissions

        actual_permissions = await client.drive_item_permissions(drive_id, item_id)

        assert actual_permissions == permissions

    @pytest.mark.asyncio
    async def test_site_list_role_assignments(self, client, patch_fetch):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        role_assignments = {"value": ["role"]}
        patch_fetch.return_value = role_assignments

        actual_role_assignments = await client.site_list_role_assignments(
            site_list_role_assignments_url, site_list_name
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_list_role_assignments_not_found(self, client, patch_fetch):
        site_list_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_list_name = "site_list"

        patch_fetch.side_effect = NotFound

        role_assignments = await client.site_list_role_assignments(
            site_list_role_assignments_url, site_list_name
        )

        assert len(role_assignments) == 0

    @pytest.mark.asyncio
    async def test_site_list_item_role_assignments(self, client, patch_fetch):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        role_assignments = {"value": ["role"]}

        patch_fetch.return_value = role_assignments

        actual_role_assignments = await client.site_list_item_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_list_item_role_assignments_not_found(self, client, patch_fetch):
        site_list_item_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        list_title = "list_title"
        list_item_id = 1

        patch_fetch.side_effect = NotFound

        role_assignments = await client.site_list_item_role_assignments(
            site_list_item_role_assignments_url, list_title, list_item_id
        )

        assert len(role_assignments) == 0

    @pytest.mark.asyncio
    async def test_site_page_role_assignments(self, client, patch_fetch):
        site_page_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_page_id = 1
        role_assignments = {"value": ["role"]}

        patch_fetch.return_value = role_assignments

        actual_role_assignments = await client.site_page_role_assignments(
            site_page_role_assignments_url, site_page_id
        )

        assert actual_role_assignments == role_assignments

    @pytest.mark.asyncio
    async def test_site_page_role_assignments_not_found(self, client, patch_fetch):
        site_page_role_assignments_url = f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/roleassignments"
        site_page_id = 1

        patch_fetch.side_effect = NotFound

        role_assignments = await client.site_page_role_assignments(
            site_page_role_assignments_url, site_page_id
        )

        assert len(role_assignments) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment(self, client, patch_fetch):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}
        users_and_groups = ["user", "group"]

        patch_fetch.return_value = users_and_groups

        actual_users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert actual_users_and_groups == users_and_groups

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_not_found(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}

        patch_fetch.side_effect = NotFound

        users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(users_and_groups) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_internal_server_error(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )
        role_assignment = {"name": "role", "PrincipalId": 1}

        patch_fetch.side_effect = InternalServerError

        users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(users_and_groups) == 0

    @pytest.mark.asyncio
    async def test_users_and_groups_for_role_assignment_missing_principal_id(
        self, client, patch_fetch
    ):
        users_by_id_url = (
            f"https://{self.tenant_name}.sharepoint.com/random/totally/made/up/users"
        )

        # missing principal id
        role_assignment = {"name": "role"}
        users_and_groups = ["user", "group"]

        patch_fetch.return_value = users_and_groups

        actual_users_and_groups = await client.users_and_groups_for_role_assignment(
            users_by_id_url, role_assignment
        )

        assert len(actual_users_and_groups) == 0


class TestSharepointOnlineAdvancedRulesValidator:
    @pytest_asyncio.fixture
    def validator(self):
        return SharepointOnlineAdvancedRulesValidator()

    @pytest.mark.asyncio
    async def test_validate(self, validator):
        valid_rules = {"maxDataAge": 15}

        result = await validator.validate(valid_rules)

        assert result.is_valid

    @pytest.mark.asyncio
    async def test_validate_invalid_rule(self, validator):
        invalid_rules = {"maxDataAge": "why is this a string"}

        result = await validator.validate(invalid_rules)

        assert not result.is_valid


class TestSharepointOnlineDataSource:
    @property
    def month_ago(self):
        return datetime.now(timezone.utc) - timedelta(days=30)

    @property
    def day_ago(self):
        return datetime.now(timezone.utc) - timedelta(days=1)

    @property
    def site_collections(self):
        return [
            {
                "siteCollection": {"hostname": "test.sharepoint.com"},
                "webUrl": "https://test.sharepoint.com",
            }
        ]

    @property
    def sites(self):
        return [
            {
                "id": "1",
                "webUrl": "https://test.sharepoint.com/site-1",
                "name": "site-1",
            }
        ]

    @property
    def site_drives(self):
        return [{"id": "2"}]

    @property
    def drive_items(self):
        return [
            {"id": "3", "lastModifiedDateTime": self.month_ago},
            {"id": "4", "lastModifiedDateTime": self.day_ago},
        ]

    @property
    def site_lists(self):
        return [{"id": "5", "name": "My test list"}]

    @property
    def site_list_items(self):
        return [
            {
                "id": "6",
                "contentType": {"name": "Item"},
                "fields": {"Attachments": ""},
                "lastModifiedDateTime": self.month_ago,
            },
            {
                "id": "7",
                "contentType": {"name": "Web Template Extensions"},
                "fields": {},
            },  # Will be ignored!!!
            {
                "id": "8",
                "contentType": {"name": "Something without attachments"},
                "fields": {},
            },
        ]

    @property
    def site_list_item_attachments(self):
        return [
            {"odata.id": "9", "name": "attachment 1.txt"},
            {"odata.id": "10", "name": "attachment 2.txt"},
        ]

    @property
    def site_pages(self):
        return [{"Id": 4, "odata.id": "11", "GUID": "thats-not-a-guid"}]

    @property
    def site_groups(self):
        return {"value": [{"Title": GROUP_1}, {"Title": GROUP_2}, {}, {"Title": None}]}

    @property
    def site_users(self):
        return {
            "value": [
                {"UserPrincipalName": USER_1},
                {"UserPrincipalName": USER_2},
                {},
                {"UserPrincipalName": None},
            ]
        }

    @property
    def drive_item_permissions(self):
        return {
            # three valid values: GROUP_1 (1x, will be de-deduplicated), USER_1 and USER_2
            "value": [
                {"grantedToV2": {"user": {"email": USER_1}}},
                {
                    "grantedToV2": {"siteGroup": {"loginName": GROUP_1}},
                    "grantedTo": {"siteGroup": {"loginName": GROUP_1}},
                },
                {"grantedTo": {"user": {"email": USER_2}}},
                {
                    "grantedTo": {
                        "user": {"email": None},
                        "siteGroup": {"loginName": None},
                    },
                    "grantedToV2": {
                        "user": {"email": None},
                        "siteGroup": {"loginName": None},
                    },
                },
                {
                    "grantedTo": {"user": {}, "siteGroup": {}},
                    "grantedToV2": {"user": {}, "siteGroup": {}},
                },
                {
                    "grantedTo": {"user": None, "siteGroup": None},
                    "grantedToV2": {"user": None, "siteGroup": None},
                },
                {"grantedTo": {}, "grantedToV2": {}},
                {"grantedTo": None, "grantedToV2": None},
                {},
                None,
            ]
        }

    @property
    def site_list_role_assignments(self):
        return {"value": ["role"]}

    @property
    def users_and_groups_for_role_assignments(self):
        return [USER_1, GROUP_1]

    @property
    def site_list_item_role_assignments(self):
        return {"value": ["role"]}

    @property
    def site_page_role_assignments(self):
        return {"value": ["role"]}

    @property
    def graph_api_token(self):
        return "graph bearer"

    @property
    def rest_api_token(self):
        return "rest bearer"

    @property
    def valid_tenant(self):
        return {"NameSpaceType": "VALID"}

    @pytest_asyncio.fixture
    async def patch_sharepoint_client(self):
        client = AsyncMock()

        with patch(
            "connectors.sources.sharepoint_online.SharepointOnlineClient",
            return_value=AsyncMock(),
        ) as new_mock:
            client = new_mock.return_value
            client.site_collections = AsyncIterator(self.site_collections)
            client.sites = AsyncIterator(self.sites)
            client.site_groups = AsyncMock(return_value=self.site_groups)
            client.site_users = AsyncMock(return_value=self.site_users)
            client.drive_item_permissions = AsyncMock(
                return_value=self.drive_item_permissions
            )
            client.site_list_role_assignments = AsyncMock(
                return_value=self.site_list_role_assignments
            )
            client.site_list_item_role_assignments = AsyncMock(
                return_value=self.site_list_item_role_assignments
            )
            client.site_page_role_assignments = AsyncMock(
                return_value=self.site_page_role_assignments
            )
            client.users_and_groups_for_role_assignment = AsyncMock(
                return_value=self.users_and_groups_for_role_assignments
            )
            client.site_drives = AsyncIterator(self.site_drives)
            client.drive_items = AsyncIterator(self.drive_items)
            client.site_lists = AsyncIterator(self.site_lists)
            client.site_list_items = AsyncIterator(self.site_list_items)
            client.site_list_item_attachments = AsyncIterator(
                self.site_list_item_attachments
            )
            client.site_pages = AsyncIterator(self.site_pages)

            client.graph_api_token = AsyncMock()
            client.graph_api_token.get.return_value = self.graph_api_token
            client.rest_api_token = AsyncMock()
            client.rest_api_token.get.return_value = self.rest_api_token

            client.tenant_details = AsyncMock(return_value=self.valid_tenant)

            yield client

    @pytest.mark.asyncio
    async def test_get_docs_without_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        source._dls_enabled = Mock(return_value=False)

        results = []
        downloads = []
        async for doc, download_func in source.get_docs():
            results.append(doc)

            if download_func:
                downloads.append(download_func)

        assert len(results) == 11
        assert len(
            [i for i in results if i["object_type"] == "site_collection"]
        ) == len(self.site_collections)
        assert len([i for i in results if i["object_type"] == "site"]) == len(
            self.sites
        )
        assert len([i for i in results if i["object_type"] == "site_drive"]) == len(
            self.site_drives
        )
        assert len([i for i in results if i["object_type"] == "drive_item"]) == len(
            self.drive_items
        )
        assert len([i for i in results if i["object_type"] == "site_list"]) == len(
            self.site_lists
        )
        assert (
            len([i for i in results if i["object_type"] == "list_item"])
            == len(self.site_list_items) - 1
        )  # -1 because one of them is ignored!
        assert len(
            [i for i in results if i["object_type"] == "list_item_attachment"]
        ) == len(self.site_list_item_attachments)
        assert len([i for i in results if i["object_type"] == "site_page"]) == len(
            self.site_pages
        )

    @pytest.mark.asyncio
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    async def test_get_docs_with_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        source._dls_enabled = Mock(return_value=True)

        results = []
        downloads = []
        async for doc, download_func in source.get_docs():
            results.append(doc)

            if download_func:
                downloads.append(download_func)

        site_collections = [i for i in results if i["object_type"] == "site_collection"]
        sites = [i for i in results if i["object_type"] == "site"]
        site_drives = [i for i in results if i["object_type"] == "site_drive"]
        drive_items = [i for i in results if i["object_type"] == "drive_item"]
        site_lists = [i for i in results if i["object_type"] == "site_list"]
        list_items = [i for i in results if i["object_type"] == "list_item"]
        list_item_attachments = [
            i for i in results if i["object_type"] == "list_item_attachment"
        ]
        site_pages = [i for i in results if i["object_type"] == "site_page"]

        assert len(results) == 11

        assert len(site_collections) == len(self.site_collections)
        assert all(
            [
                ALLOW_ACCESS_CONTROL_PATCHED in site_collection
                for site_collection in site_collections
            ]
        )

        assert len(sites) == len(self.sites)
        assert all([ALLOW_ACCESS_CONTROL_PATCHED in site for site in sites])

        assert len(site_drives) == len(self.site_drives)
        assert all(
            [ALLOW_ACCESS_CONTROL_PATCHED in site_drive for site_drive in site_drives]
        )

        assert len(drive_items) == len(self.drive_items)
        assert all(
            [ALLOW_ACCESS_CONTROL_PATCHED in drive_item for drive_item in drive_items]
        )

        assert len(site_lists) == len(self.site_lists)
        assert all(
            [ALLOW_ACCESS_CONTROL_PATCHED in site_list for site_list in site_lists]
        )

        assert (
            len(list_items) == len(self.site_list_items) - 1
        )  # -1 because one of them is ignored!
        assert all(
            [ALLOW_ACCESS_CONTROL_PATCHED in list_item for list_item in list_items]
        )

        assert len(list_item_attachments) == len(self.site_list_item_attachments)
        assert all(
            [
                ALLOW_ACCESS_CONTROL_PATCHED in list_item_attachment
                for list_item_attachment in list_item_attachments
            ]
        )

        assert len(site_pages) == len(self.site_pages)
        assert all(
            [ALLOW_ACCESS_CONTROL_PATCHED in site_page for site_page in site_pages]
        )

    def test_get_default_configuration(self):
        config = SharepointOnlineDataSource.get_default_configuration()

        assert config is not None

    @pytest.mark.asyncio
    async def test_validate_config(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource, site_collections=WILDCARD)

        await source.validate_config()

        # Assert that tokens are awaited
        # They raise human-readable errors if something goes wrong
        # Therefore it's important
        patch_sharepoint_client.graph_api_token.get.assert_awaited()
        patch_sharepoint_client.rest_api_token.get.assert_awaited()

    @pytest.mark.asyncio
    async def test_validate_config_when_invalid_tenant(self, patch_sharepoint_client):
        invalid_tenant_name = "wat"

        source = create_source(
            SharepointOnlineDataSource,
            tenant_name=invalid_tenant_name,
            site_collections=WILDCARD,
        )
        patch_sharepoint_client.tenant_details.return_value = {
            "NameSpaceType": "Unknown"
        }

        with pytest.raises(Exception) as e:
            await source.validate_config()

        assert e.match(invalid_tenant_name)

    @pytest.mark.asyncio
    async def test_validate_config_non_existing_collection(
        self, patch_sharepoint_client
    ):
        non_existing_site = "something"
        another_non_existing_site = "something-something"

        source = create_source(
            SharepointOnlineDataSource,
            site_collections=[non_existing_site, another_non_existing_site],
        )

        with pytest.raises(Exception) as e:
            await source.validate_config()

        # Says which site does not exist
        assert e.match(non_existing_site)
        assert e.match(another_non_existing_site)

    @pytest.mark.asyncio
    async def test_get_attachment_content(self, patch_sharepoint_client):
        attachment = {"odata.id": "1", "_original_filename": "file.ppt"}
        message = b"This is content of attachment"

        async def download_func(attachment_id, async_buffer):
            await async_buffer.write(message)

        patch_sharepoint_client.download_attachment = download_func
        source = create_source(SharepointOnlineDataSource)

        download_result = await source.get_attachment_content(attachment, doit=True)

        assert download_result["_attachment"] == base64.b64encode(message).decode()
        assert "body" not in download_result

    @pytest.mark.asyncio
    async def test_get_attachment_with_text_extraction_enabled_adds_body(
        self, patch_sharepoint_client
    ):
        attachment = {"odata.id": "1", "_original_filename": "file.ppt"}
        message = "This is the text content of drive item"

        with patch(
            "connectors.utils.ExtractionService.extract_text", return_value=message
        ) as extraction_service_mock:

            async def download_func(attachment_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_attachment = download_func
            source = create_source(
                SharepointOnlineDataSource, use_text_extraction_service=True
            )

            download_result = await source.get_attachment_content(attachment, doit=True)

            extraction_service_mock.assert_called_once()
            assert download_result["body"] == message
            assert "_attachment" not in download_result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "filesize, expect_download", [(15, True), (10485761, False)]
    )
    async def test_get_drive_item_content(
        self, patch_sharepoint_client, filesize, expect_download
    ):
        drive_item = {
            "id": "1",
            "size": filesize,
            "lastModifiedDateTime": datetime.now(timezone.utc),
            "parentReference": {"driveId": "drive-1"},
            "_original_filename": "file.txt",
        }
        message = b"This is content of drive item"

        async def download_func(drive_id, drive_item_id, async_buffer):
            await async_buffer.write(message)

        patch_sharepoint_client.download_drive_item = download_func
        source = create_source(SharepointOnlineDataSource)

        download_result = await source.get_drive_item_content(drive_item, doit=True)

        if expect_download:
            assert download_result["_attachment"] == base64.b64encode(message).decode()
            assert "body" not in download_result
        else:
            assert download_result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("filesize", [(15), (10485761)])
    async def test_get_content_with_text_extraction_enabled_adds_body(
        self, patch_sharepoint_client, filesize
    ):
        drive_item = {
            "id": "1",
            "size": filesize,
            "lastModifiedDateTime": datetime.now(timezone.utc),
            "parentReference": {"driveId": "drive-1"},
            "_original_filename": "file.txt",
        }
        message = "This is the text content of drive item"

        with patch(
            "connectors.utils.ExtractionService.extract_text", return_value=message
        ) as extraction_service_mock:

            async def download_func(drive_id, drive_item_id, async_buffer):
                await async_buffer.write(bytes(message, "utf-8"))

            patch_sharepoint_client.download_drive_item = download_func
            source = create_source(
                SharepointOnlineDataSource, use_text_extraction_service=True
            )

            download_result = await source.get_drive_item_content(drive_item, doit=True)

            extraction_service_mock.assert_called_once()
            assert download_result["body"] == message
            assert "_attachment" not in download_result

    @pytest.mark.asyncio
    async def test_with_site_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, True)
        patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

        site = {"Id": 1, "webUrl": "some url"}

        site_with_access_control = await source._with_site_access_control(site)
        access_control = site_with_access_control[ACCESS_CONTROL]

        two_users = 2
        two_groups = 2

        assert len(access_control) == NUMBER_OF_DEFAULT_GROUPS + two_groups + two_users
        assert USER_1 in access_control
        assert USER_2 in access_control
        assert GROUP_1 in access_control
        assert GROUP_2 in access_control

    @pytest.mark.asyncio
    async def test_with_drive_item_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, True)
        site_drive = {"id": 1}
        drive_item = {"id": 2}

        drive_item_with_access_control = await source._with_drive_item_access_control(
            site_drive, drive_item
        )
        access_control = drive_item_with_access_control[ACCESS_CONTROL]

        two_users = 2
        one_group = 1

        assert len(access_control) == NUMBER_OF_DEFAULT_GROUPS + two_users + one_group
        assert all(
            [default_group in access_control for default_group in DEFAULT_GROUPS]
        )
        assert USER_1 in access_control
        assert USER_2 in access_control
        assert GROUP_1 in access_control

    @pytest.mark.asyncio
    async def test_with_site_list_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, True)
        patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

        site_web_url = "some url"
        site_list = {"name": "site_list"}

        site_list_with_access_control = await source._with_site_list_access_control(
            site_web_url, site_list
        )
        access_control = site_list_with_access_control[ACCESS_CONTROL]

        one_user = 1
        one_group = 1

        assert len(access_control) == NUMBER_OF_DEFAULT_GROUPS + one_user + one_group
        assert all(
            [default_group in access_control for default_group in DEFAULT_GROUPS]
        )
        assert USER_1 in access_control
        assert GROUP_1 in access_control

    @pytest.mark.asyncio
    async def test_with_site_page_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, True)
        patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

        site_web_url = "some url"
        site_page = {"Id": 1}

        site_page_with_access_control = await source._with_site_page_access_control(
            site_web_url, site_page
        )
        access_control = site_page_with_access_control[ACCESS_CONTROL]

        one_user = 1
        one_group = 1

        assert len(access_control) == NUMBER_OF_DEFAULT_GROUPS + one_user + one_group
        assert all(
            [default_group in access_control for default_group in DEFAULT_GROUPS]
        )
        assert USER_1 in access_control
        assert GROUP_1 in access_control

    @pytest.mark.asyncio
    async def test_access_control_for_role_assignments(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

        site_web_url = "some url"
        role_assignments = {"value": ["role"]}

        access_control = await source._access_control_for_role_assignments(
            site_web_url, role_assignments
        )

        one_user = 1
        one_group = 1

        assert len(access_control) == one_user + one_group
        assert USER_1 in access_control
        assert GROUP_1 in access_control

    @pytest.mark.asyncio
    async def test_with_list_item_access_control(self, patch_sharepoint_client):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, True)
        patch_sharepoint_client._validate_sharepoint_rest_url = Mock()

        site_web_url = "some url"
        site_list_name = "site_list"
        list_item = {"id": 1}

        list_item_with_access_control = await source._with_list_item_access_control(
            site_web_url, site_list_name, list_item
        )
        access_control = list_item_with_access_control[ACCESS_CONTROL]

        one_user = 1
        one_group = 1

        assert len(access_control) == NUMBER_OF_DEFAULT_GROUPS + one_user + one_group
        assert USER_1 in access_control
        assert GROUP_1 in access_control

    @pytest.mark.parametrize(
        "dls_enabled, document, access_control, expected_decorated_document",
        [
            (
                False,
                {},
                [USER_1],
                {},
            ),
            (
                True,
                {},
                [USER_1],
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_1, *DEFAULT_GROUPS_PATCHED]},
            ),
            (True, {}, [], {ALLOW_ACCESS_CONTROL_PATCHED: DEFAULT_GROUPS_PATCHED}),
            (
                True,
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_1]},
                [USER_2],
                {
                    ALLOW_ACCESS_CONTROL_PATCHED: [
                        USER_1,
                        USER_2,
                        *DEFAULT_GROUPS_PATCHED,
                    ]
                },
            ),
            (
                True,
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_1]},
                [],
                {ALLOW_ACCESS_CONTROL_PATCHED: [USER_1, *DEFAULT_GROUPS_PATCHED]},
            ),
        ],
    )
    @patch(
        "connectors.sources.sharepoint_online.ACCESS_CONTROL",
        ALLOW_ACCESS_CONTROL_PATCHED,
    )
    @patch(
        "connectors.sources.sharepoint_online.DEFAULT_GROUPS", DEFAULT_GROUPS_PATCHED
    )
    def test_decorate_with_access_control(
        self, dls_enabled, document, access_control, expected_decorated_document
    ):
        source = create_source(SharepointOnlineDataSource)
        set_dls_enabled(source, dls_enabled)
        decorated_document = source._decorate_with_access_control(
            document, access_control
        )

        assert (
            decorated_document.get(ALLOW_ACCESS_CONTROL_PATCHED, []).sort()
            == expected_decorated_document.get(ALLOW_ACCESS_CONTROL_PATCHED, []).sort()
        )
