#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from functools import partial
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch, ANY, MagicMock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import StreamReader

from connectors.source import DataSourceConfiguration
from connectors.sources.sharepoint_online import (
    GraphAPIToken,
    MicrosoftAPISession,
    MicrosoftSecurityToken,
    NotFound,
    SharepointOnlineClient,
    SharepointOnlineDataSource,
    SharepointRestAPIToken,
    WILDCARD
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source


class TestMicrosoftSecurityToken:
    class StubMicrosoftSecurityToken(MicrosoftSecurityToken):
        def __init__(self, bearer, expires_in):
            super().__init__(None, None, None, None, None)
            self.bearer = bearer
            self.expires_in = expires_in

        async def _fetch_token(self):
            return (self.bearer, self.expires_in)

    @pytest.mark.asyncio
    async def test_get_returns_results_from_fetch_token(self):
        bearer = 'something'
        expires_in = 0.1
        
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(bearer, expires_in)

        actual = await token.get()

        assert actual == bearer

    @pytest.mark.asyncio
    async def test_get_returns_cached_value_when_token_did_not_expire(self):
        original_bearer = 'something'
        updated_bearer = 'another'
        expires_in = 1
        
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(original_bearer, expires_in)

        first_bearer = await token.get()
        token.bearer = updated_bearer

        second_bearer = await token.get()

        assert first_bearer == second_bearer
        assert second_bearer == original_bearer

    @pytest.mark.asyncio
    async def test_get_returns_new_value_when_token_expired(self):
        original_bearer = 'something'
        updated_bearer = 'another'
        expires_in = 0.01
        
        token = TestMicrosoftSecurityToken.StubMicrosoftSecurityToken(original_bearer, expires_in)

        first_bearer = await token.get()
        token.bearer = updated_bearer

        await asyncio.sleep(expires_in + 0.01)

        second_bearer = await token.get()

        assert first_bearer != second_bearer
        assert second_bearer == updated_bearer


class TestSharepointOnlineClient:
    @pytest_asyncio.fixture
    async def client(self):
        client = SharepointOnlineClient("tid", "tname", "cid", "csecret")
        yield client
        await client.close()

    @pytest_asyncio.fixture
    async def patch_fetch(self):
        with patch.object(MicrosoftAPISession, "fetch", return_value=AsyncMock()) as fetch:
            yield fetch

    @pytest_asyncio.fixture
    async def patch_scroll(self):
        with patch.object(MicrosoftAPISession, "scroll", return_value=AsyncMock()) as scroll:
            yield scroll

    @pytest_asyncio.fixture
    async def patch_pipe(self):
        with patch.object(MicrosoftAPISession, "pipe", return_value=AsyncMock()) as pipe:
            yield pipe

    async def _execute_scrolling_method(self, method, patch_scroll, setup_items):
        half = len(setup_items)//2
        patch_scroll.return_value = AsyncIterator([setup_items[:half], setup_items[half:]]) # simulate 2 pages

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
        actual_items = ['1', '2', '3', '4']

        returned_items = await self._execute_scrolling_method(client.groups, patch_scroll, actual_items)

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites(self, client, patch_scroll):
        group_id = "12345"

        actual_items = ['1', '2', '3', '4']

        returned_items = await self._execute_scrolling_method(partial(client.group_sites, group_id), patch_scroll, actual_items)

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_group_sites_not_found(self, client, patch_scroll):
        group_id = "12345"

        await self._test_scrolling_method_not_found(partial(client.group_sites, group_id), patch_scroll)

    @pytest.mark.asyncio
    async def test_site_collections(self, client, patch_scroll):
        actual_items = ['1', '2', '3', '4']

        returned_items = await self._execute_scrolling_method(client.site_collections, patch_scroll, actual_items)

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_sites_wildcard(self, client, patch_scroll):
        root_site = "root"
        actual_items = [{ "name": "First" }, {"name": "Second"}, { "name": "Third" }, { "name": "Fourth"}]

        returned_items = await self._execute_scrolling_method(partial(client.sites, root_site, WILDCARD), patch_scroll, actual_items)

        assert len(returned_items) == len(actual_items)
        assert returned_items == actual_items

    @pytest.mark.asyncio
    async def test_sites_filter(self, client, patch_scroll):
        root_site = "root"
        actual_items = [{ "name": "First" }, {"name": "Second"}, { "name": "Third" }, { "name": "Fourth"}]
        filter_ = ["First", "Third"]

        returned_items = await self._execute_scrolling_method(partial(client.sites, root_site, filter_), patch_scroll, actual_items)

        assert len(returned_items) == len(filter_)
        assert actual_items[0] in returned_items
        assert actual_items[2] in returned_items

    @pytest.mark.asyncio
    async def test_site_drives(self, client, patch_scroll):
        site_id = "12345"

        actual_items = ['1', '2', '3', '4']

        returned_items = await self._execute_scrolling_method(partial(client.site_drives, site_id), patch_scroll, actual_items)

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
