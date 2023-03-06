#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the ServiceNow source class methods"""
from unittest import mock

import aiohttp
import pytest

from connectors.source import DataSourceConfiguration
from connectors.sources.servicenow import ServiceNowDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator


class MockResponse:
    """Mock response of aiohttp get method"""

    def __init__(self, res, headers):
        """Setup a response"""
        self._res = res
        self.headers = headers
        self.content = StreamerReader(self._res)

    async def read(self):
        """Method to read response"""
        return self._res

    async def __aenter__(self):
        """Enters an async with block"""
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Closes an async with block"""
        pass


class StreamerReader:
    """Mock Stream Reader"""

    def __init__(self, res):
        """Setup a response"""
        self._res = res
        self._size = None

    async def iter_chunks(self):
        """Method to iterate over content"""
        yield self._res, self._size


def test_get_configuration(patch_logger):
    # Setup
    klass = ServiceNowDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["username"] == "admin"


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_concurrent(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)
    source.concurrent_downloads = 100

    # Execute
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_without_username(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)
    source.configuration.set_field(name="username", value="")

    # Execute
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
async def test_close_with_client_session(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_ping_for_successful_connection(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)

    # Execute
    with mock.patch.object(
        ServiceNowDataSource,
        "_api_call",
        return_value=AsyncIterator([{"service_label": "service_name"}]),
    ):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_for_unsuccessful_connection(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)

    # Execute
    with mock.patch.object(
        ServiceNowDataSource, "_api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_tweak_bulk_options():
    # Setup
    source = create_source(ServiceNowDataSource)
    options = {}
    options["concurrent_downloads"] = 10

    # Execute
    source.tweak_bulk_options(options)


@pytest.mark.asyncio
async def test_api_call(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    response_list = []
    with mock.patch.object(
        source.session,
        "get",
        side_effect=[
            MockResponse(
                res=b'{"result": [{"id": "record_1"}]}',
                headers={
                    "Connection": "Keep-alive",
                    "Content-Type": "application/json",
                },
            ),
            MockResponse(
                res=b'{"result": []}',
                headers={
                    "Connection": "Keep-alive",
                    "Content-Type": "application/json",
                },
            ),
        ],
    ):
        async for response in source._api_call(
            url="/test", params={"sysparm_query": "label=Incident"}
        ):
            response_list.append(response)

    # Assert
    assert [{"id": "record_1"}] in response_list


@pytest.mark.asyncio
async def test_api_call_with_attachment(patch_logger):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    response_list = []
    with mock.patch.object(
        source.session,
        "get",
        return_value=MockResponse(
            res=b"Attachment Content",
            headers={"Content-Type": "application/json"},
        ),
    ):
        async for response in source._api_call(
            url="/test", params={}, is_attachment=True
        ):
            response_list.append(response)

    # Assert
    assert await response_list[0].read() == b"Attachment Content"


@pytest.fixture
def patch_default_retry_interval():
    """Patch retry interval to 0"""

    with mock.patch("connectors.sources.servicenow.RETRY_INTERVAL", 0):
        yield


@pytest.mark.asyncio
async def test_api_call_with_retry(patch_logger, patch_default_retry_interval):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    with pytest.raises(Exception):
        with mock.patch.object(
            source.session, "get", side_effect=aiohttp.ServerDisconnectedError()
        ):
            async for response in source._api_call(url="/test", params={}):
                pass


@pytest.mark.asyncio
async def test_api_call_with_close_connection(
    patch_logger, patch_default_retry_interval
):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    with pytest.raises(Exception):
        with mock.patch.object(
            source.session,
            "get",
            return_value=MockResponse(
                res=b'{"result": [{"id": "record_1"}]}',
                headers={"Connection": "close", "Content-Type": "application/json"},
            ),
        ):
            async for response in source._api_call(url="/test", params={}):
                pass


@pytest.mark.asyncio
async def test_api_call_with_empty_response(patch_logger, patch_default_retry_interval):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    with pytest.raises(Exception):
        with mock.patch.object(
            source.session,
            "get",
            side_effect=[
                MockResponse(
                    res=b"",
                    headers={"Content-Type": "application/json"},
                ),
            ],
        ):
            async for response in source._api_call(url="/test", params={}):
                pass


@pytest.mark.asyncio
async def test_api_call_with_text_response(patch_logger, patch_default_retry_interval):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()

    # Execute
    with pytest.raises(Exception):
        with mock.patch.object(
            source.session,
            "get",
            side_effect=[
                MockResponse(
                    res=b"Text",
                    headers={"Content-Type": "text/html"},
                ),
            ],
        ):
            async for response in source._api_call(url="/test", params={}):
                pass


@pytest.mark.asyncio
async def test_api_call_for_skipping(patch_logger, patch_default_retry_interval):
    # Setup
    source = create_source(ServiceNowDataSource)
    source._generate_session()
    mock_response = MockResponse(
        res=b"",
        headers={"Content-Type": "text/html"},
    )

    # Execute
    with pytest.raises(Exception):
        with mock.patch.object(
            source.session,
            "get",
            side_effect=[mock_response, mock_response, mock_response],
        ):
            async for response in source._api_call(url="/test", params={}):
                pass
