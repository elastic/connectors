#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the ServiceNow source class methods"""
from unittest import mock

import pytest
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.servicenow import ServiceNowClient, ServiceNowDataSource
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


def test_get_configuration():
    config = DataSourceConfiguration(ServiceNowDataSource.get_default_configuration())

    assert config["services"] == ["*"]
    assert config["username"] == "admin"
    assert config["password"] == "changeme"


@pytest.mark.parametrize("field", ["url", "username", "password", "services"])
@pytest.mark.asyncio
async def test_validate_config_missing_fields_then_raise(field):
    source = create_source(ServiceNowDataSource)
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_service_then_raise():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["label_1", "label_3"]

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Services 'label_3' are not available. Available services are: 'label_1'",
    ):
        with mock.patch.object(
            ServiceNowClient,
            "_api_call",
            return_value=AsyncIterator(
                [
                    [
                        {"name": "name_1", "label": "label_1"},
                        {"name": "name_2", "label": "label_2"},
                    ]
                ]
            ),
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client._get_session

    await source.close()
    assert hasattr(source.servicenow_client.__dict__, "_get_session") is False


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator([{"service_label": "service_name"}]),
    ):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_for_unsuccessful_connection_then_raise():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient, "_api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_tweak_bulk_options():
    source = create_source(ServiceNowDataSource)
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    source.tweak_bulk_options(options)
    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_api_call():
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session

    response_list = []
    with mock.patch.object(
        session,
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
        async for response in source.servicenow_client._api_call(
            url="/test", params={"sysparm_query": "label=Incident"}
        ):
            response_list.append(response)

    assert [{"id": "record_1"}] in response_list


@pytest.mark.asyncio
async def test_api_call_with_attachment():
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session

    response_list = []
    with mock.patch.object(
        session,
        "get",
        return_value=MockResponse(
            res=b"Attachment Content",
            headers={"Content-Type": "application/json"},
        ),
    ):
        async for response in source.servicenow_client._api_call(
            url="/test", params={}, is_attachment=True
        ):
            response_list.append(response)

    assert await response_list[0].read() == b"Attachment Content"


@pytest.fixture
def patch_default_retry_interval():
    """Patch retry interval to 0"""

    with mock.patch("connectors.sources.servicenow.RETRY_INTERVAL", 0):
        yield


@pytest.mark.asyncio
async def test_api_call_with_retry(patch_default_retry_interval):
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session

    with pytest.raises(Exception):
        with mock.patch.object(session, "get", side_effect=ServerDisconnectedError):
            async for response in source.servicenow_client._api_call(
                url="/test", params={}
            ):
                pass


@pytest.mark.asyncio
async def test_api_call_with_close_connection(patch_default_retry_interval):
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session

    with pytest.raises(Exception):
        with mock.patch.object(
            session,
            "get",
            return_value=MockResponse(
                res=b'{"result": [{"id": "record_1"}]}',
                headers={"Connection": "close", "Content-Type": "application/json"},
            ),
        ):
            async for response in source.servicenow_client._api_call(
                url="/test", params={}
            ):
                pass


@pytest.mark.asyncio
async def test_api_call_with_empty_response(patch_default_retry_interval):
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session
    mock_response = MockResponse(
        res=b"",
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(Exception):
        with mock.patch.object(
            session,
            "get",
            side_effect=[
                mock_response,
                mock_response,
                mock_response,
            ],
        ):
            async for response in source.servicenow_client._api_call(
                url="/test", params={}
            ):
                pass


@pytest.mark.asyncio
async def test_api_call_with_text_response(patch_default_retry_interval):
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session
    mock_response = MockResponse(
        res=b"Text",
        headers={"Content-Type": "text/html"},
    )

    with pytest.raises(Exception):
        with mock.patch.object(
            session,
            "get",
            side_effect=[
                mock_response,
                mock_response,
                mock_response,
            ],
        ):
            async for response in source.servicenow_client._api_call(
                url="/test", params={}
            ):
                pass


@pytest.mark.asyncio
async def test_api_call_for_max_retries(patch_default_retry_interval):
    source = create_source(ServiceNowDataSource)
    session = source.servicenow_client._get_session
    mock_response = MockResponse(
        res=b"",
        headers={"Content-Type": "text/html"},
    )

    with pytest.raises(Exception):
        with mock.patch.object(
            session,
            "get",
            side_effect=[mock_response, mock_response, mock_response],
        ):
            async for response in source.servicenow_client._api_call(
                url="/test", params={}
            ):
                pass


@pytest.mark.asyncio
async def test_filter_services():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["label_1", "label_3"]

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator(
            [
                [
                    {"name": "name_1", "label": "label_1"},
                    {"name": "name_2", "label": "label_2"},
                ]
            ]
        ),
    ):
        response = await source.servicenow_client.filter_services()

    assert response == (["name_1"], ["label_3"])


@pytest.mark.asyncio
async def test_filter_services_with_exception():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["label_1", "label_3"]

    with mock.patch.object(
        ServiceNowClient, "_api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.servicenow_client.filter_services()


@pytest.mark.asyncio
async def test_get_docs_with_skipping():
    source = create_source(ServiceNowDataSource)

    response_list = []
    with mock.patch(
        "connectors.sources.servicenow.DEFAULT_SERVICE_NAMES", ("incident",)
    ):
        with mock.patch.object(
            ServiceNowClient,
            "_api_call",
            side_effect=[
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "incident",
                                "sys_user": "admin",
                                "type": "table_record",
                            }
                        ]
                    ]
                ),
                Exception("Something went wrong"),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response)

    assert (
        {
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "incident",
            "sys_user": "admin",
            "type": "table_record",
        },
        None,
    ) in response_list


@pytest.mark.asyncio
async def test_get_docs_with_configured_services():
    source = create_source(ServiceNowDataSource)
    source.servicenow_client.services = ["custom"]

    response_list = []
    with mock.patch.object(
        ServiceNowClient, "filter_services", return_value=(["custom"], [])
    ):
        with mock.patch.object(
            ServiceNowClient,
            "_api_call",
            side_effect=[
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "custom",
                                "sys_user": "user1",
                                "type": "table_record",
                            },
                        ]
                    ]
                ),
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_2",
                                "table_sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "custom",
                                "sys_user": "user1",
                                "type": "attachment_metadata",
                            },
                        ]
                    ]
                ),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response)

    assert (
        {
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "custom",
            "sys_user": "user1",
            "type": "table_record",
        },
        None,
    ) in response_list


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_doit():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator(
            [
                MockResponse(
                    res=b"Attachment Content",
                    headers={},
                )
            ]
        ),
    ):
        response = await source.servicenow_client.fetch_attachment_content(
            metadata={
                "id": "id_1",
                "_timestamp": "1212-12-12 12:12:12",
                "file_name": "file_1.txt",
                "size_bytes": "2048",
            },
            doit=True,
        )

    assert response == {
        "_id": "id_1",
        "_timestamp": "1212-12-12 12:12:12",
        "_attachment": "QXR0YWNobWVudCBDb250ZW50",
    }


@pytest.mark.asyncio
async def test_fetch_attachment_content_without_doit():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator(
            [
                MockResponse(
                    res=b"Attachment Content",
                    headers={},
                )
            ]
        ),
    ):
        response = await source.servicenow_client.fetch_attachment_content(
            metadata={
                "id": "id_1",
                "_timestamp": "1212-12-12 12:12:12",
                "file_name": "file_1.txt",
                "size_bytes": "2048",
            }
        )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_exception():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        side_effect=Exception("Something went wrong"),
    ):
        response = await source.servicenow_client.fetch_attachment_content(
            metadata={
                "id": "id_1",
                "_timestamp": "1212-12-12 12:12:12",
                "file_name": "file_1.txt",
                "size_bytes": "2048",
            },
            doit=True,
        )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_unsupported_file_type_then_skip():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator(
            [
                MockResponse(
                    res=b"Attachment Content",
                    headers={},
                )
            ]
        ),
    ):
        response = await source.servicenow_client.fetch_attachment_content(
            metadata={
                "id": "id_1",
                "_timestamp": "1212-12-12 12:12:12",
                "file_name": "file_1.png",
                "size_bytes": "2048",
            },
            doit=True,
        )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_unsupported_file_size_then_skip():
    source = create_source(ServiceNowDataSource)

    with mock.patch.object(
        ServiceNowClient,
        "_api_call",
        return_value=AsyncIterator(
            [
                MockResponse(
                    res=b"Attachment Content",
                    headers={},
                )
            ]
        ),
    ):
        response = await source.servicenow_client.fetch_attachment_content(
            metadata={
                "id": "id_1",
                "_timestamp": "1212-12-12 12:12:12",
                "file_name": "file_1.txt",
                "size_bytes": "10485761",
            },
            doit=True,
        )

    assert response is None
