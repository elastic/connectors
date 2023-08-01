#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the ServiceNow source class methods"""
from unittest import mock

import pytest
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.servicenow import (
    InvalidResponse,
    ServiceNowAdvancedRulesValidator,
    ServiceNowClient,
    ServiceNowDataSource,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

SAMPLE_RESPONSE = b'{"batch_request_id":"1","serviced_requests":[{"id":"1", "body":"eyJyZXN1bHQiOlt7Im5hbWUiOiJzbl9zbV9qb3VybmFsMDAwMiIsImxhYmVsIjoiU2VjcmV0cyBNYW5hZ2VtZW50IEpvdXJuYWwifV19","status_code":200,"status_text":"OK","execution_time":19}],"unserviced_requests":[]}'
ADVANCED_SNIPPET = "advanced_snippet"

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

    async def iter_chunked(self, size):
        """Method to iterate over content"""
        yield self._res


def test_get_configuration():
    config = DataSourceConfiguration(ServiceNowDataSource.get_default_configuration())
    assert config["services"] == ["*"]

def setup_servicenow():
    # Set up default config with default values
    source = create_source(ServiceNowDataSource)
    source.configuration.set_field(name="username", value="admin")
    source.configuration.set_field(name="password", value="changeme")
    return source


@pytest.mark.parametrize("field", ["url", "username", "password", "services"])
@pytest.mark.asyncio
async def test_validate_config_missing_fields_then_raise(field):
    source = setup_servicenow()
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_service_then_raise():
    source = setup_servicenow()
    source.servicenow_client.services = ["label_1", "label_3"]

    source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Services 'label_3' are not available. Available services are: 'label_1'",
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
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
    source = setup_servicenow()
    await source.close()
    assert hasattr(source.servicenow_client.__dict__, "_get_session") is False


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    source = setup_servicenow()
    with mock.patch.object(
        ServiceNowClient,
        "get_table_length",
        return_value=mock.AsyncMock(return_value=2),
    ):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_for_unsuccessful_connection_then_raise():
    source = setup_servicenow()
    with mock.patch.object(
        ServiceNowClient,
        "get_table_length",
        side_effect=Exception("Something went wrong"),
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_tweak_bulk_options():
    source = setup_servicenow()
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    source.tweak_bulk_options(options)
    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_get_data():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE, headers={"Content-Type": "application/json"}
        )
    )

    response_list = []
    async for response in source.servicenow_client.get_data(batched_apis={"API1"}):
        response_list.append(response)

    assert [
        {"name": "sn_sm_journal0002", "label": "Secrets Management Journal"}
    ] in response_list


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_retry(mock_apply_retry_strategy):
    source = setup_servicenow()
    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=ServerDisconnectedError
    )

    with pytest.raises(Exception):
        async for _ in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
async def test_get_table_length():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )
    response = await source.servicenow_client.get_table_length("Service1")

    assert response == 2


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_table_length_with_retry(mock_apply_retry_strategy):
    source = setup_servicenow()
    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=ServerDisconnectedError
    )

    with pytest.raises(Exception):
        await source.servicenow_client.get_table_length("Service1")


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_empty_response(mock_apply_retry_strategy):
    source = setup_servicenow()
    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=b"",
            headers={"Content-Type": "application/json"},
        )
    )

    with pytest.raises(InvalidResponse):
        async for _ in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_data_with_text_response(mock_apply_retry_strategy):
    source = setup_servicenow()
    mock_apply_retry_strategy.return_value = mock.Mock()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=b"Text",
            headers={"Content-Type": "text/html"},
        )
    )

    with pytest.raises(InvalidResponse):
        async for _ in source.servicenow_client.get_data(batched_apis={"API1"}):
            pass


@pytest.mark.asyncio
async def test_filter_services_with_exception():
    source = setup_servicenow()
    source.servicenow_client.services = ["label_1", "label_3"]

    source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)
    with mock.patch.object(
        ServiceNowClient, "get_data", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.servicenow_client.filter_services()


@pytest.mark.asyncio
async def test_get_docs_with_skipping_table_data():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )
    response_list = []
    with mock.patch(
        "connectors.sources.servicenow.DEFAULT_SERVICE_NAMES", ("incident",)
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            side_effect=[
                Exception("Something went wrong"),
            ],
        ):
            async for response in source.get_docs():
                response_list.append(response)

    assert response_list == []


@pytest.mark.asyncio
async def test_get_docs_with_skipping_attachment_data():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )

    response_list = []
    with mock.patch(
        "connectors.sources.servicenow.DEFAULT_SERVICE_NAMES", ("incident",)
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
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
    source = setup_servicenow()
    source.servicenow_client.services = ["custom"]
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )

    response_list = []
    with mock.patch.object(
        ServiceNowClient, "filter_services", return_value=({"custom": "custom"}, [])
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
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
                response_list.append(response[0])
    assert [
        {
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "custom",
            "sys_user": "user1",
            "type": "table_record",
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
        },
        {
            "sys_id": "id_2",
            "table_sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "custom",
            "sys_user": "user1",
            "type": "attachment_metadata",
            "_id": "id_2",
            "_timestamp": "1212-12-12T12:12:12",
        },
    ] == response_list


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_doit():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

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
async def test_fetch_attachment_content_with_upper_extension():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1.TXT",
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
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

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
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        side_effect=Exception("Something went wrong")
    )

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
async def test_fetch_attachment_content_with_unsupported_extension_then_skip():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

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
async def test_fetch_attachment_content_without_extension_then_skip():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

    response = await source.servicenow_client.fetch_attachment_content(
        metadata={
            "id": "id_1",
            "_timestamp": "1212-12-12 12:12:12",
            "file_name": "file_1",
            "size_bytes": "2048",
        },
        doit=True,
    )

    assert response is None


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_unsupported_file_size_then_skip():
    source = setup_servicenow()
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(res=b"Attachment Content", headers={})
    )

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


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: one custom query
            [{"service": "User", "query": "user_nameSTARTSWITHa"}],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: two custom queries
            [
                {"service": "User", "query": "user_nameSTARTSWITHa"},
                {"service": "User", "query": "user_nameSTARTSWITHb"},
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: query field missing
            [
                {"service": "User", "query": "user_nameSTARTSWITHa"},
                {"service": "User", "query": ""},
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=mock.ANY,
            ),
        ),
        (
            # invalid: property field invalid
            [
                {"service": "User", "query": "user_nameSTARTSWITHa"},
                {"services": "User", "query": "user_nameSTARTSWITHa"},
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=mock.ANY,
            ),
        ),
        (
            # invalid: service as array -> wrong type
            [{"service": ["User"], "query": "user_nameSTARTSWITHa"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=mock.ANY,
            ),
        ),
        (
            # invalid: invalid service name
            [
                {"service": "User", "query": ["user_nameSTARTSWITHa"]},
                {"service": "Knowledge", "query": "user_nameSTARTSWITHa"},
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=mock.ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    source = setup_servicenow()
    source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)

    with mock.patch.object(
        ServiceNowClient,
        "get_data",
        return_value=AsyncIterator(
            [
                [
                    {"name": "user", "label": "User"},
                    {"name": "incident", "label": "User"},
                ]
            ]
        ),
    ):
        validation_result = await ServiceNowAdvancedRulesValidator(source).validate(
            advanced_rules
        )

    assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "filtering",
    [
        Filter(
            {
                ADVANCED_SNIPPET: {
                    "value": [
                        {"service": "Incident", "query": "user_nameSTARTSWITHa"},
                        {"service": "Incident", "query": "user_nameSTARTSWITHj"},
                    ]
                }
            }
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering):
    source = setup_servicenow()
    source.servicenow_client.services = ["custom"]
    source.servicenow_client._api_call = mock.AsyncMock(
        return_value=MockResponse(
            res=SAMPLE_RESPONSE,
            headers={"Content-Type": "application/json", "x-total-count": 2},
        )
    )

    response_list = []
    with mock.patch.object(
        ServiceNowClient, "filter_services", return_value=({"Incident": "incident"}, [])
    ):
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            side_effect=[
                AsyncIterator(
                    [
                        [
                            {
                                "sys_id": "id_1",
                                "sys_updated_on": "1212-12-12 12:12:12",
                                "sys_class_name": "incident",
                                "sys_user": "abc",
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
                                "sys_class_name": "incident",
                                "sys_user": "abc",
                                "type": "attachment_metadata",
                            },
                        ]
                    ]
                ),
            ],
        ):
            async for response in source.get_docs(filtering):
                response_list.append(response[0])
    assert [
        {
            "sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "incident",
            "sys_user": "abc",
            "type": "table_record",
            "_id": "id_1",
            "_timestamp": "1212-12-12T12:12:12",
        },
        {
            "sys_id": "id_2",
            "table_sys_id": "id_1",
            "sys_updated_on": "1212-12-12 12:12:12",
            "sys_class_name": "incident",
            "sys_user": "abc",
            "type": "attachment_metadata",
            "_id": "id_2",
            "_timestamp": "1212-12-12T12:12:12",
        },
    ] == response_list
