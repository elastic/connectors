#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the ServiceNow source class methods"""

from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from aiohttp.client_exceptions import ServerDisconnectedError
from connectors_sdk.filtering.validation import Filter, SyncRuleValidationResult
from connectors_sdk.source import ConfigurableFieldValueError, DataSourceConfiguration

from connectors.access_control import ACCESS_CONTROL, DLS_QUERY
from connectors.sources.servicenow.client import InvalidResponse, ServiceNowClient
from connectors.sources.servicenow.datasource import (
    ServiceNowDataSource,
    _prefix_role_id,
)
from connectors.sources.servicenow.validator import ServiceNowAdvancedRulesValidator
from tests.commons import AsyncIterator
from tests.sources.support import create_source

SAMPLE_RESPONSE = b'{"batch_request_id":"1","serviced_requests":[{"id":"1", "body":"eyJyZXN1bHQiOlt7Im5hbWUiOiJzbl9zbV9qb3VybmFsMDAwMiIsImxhYmVsIjoiU2VjcmV0cyBNYW5hZ2VtZW50IEpvdXJuYWwifV19","status_code":200,"status_text":"OK","execution_time":19}],"unserviced_requests":[]}'
ADVANCED_SNIPPET = "advanced_snippet"


@asynccontextmanager
async def create_service_now_source(
    use_text_extraction_service=False, expand_role_members=True
):
    async with create_source(
        ServiceNowDataSource,
        url="http://127.0.0.1:1234",
        username="admin",
        password="changeme",
        services="*",
        use_text_extraction_service=use_text_extraction_service,
        expand_role_members=expand_role_members,
    ) as source:
        yield source


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


@pytest.mark.parametrize("field", ["username", "password", "services"])
@pytest.mark.asyncio
async def test_validate_config_missing_fields_then_raise(field):
    async with create_service_now_source() as source:
        source.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_service_then_raise():
    async with create_service_now_source() as source:
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
                            {"sys_id": "id_1", "name": "name_1", "label": "label_1"},
                            {"sys_id": "id_2", "name": "name_2", "label": "label_2"},
                        ]
                    ]
                ),
            ):
                await source.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    async with create_service_now_source() as source:
        with mock.patch.object(
            ServiceNowClient,
            "get_table_length",
            return_value=mock.AsyncMock(return_value=2),
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_unsuccessful_connection_then_raise():
    async with create_service_now_source() as source:
        with mock.patch.object(
            ServiceNowClient,
            "get_table_length",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_tweak_bulk_options():
    async with create_service_now_source() as source:
        source.concurrent_downloads = 10
        options = {"concurrent_downloads": 5}

        source.tweak_bulk_options(options)
        assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_get_data():
    async with create_service_now_source() as source:
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
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_data_with_retry():
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            side_effect=ServerDisconnectedError
        )

        with pytest.raises(Exception):
            async for _ in source.servicenow_client.get_data(batched_apis={"API1"}):
                pass


@pytest.mark.asyncio
async def test_get_table_length():
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )
        response = await source.servicenow_client.get_table_length("Service1")

        assert response == 2


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_table_length_with_retry():
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            side_effect=ServerDisconnectedError
        )

        with pytest.raises(Exception):
            await source.servicenow_client.get_table_length("Service1")


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_data_with_empty_response():
    async with create_service_now_source() as source:
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
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_data_with_text_response():
    async with create_service_now_source() as source:
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
    async with create_service_now_source() as source:
        source.servicenow_client.services = ["label_1", "label_3"]

        source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)
        with mock.patch.object(
            ServiceNowClient, "get_data", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.servicenow_client.filter_services()


@pytest.mark.asyncio
async def test_filter_services_when_sysparm_fields_missing():
    async with create_service_now_source() as source:
        source.servicenow_client.services = ["Incident", "Feature", "User"]

        source.servicenow_client.get_table_length = mock.AsyncMock(return_value=3)
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            return_value=AsyncIterator(
                [
                    [
                        {"sys_id": "id_1", "name": "user"},
                        {"sys_id": "id_2", "label": "Feature"},
                        {"sys_id": "id_3", "name": "incident", "label": "Incident"},
                    ]
                ]
            ),
        ):
            result = await source.servicenow_client.filter_services(
                source.servicenow_client.services
            )
            assert result[0] == {"Incident": "incident"}
            assert sorted(result[1]) == sorted(["Feature", "User"])


@pytest.mark.asyncio
async def test_filter_services_when_sysparm_fields_missing_for_unrelated_table():
    async with create_service_now_source() as source:
        source.servicenow_client.services = ["Incident", "Feature"]

        source.servicenow_client.get_table_length = mock.AsyncMock(return_value=4)
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            return_value=AsyncIterator(
                [
                    [
                        {"sys_id": "id_1", "name": "feature", "label": "Feature"},
                        {"sys_id": "id_2", "name": "incident", "label": "Incident"},
                        {"sys_id": "id_3", "name": "Label-less Foo"},
                        {"sys_id": "id_4", "label": "nameless_bar"},
                    ]
                ]
            ),
        ):
            result = await source.servicenow_client.filter_services(
                source.servicenow_client.services
            )
            assert result[0] == {"Incident": "incident", "Feature": "feature"}
            # unrelated tables are ignored and don't cause errors
            assert result[1] == []


@pytest.mark.asyncio
async def test_get_docs_with_skipping_table_data():
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )
        response_list = []
        with mock.patch(
            "connectors.sources.servicenow.datasource.DEFAULT_SERVICE_NAMES",
            {"incident": ["sn_incident_read"]},
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
@pytest.mark.parametrize(
    "dls_enabled, expected_response",
    [
        (
            True,
            {
                "_allow_access_control": ["user_id_1, user_id_2"],
                "_id": "id_1",
                "_timestamp": "1212-12-12T12:12:12",
                "sys_id": "id_1",
                "sys_updated_on": "1212-12-12 12:12:12",
                "sys_class_name": "incident",
                "sys_user": "admin",
                "type": "table_record",
            },
        ),
        (
            False,
            {
                "_id": "id_1",
                "_timestamp": "1212-12-12T12:12:12",
                "sys_id": "id_1",
                "sys_updated_on": "1212-12-12 12:12:12",
                "sys_class_name": "incident",
                "sys_user": "admin",
                "type": "table_record",
            },
        ),
    ],
)
async def test_get_docs_with_skipping_attachment_data(dls_enabled, expected_response):
    async with create_service_now_source() as source:
        source._dls_enabled = Mock(return_value=dls_enabled)
        source._fetch_access_controls = mock.AsyncMock(
            return_value=["user_id_1, user_id_2"]
        )
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )

        response_list = []
        with mock.patch(
            "connectors.sources.servicenow.datasource.DEFAULT_SERVICE_NAMES",
            {"incident": ["sn_incident_read"]},
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
            expected_response,
            None,
        ) in response_list


@pytest.mark.asyncio
async def test_get_docs_with_configured_services():
    async with create_service_now_source() as source:
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
async def test_fetch_attachment_content_with_extraction_service():
    with (
        patch(
            "connectors_sdk.content_extraction.ContentExtraction.extract_text",
            return_value="Attachment Content",
        ),
        patch(
            "connectors_sdk.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
    ):
        async with create_service_now_source(
            use_text_extraction_service=True
        ) as source:
            source.servicenow_client._api_call = mock.AsyncMock(
                return_value=MockResponse(res=b"Attachment Content", headers={})
            )

            response = await source.get_content(
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
                "body": "Attachment Content",
            }


@pytest.mark.asyncio
async def test_fetch_attachment_content_with_upper_extension():
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            side_effect=Exception("Something went wrong")
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(res=b"Attachment Content", headers={})
        )

        response = await source.get_content(
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
    async with create_service_now_source() as source:
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
    async with create_service_now_source() as source:
        source.servicenow_client.services = ["custom"]
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )

        response_list = []
        with mock.patch.object(
            ServiceNowClient,
            "filter_services",
            return_value=({"Incident": "incident"}, []),
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


@pytest.mark.parametrize(
    "filtering",
    [
        Filter(
            {
                ADVANCED_SNIPPET: {
                    "value": [
                        {"service": "Incident", "query": "user_nameSTARTSWITHa"},
                    ]
                }
            }
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules_pagination(filtering):
    expected_filter_apis = [
        {
            "headers": [
                {"name": "Content-Type", "value": "application/json"},
                {"name": "Accept", "value": "application/json"},
            ],
            "id": mock.ANY,
            "method": "GET",
            "url": "/api/now/table/incident?sysparm_query=ORDERBYsys_created_on%5Euser_nameSTARTSWITHa&sysparm_limit=2&sysparm_offset=0",
        },
        {
            "headers": [
                {"name": "Content-Type", "value": "application/json"},
                {"name": "Accept", "value": "application/json"},
            ],
            "id": mock.ANY,
            "method": "GET",
            "url": "/api/now/table/incident?sysparm_query=ORDERBYsys_created_on%5Euser_nameSTARTSWITHa&sysparm_limit=2&sysparm_offset=2",
        },
    ]

    with patch("connectors.sources.servicenow.client.TABLE_FETCH_SIZE", 2):
        async with create_service_now_source() as source:
            source.servicenow_client._api_call = mock.AsyncMock(
                return_value=MockResponse(
                    res=SAMPLE_RESPONSE,
                    headers={"Content-Type": "application/json", "x-total-count": 3},
                )
            )

            response_list = []
            with mock.patch.object(
                ServiceNowClient,
                "filter_services",
                return_value=({"Incident": "incident"}, []),
            ):
                with mock.patch.object(
                    ServiceNowClient,
                    "get_data",
                    return_value=AsyncIterator(
                        [
                            [
                                {
                                    "sys_updated_on": "2023-10-10 05:21:45",
                                    "sys_id": "id_1",
                                    "email": "admin@email.com",
                                    "user_name": "demo.user",
                                }
                            ]
                        ]
                    ),
                ):
                    with mock.patch.object(
                        source, "_fetch_table_data", wraps=source._fetch_table_data
                    ) as mock_fetch_table_data:
                        async for response in source.get_docs(filtering):
                            response_list.append(response[0])

                mock_fetch_table_data.assert_called_once_with(
                    expected_filter_apis, None
                )
            assert [
                {
                    "sys_updated_on": "2023-10-10 05:21:45",
                    "sys_id": "id_1",
                    "email": "admin@email.com",
                    "user_name": "demo.user",
                    "_id": "id_1",
                    "_timestamp": "2023-10-10T05:21:45",
                }
            ] == response_list


@pytest.mark.asyncio
async def test_get_access_control():
    expected_response = {
        "_id": "id_1",
        "identity": {
            "user_id": "user_id:id_1",
            "display_name": "username:demo.user",
            "email": "email:admin@email.com",
        },
        "created_at": "2023-10-10T05:21:45",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "user_id:id_1",
                        "username:demo.user",
                        "email:admin@email.com",
                    ]
                },
                "source": DLS_QUERY,
            }
        },
    }
    async with create_service_now_source(expand_role_members=True) as source:
        source.servicenow_client.get_table_length = mock.AsyncMock(return_value=2)
        source._dls_enabled = Mock(return_value=True)
        with mock.patch.object(
            ServiceNowClient,
            "get_data",
            return_value=AsyncIterator(
                [
                    [
                        {
                            "sys_updated_on": "2023-10-10 05:21:45",
                            "sys_id": "id_1",
                            "email": "admin@email.com",
                            "user_name": "demo.user",
                        }
                    ]
                ]
            ),
        ):
            async for user in source.get_access_control():
                assert user == expected_response


@pytest.mark.asyncio
async def test_get_access_control_dls_disabled():
    async with create_service_now_source() as source:
        source._dls_enabled = Mock(return_value=False)

        access_control_list = []
        async for access_control in source.get_access_control():
            access_control_list.append(access_control)

        assert len(access_control_list) == 0


@pytest.mark.asyncio
async def test_get_access_control_includes_role_ids():
    async with create_service_now_source(expand_role_members=False) as source:
        source._dls_enabled = Mock(return_value=True)
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "user": {"value": "id_1"},
                            "role": {"value": "role_id_1"},
                        }
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_updated_on": "2023-10-10 05:21:45",
                            "sys_id": "id_1",
                            "email": "admin@email.com",
                            "user_name": "demo.user",
                            "_id": "id_1",
                            "_timestamp": "2023-10-10T05:21:45",
                        }
                    ]
                ),
            ],
        ):
            users = [user async for user in source.get_access_control()]

        assert len(users) == 1
        assert users[0]["identity"]["role_ids"] == ["role_id:role_id_1"]
        assert (
            "role_id:role_id_1"
            in users[0]["query"]["template"]["params"]["access_control"]
        )


@pytest.mark.asyncio
async def test_get_access_control_legacy_does_not_fetch_roles():
    async with create_service_now_source(expand_role_members=True) as source:
        source._dls_enabled = Mock(return_value=True)
        source._fetch_user_roles_map = mock.AsyncMock(
            side_effect=AssertionError("legacy mode must not fetch roles")
        )
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            return_value=AsyncIterator(
                [
                    {
                        "sys_updated_on": "2023-10-10 05:21:45",
                        "sys_id": "id_1",
                        "email": "admin@email.com",
                        "user_name": "demo.user",
                        "_id": "id_1",
                        "_timestamp": "2023-10-10T05:21:45",
                    }
                ]
            ),
        ):
            users = [user async for user in source.get_access_control()]

        assert len(users) == 1
        assert "role_ids" not in users[0]["identity"]
        source._fetch_user_roles_map.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_access_controls_legacy_expands_users():
    async with create_service_now_source(expand_role_members=True) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "role_1",
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                AsyncIterator([{"user": {"value": "user_id_1"}}]),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert access_control == ["user_id:user_id_1"]


@pytest.mark.asyncio
async def test_fetch_access_controls_legacy_skips_deleted_role():
    async with create_service_now_source(expand_role_members=True) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "role_1",
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {"sys_user_role": {"value": "deleted_role_id"}},
                        {"sys_user_role": {"value": "role_id_1"}},
                    ]
                ),
                AsyncIterator([{"user": {"value": "user_id_1"}}]),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert access_control == ["user_id:user_id_1"]


@pytest.mark.asyncio
async def test_fetch_access_controls_legacy_for_public():
    async with create_service_now_source(expand_role_members=True) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "public",
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_updated_on": "2023-10-10 05:21:45",
                            "sys_id": "user_id_1",
                            "email": "admin@email.com",
                            "user_name": "demo.user",
                        },
                        {
                            "sys_updated_on": "2023-10-10 05:21:45",
                            "sys_id": "user_id_2",
                            "email": "admin@email.com",
                            "user_name": "sample.user",
                        },
                    ]
                ),
                AsyncIterator([{"user": {"value": "user_id_1"}}]),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert sorted(access_control) == sorted(
                ["user_id:user_id_1", "user_id:user_id_2"]
            )


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_returns_role_tokens():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "role_1",
                        },
                    ]
                ),
            ],
        ) as mock_generator:
            access_control = await source._fetch_access_controls("service_name")
            assert access_control == ["role_id:role_id_1"]
            # Table ACL roles + sys_user_role only — no user expansion pass
            assert mock_generator.call_count == 2


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_public_returns_none():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "public",
                        },
                    ]
                ),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert access_control is None


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_default_table_returns_role_tokens():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {"sys_id": "admin_id", "name": "admin"},
                        {"sys_id": "itil_id", "name": "itil"},
                        {
                            "sys_id": "sn_incident_read_id",
                            "name": "sn_incident_read",
                        },
                        {"sys_id": "ml_report_user_id", "name": "ml_report_user"},
                        {"sys_id": "ml_admin_id", "name": "ml_admin"},
                    ]
                ),
            ],
        ):
            access_control = await source._fetch_access_controls("incident")
            assert access_control == sorted(
                [
                    "role_id:admin_id",
                    "role_id:itil_id",
                    "role_id:sn_incident_read_id",
                    "role_id:ml_report_user_id",
                    "role_id:ml_admin_id",
                ]
            )


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_filters_deleted_roles():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator(
                    [
                        {"sys_user_role": {"value": "deleted_role_id"}},
                        {"sys_user_role": {"value": "role_id_1"}},
                    ]
                ),
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "role_1",
                        },
                    ]
                ),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert access_control == ["role_id:role_id_1"]


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_no_acl_entries_returns_none():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[AsyncIterator([])],
        ) as mock_generator:
            access_control = await source._fetch_access_controls("service_name")
            assert access_control is None
            # Empty ACL short-circuits before loading sys_user_role
            assert mock_generator.call_count == 1
            assert (
                mock_generator.call_args.kwargs["service_name"]
                == "sys_security_acl_role"
            )


@pytest.mark.asyncio
async def test_roles_maps_cached_per_sync():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator([{"sys_user_role": {"value": "role_id_1"}}]),
                AsyncIterator(
                    [
                        {"sys_id": "role_id_1", "name": "role_1"},
                        {"sys_id": "role_id_2", "name": "role_2"},
                    ]
                ),
                AsyncIterator([{"sys_user_role": {"value": "role_id_2"}}]),
            ],
        ) as mock_generator:
            first = await source._fetch_access_controls("table_a")
            second = await source._fetch_access_controls("table_b")

        assert first == ["role_id:role_id_1"]
        assert second == ["role_id:role_id_2"]
        # One ACL fetch per table + one cached sys_user_role fetch
        assert mock_generator.call_count == 3
        assert (
            mock_generator.call_args_list[0].kwargs["service_name"]
            == "sys_security_acl_role"
        )
        assert (
            mock_generator.call_args_list[1].kwargs["service_name"] == "sys_user_role"
        )


@pytest.mark.asyncio
async def test_decorate_skips_field_for_public():
    async with create_service_now_source() as source:
        source._dls_enabled = Mock(return_value=True)
        document = {"_id": "id_1"}
        result = source._decorate_with_access_control(document, None)
        assert ACCESS_CONTROL not in result


@pytest.mark.asyncio
async def test_decorate_with_role_tokens():
    async with create_service_now_source() as source:
        source._dls_enabled = Mock(return_value=True)
        document = {"_id": "id_1"}
        result = source._decorate_with_access_control(
            document, [_prefix_role_id("role_id_b"), _prefix_role_id("role_id_a")]
        )
        assert result[ACCESS_CONTROL] == ["role_id:role_id_a", "role_id:role_id_b"]


@pytest.mark.asyncio
async def test_decorate_denies_all_for_empty_acl():
    async with create_service_now_source() as source:
        source._dls_enabled = Mock(return_value=True)
        document = {"_id": "id_1"}
        result = source._decorate_with_access_control(document, [])
        assert result[ACCESS_CONTROL] == []


@pytest.mark.asyncio
async def test_fetch_access_controls_compact_empty_roles_returns_none():
    async with create_service_now_source(expand_role_members=False) as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                AsyncIterator([]),
            ],
        ):
            access_control = await source._fetch_access_controls("incident")
            assert access_control is None


@pytest.mark.asyncio
async def test_fetch_user_roles_map_skips_rows_without_role():
    async with create_service_now_source() as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            return_value=AsyncIterator(
                [
                    {"user": {"value": "user_id_1"}},
                    {
                        "user": {"value": "user_id_2"},
                        "role": {"value": "role_id_1"},
                    },
                ]
            ),
        ):
            user_roles = await source._fetch_user_roles_map()
            assert user_roles == {"user_id_2": {"role_id_1"}}


@pytest.mark.asyncio
async def test_expand_role_members_defaults_true_when_missing():
    config = ServiceNowDataSource.get_default_configuration()
    config.pop("expand_role_members")
    async with create_source(
        ServiceNowDataSource,
        url="http://127.0.0.1:1234",
        username="admin",
        password="changeme",
        services="*",
    ) as source:
        source.configuration = DataSourceConfiguration(config)
        source.configuration.set_defaults(
            ServiceNowDataSource.get_default_configuration()
        )
        assert source._expand_role_members() is True


@pytest.mark.asyncio
async def test_get_docs_advanced_rules_with_dls():
    filtering = Filter(
        {
            ADVANCED_SNIPPET: {
                "value": [
                    {"service": "Incident", "query": "user_nameSTARTSWITHa"},
                ]
            }
        }
    )
    async with create_service_now_source(expand_role_members=False) as source:
        source.servicenow_client.services = ["custom"]
        source._dls_enabled = Mock(return_value=True)
        source._fetch_access_controls = mock.AsyncMock(
            return_value=["role_id:role_id_1"]
        )
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )

        response_list = []
        with mock.patch.object(
            ServiceNowClient,
            "filter_services",
            return_value=({"Incident": "incident"}, []),
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

        source._fetch_access_controls.assert_awaited_once_with(table_name="incident")
        assert response_list[0][ACCESS_CONTROL] == ["role_id:role_id_1"]
        assert response_list[1][ACCESS_CONTROL] == ["role_id:role_id_1"]


@pytest.mark.asyncio
async def test_get_docs_compact_stamps_roles_on_records():
    async with create_service_now_source(expand_role_members=False) as source:
        source._dls_enabled = Mock(return_value=True)
        source._fetch_access_controls = mock.AsyncMock(
            return_value=["role_id:role_id_1"]
        )
        source.servicenow_client._api_call = mock.AsyncMock(
            return_value=MockResponse(
                res=SAMPLE_RESPONSE,
                headers={"Content-Type": "application/json", "x-total-count": 2},
            )
        )

        response_list = []
        with mock.patch(
            "connectors.sources.servicenow.datasource.DEFAULT_SERVICE_NAMES",
            {"incident": ["sn_incident_read"]},
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
                "_allow_access_control": ["role_id:role_id_1"],
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
async def test_end_signal_is_added_to_queue_in_case_of_exception():
    END_SIGNAL = "RECORD_TASK_FINISHED"
    async with create_service_now_source() as source:
        with patch.object(
            source,
            "_fetch_attachment_metadata",
            side_effect=Exception("Error fetching attachments"),
        ):
            with pytest.raises(Exception):
                await source._attachment_metadata_producer(
                    records_ids=["record_1", "record_2"], access_control=[]
                )
                assert source.queue.get_nowait() == END_SIGNAL
