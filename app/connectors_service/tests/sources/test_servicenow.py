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
from connectors_sdk.source import ConfigurableFieldValueError

from connectors.access_control import DLS_QUERY
from connectors.sources.servicenow.client import InvalidResponse, ServiceNowClient
from connectors.sources.servicenow.datasource import (
    ServiceNowDataSource,
)
from connectors.sources.servicenow.validator import ServiceNowAdvancedRulesValidator
from tests.commons import AsyncIterator
from tests.sources.support import create_source

SAMPLE_RESPONSE = b'{"batch_request_id":"1","serviced_requests":[{"id":"1", "body":"eyJyZXN1bHQiOlt7Im5hbWUiOiJzbl9zbV9qb3VybmFsMDAwMiIsImxhYmVsIjoiU2VjcmV0cyBNYW5hZ2VtZW50IEpvdXJuYWwifV19","status_code":200,"status_text":"OK","execution_time":19}],"unserviced_requests":[]}'
ADVANCED_SNIPPET = "advanced_snippet"


@asynccontextmanager
async def create_service_now_source(use_text_extraction_service=False):
    async with create_source(
        ServiceNowDataSource,
        url="http://127.0.0.1:1234",
        username="admin",
        password="changeme",
        services="*",
        use_text_extraction_service=use_text_extraction_service,
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


@pytest.mark.parametrize("field", ["services"])
@pytest.mark.asyncio
async def test_validate_config_missing_fields_then_raise(field):
    # username is conditional (depends_on auth_method/grant_type) so omitting
    # it does not raise for the default oauth+password config used in this test.
    # basic_auth_password / oauth_password are tested via their own grant-type
    # paths. Only 'services' is unconditionally required.
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

            mock_fetch_table_data.assert_called_once_with(expected_filter_apis, [])
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
                        "user:admin@email.com",
                    ]
                },
                "source": DLS_QUERY,
            }
        },
    }
    async with create_service_now_source() as source:
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
async def test_fetch_access_control():
    async with create_service_now_source() as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                # 1st call: _fetch_all_users() → user_email_map (no email → empty map)
                AsyncIterator([]),
                # 2nd call: sys_user_role → roles dict
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "role_1",
                        },
                    ]
                ),
                # 3rd call: sys_security_acl_role → user_roles list
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                # 4th call: _fetch_users_by_roles(role_id_1) → users
                AsyncIterator([{"user": {"value": "user_id_1"}}]),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert access_control == ["user_id:user_id_1"]


@pytest.mark.asyncio
async def test_fetch_access_control_for_public():
    async with create_service_now_source() as source:
        with mock.patch.object(
            ServiceNowDataSource,
            "_table_data_generator",
            side_effect=[
                # 1st call: _fetch_all_users() → user_email_map
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
                            "email": "sample@email.com",
                            "user_name": "sample.user",
                        },
                    ]
                ),
                # 2nd call: sys_user_role → roles dict
                AsyncIterator(
                    [
                        {
                            "sys_id": "role_id_1",
                            "name": "public",
                        },
                    ]
                ),
                # 3rd call: sys_security_acl_role → user_roles list
                AsyncIterator(
                    [
                        {
                            "sys_user_role": {"value": "role_id_1"},
                        },
                    ]
                ),
                # 4th call: _fetch_all_users() for the public role → all users
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
                            "email": "sample@email.com",
                            "user_name": "sample.user",
                        },
                    ]
                ),
                # 5th call: _fetch_users_by_roles(role_id_1) → users
                AsyncIterator([{"user": {"value": "user_id_1"}}]),
            ],
        ):
            access_control = await source._fetch_access_controls("service_name")
            assert sorted(access_control) == sorted(
                ["user:admin@email.com", "user:sample@email.com"]
            )


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


# ── OAuth grant type tests ────────────────────────────────────────────────────

def _make_token_response(access_token="tok_abc", expires_in=1800, refresh_token=None):
    """Return a minimal mock JSON payload matching ServiceNow's token endpoint."""
    payload = {"access_token": access_token, "expires_in": expires_in}
    if refresh_token:
        payload["refresh_token"] = refresh_token
    return payload


def _mock_token_post(token_payload, status=200):
    """Construct a nested mock that makes aiohttp.ClientSession().post() work."""
    response = mock.AsyncMock()
    response.status = status
    response.json = mock.AsyncMock(return_value=token_payload)
    response.raise_for_status = mock.MagicMock()
    ctx = mock.AsyncMock()
    ctx.__aenter__ = mock.AsyncMock(return_value=response)
    ctx.__aexit__ = mock.AsyncMock(return_value=False)
    session = mock.AsyncMock()
    session.post = mock.MagicMock(return_value=ctx)
    session_ctx = mock.AsyncMock()
    session_ctx.__aenter__ = mock.AsyncMock(return_value=session)
    session_ctx.__aexit__ = mock.AsyncMock(return_value=False)
    return session_ctx, session


def _make_client(extra_config=None):
    """Return a ServiceNowClient with a minimal valid configuration."""
    from connectors_sdk.source import DataSourceConfiguration

    config_dict = ServiceNowDataSource.get_default_configuration()
    defaults = {
        "url": "https://dev12345.service-now.com",
        "username": "basic_admin",       # Basic Auth username field
        "oauth_username": "admin",           # OAuth username field (password grant)
        "oauth_username_refresh": "admin",   # OAuth username field (refresh_token grant)
        "services": "*",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "oauth_password": "test_password",
        "basic_auth_password": "test_basic_pw",
    }
    for k, v in defaults.items():
        if k in config_dict:
            config_dict[k]["value"] = v
    if extra_config:
        for k, v in extra_config.items():
            if k in config_dict:
                config_dict[k]["value"] = v
            else:
                from connectors_sdk.source import DEFAULT_CONFIGURATION
                config_dict[k] = DEFAULT_CONFIGURATION.copy() | {"value": v}
    return ServiceNowClient(configuration=DataSourceConfiguration(config_dict))


@pytest.mark.asyncio
async def test_fetch_access_token_password_grant():
    """Password grant: username + password are sent to the token endpoint."""
    client = _make_client({"oauth_grant_type": "password"})
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        token = await client._fetch_access_token()

    assert token == "tok_abc"
    call_kwargs = session.post.call_args
    sent_data = call_kwargs[1]["data"]
    assert sent_data["grant_type"] == "password"
    assert sent_data["username"] == "admin"
    assert sent_data["password"] == "test_password"
    assert sent_data["client_id"] == "test_client_id"
    assert sent_data["client_secret"] == "test_client_secret"


@pytest.mark.asyncio
async def test_fetch_access_token_client_credentials_grant():
    """Client Credentials grant: no username or password in the request."""
    client = _make_client({"oauth_grant_type": "client_credentials"})
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        token = await client._fetch_access_token()

    assert token == "tok_abc"
    sent_data = session.post.call_args[1]["data"]
    assert sent_data["grant_type"] == "client_credentials"
    assert "username" not in sent_data
    assert "password" not in sent_data


@pytest.mark.asyncio
async def test_fetch_access_token_refresh_token_grant():
    """Refresh Token grant: refresh_token value sent, no password."""
    client = _make_client({
        "oauth_grant_type": "refresh_token",
        "refresh_token": "my_refresh_tok",
    })
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        token = await client._fetch_access_token()

    assert token == "tok_abc"
    sent_data = session.post.call_args[1]["data"]
    assert sent_data["grant_type"] == "refresh_token"
    assert sent_data["username"] == "admin"
    assert sent_data["refresh_token"] == "my_refresh_tok"
    assert "password" not in sent_data


@pytest.mark.asyncio
async def test_fetch_access_token_authorization_code_grant_first_call():
    """Authorization Code grant: first call exchanges the authorization code."""
    client = _make_client({
        "oauth_grant_type": "authorization_code",
        "oauth_authorization_code": "one_time_code_xyz",
        "oauth_redirect_uri": "https://myapp.example.com/callback",
    })
    token_payload = _make_token_response(refresh_token="returned_refresh")
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        token = await client._fetch_access_token()

    assert token == "tok_abc"
    sent_data = session.post.call_args[1]["data"]
    assert sent_data["grant_type"] == "authorization_code"
    assert sent_data["code"] == "one_time_code_xyz"
    assert sent_data["redirect_uri"] == "https://myapp.example.com/callback"
    # The returned refresh token should be cached for subsequent calls
    assert client._cached_refresh_token == "returned_refresh"


@pytest.mark.asyncio
async def test_fetch_access_token_authorization_code_grant_subsequent_call():
    """Authorization Code grant: subsequent calls use the cached refresh token."""
    client = _make_client({
        "oauth_grant_type": "authorization_code",
        "oauth_authorization_code": "one_time_code_xyz",
        "oauth_redirect_uri": "https://myapp.example.com/callback",
    })
    # Pre-seed the cached refresh token as if a first call already happened
    client._cached_refresh_token = "cached_refresh_tok"
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        token = await client._fetch_access_token()

    assert token == "tok_abc"
    sent_data = session.post.call_args[1]["data"]
    # Should use refresh_token grant internally, not authorization_code
    assert sent_data["grant_type"] == "refresh_token"
    assert sent_data["refresh_token"] == "cached_refresh_tok"
    assert "code" not in sent_data


@pytest.mark.asyncio
async def test_fetch_access_token_authorization_code_grant_with_pkce():
    """Authorization Code + PKCE: code_verifier is included in the request."""
    client = _make_client({
        "oauth_grant_type": "authorization_code",
        "oauth_authorization_code": "pkce_code_abc",
        "oauth_redirect_uri": "https://myapp.example.com/callback",
        "pkce_code_verifier": "my_code_verifier_string",
    })
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        await client._fetch_access_token()

    sent_data = session.post.call_args[1]["data"]
    assert sent_data["code_verifier"] == "my_code_verifier_string"


@pytest.mark.asyncio
async def test_fetch_access_token_jwt_bearer_grant():
    """JWT Bearer grant: assertion field is built and sent."""
    import connectors.sources.servicenow.client as sn_client

    client = _make_client({
        "oauth_grant_type": "jwt_bearer",
        "jwt_private_key": "dummy_pem_key",
        "jwt_subject": "svc_account",
        "jwt_key_id": "key-1",
        "jwt_algorithm": "RS256",
    })
    token_payload = _make_token_response()
    session_ctx, session = _mock_token_post(token_payload)

    # Mock PyJWT availability and signing
    with mock.patch.object(sn_client, "_PYJWT_AVAILABLE", True):
        with mock.patch.object(sn_client, "pyjwt") as mock_jwt:
            mock_jwt.encode = mock.MagicMock(return_value="signed_jwt_assertion")
            with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
                token = await client._fetch_access_token()

    assert token == "tok_abc"
    sent_data = session.post.call_args[1]["data"]
    assert sent_data["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
    assert sent_data["assertion"] == "signed_jwt_assertion"
    assert sent_data["client_id"] == "test_client_id"


@pytest.mark.asyncio
async def test_fetch_access_token_jwt_bearer_no_pyjwt_raises():
    """JWT Bearer grant raises RuntimeError when PyJWT is not installed."""
    import connectors.sources.servicenow.client as sn_client
    from connectors.sources.servicenow.client import InvalidResponse

    client = _make_client({
        "oauth_grant_type": "jwt_bearer",
        "jwt_private_key": "dummy_pem_key",
    })

    with mock.patch.object(sn_client, "_PYJWT_AVAILABLE", False):
        with pytest.raises(RuntimeError, match="PyJWT is required"):
            await client._fetch_access_token()


@pytest.mark.asyncio
async def test_fetch_access_token_jwt_bearer_no_private_key_raises():
    """JWT Bearer grant raises InvalidResponse when private key is missing."""
    import connectors.sources.servicenow.client as sn_client
    from connectors.sources.servicenow.client import InvalidResponse

    client = _make_client({
        "oauth_grant_type": "jwt_bearer",
        "jwt_private_key": "",
    })

    with mock.patch.object(sn_client, "_PYJWT_AVAILABLE", True):
        with pytest.raises(InvalidResponse, match="jwt_private_key must be set"):
            await client._fetch_access_token()


@pytest.mark.asyncio
async def test_fetch_access_token_missing_access_token_raises():
    """Any grant type raises InvalidResponse when access_token is absent."""
    client = _make_client({"oauth_grant_type": "client_credentials"})
    # Token endpoint returns a response without access_token
    session_ctx, _ = _mock_token_post({"expires_in": 1800})

    with mock.patch("aiohttp.ClientSession", return_value=session_ctx):
        with pytest.raises(Exception, match="access_token"):
            await client._fetch_access_token()

def test_ui_field_dependencies():
    """Verify field dependency and visibility rules for UI rendering:
    - Basic Auth shows only 'username' and 'basic_auth_password' (under auth_method)
    - OAuth Password grant shows 'client_id', 'client_secret', 'oauth_username', 'oauth_password'
    - OAuth Refresh Token grant shows 'client_id', 'client_secret', 'oauth_username_refresh', 'refresh_token'

    depends_on uses AND semantics in the SDK: all conditions must be true
    simultaneously.  Because a single field value cannot satisfy two different
    values at once, the username field is split into two entries — one per
    grant type — and oauth_password is also gated on auth_method=oauth to
    prevent it bleeding through when Basic Auth is selected (the default
    oauth_grant_type value of "password" would otherwise match).
    """
    config = ServiceNowDataSource.get_default_configuration()

    # Basic Auth fields
    assert config["username"]["depends_on"] == [{"field": "auth_method", "value": "basic"}]
    assert config["basic_auth_password"]["depends_on"] == [{"field": "auth_method", "value": "basic"}]

    # OAuth core fields
    assert config["oauth_grant_type"]["depends_on"] == [{"field": "auth_method", "value": "oauth"}]
    assert config["client_id"]["depends_on"] == [{"field": "auth_method", "value": "oauth"}]
    assert config["client_secret"]["depends_on"] == [{"field": "auth_method", "value": "oauth"}]

    # OAuth username — separate field per grant type (AND semantics prevents a
    # single entry covering two different grant-type values simultaneously)
    assert config["oauth_username"]["depends_on"] == [
        {"field": "auth_method", "value": "oauth"},
        {"field": "oauth_grant_type", "value": "password"},
    ]
    assert config["oauth_username_refresh"]["depends_on"] == [
        {"field": "auth_method", "value": "oauth"},
        {"field": "oauth_grant_type", "value": "refresh_token"},
    ]

    # OAuth password — AND-gated on both auth_method and grant type to prevent
    # bleeding through to Basic Auth (where oauth_grant_type defaults to "password")
    assert config["oauth_password"]["depends_on"] == [
        {"field": "auth_method", "value": "oauth"},
        {"field": "oauth_grant_type", "value": "password"},
    ]

    # Refresh token (shown for refresh_token grant type)
    assert config["refresh_token"]["depends_on"] == [
        {"field": "oauth_grant_type", "value": "refresh_token"}
    ]
