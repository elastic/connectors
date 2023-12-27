#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pytest

from connectors.es.management_client import ESManagementClient
from tests.commons import AsyncIterator


class TestESManagementClient:
    @pytest.mark.asyncio
    async def test_index_exists(self):
        config = {
            "username": "elastic",
            "password": "changeme",
            "host": "http://nowhere.com:9200",
        }
        index_name = "search-mongo"
        es_management_client = ESManagementClient(config)
        es_management_client.client = Mock()
        es_management_client.client.indices.exists = AsyncMock()

        await es_management_client.index_exists(index_name=index_name)
        es_management_client.client.indices.exists.assert_awaited_with(
            index=index_name, expand_wildcards="open"
        )

    @pytest.mark.asyncio
    async def test_delete_indices(self):
        config = {
            "username": "elastic",
            "password": "changeme",
            "host": "http://nowhere.com:9200",
        }
        indices = ["search-mongo"]
        es_management_client = ESManagementClient(config)
        es_management_client.client = Mock()
        es_management_client.client.indices.delete = AsyncMock()

        await es_management_client.delete_indices(indices=indices)
        es_management_client.client.indices.delete.assert_awaited_with(
            index=indices, expand_wildcards="open", ignore_unavailable=True
        )

    @pytest.mark.asyncio
    async def test_yield_existing_documents_metadata(self, mock_responses):
        config = {
            "username": "elastic",
            "password": "changeme",
            "host": "http://nowhere.com:9200",
        }
        es_management_client = ESManagementClient(config)
        es_management_client.client = AsyncMock()
        es_management_client.client.index_exists = Mock(return_value=True)

        records = [
            {"_id": "1", "_source": {"_timestamp": str(datetime.now())}},
            {"_id": "2", "_source": {"_timestamp": str(datetime.now())}},
        ]

        with mock.patch(
            "connectors.es.management_client.async_scan",
            return_value=AsyncIterator(records),
        ):
            ids = []
            async for doc_id, _ in es_management_client.yield_existing_documents_metadata(
                "something"
            ):
                ids.append(doc_id)

            assert ids == ["1", "2"]
        await es_management_client.close()
